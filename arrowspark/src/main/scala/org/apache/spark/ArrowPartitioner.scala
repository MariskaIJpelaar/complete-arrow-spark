package org.apache.spark

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.Float4Vector
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.rdd.ArrowRDD
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap32

abstract class ArrowPartitioner extends Partitioner {
  override def getPartition(key: Any): Int = throw new UnsupportedOperationException()
  def getPartitions(key: ArrowColumnarBatchRow): Array[Int]
}

class ArrowRangePartitioner[V](
     partitions: Int,
     rdd: RDD[_ <: Product2[Array[Byte], V]],
     orders: Seq[SortOrder],
     private var ascending: Boolean = true,
     val samplePointsPerPartitionHint: Int = 20) extends ArrowPartitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")
  require(samplePointsPerPartitionHint > 0,
    s"Sample points per partition must be greater than 0 but found $samplePointsPerPartitionHint")

//  private var ordering = implicitly[Ordering[ArrowColumnarBatchRow]]

  /** Note: inspiration from: org.apache.spark.RangePartitioner::sketch */
  private def sketch(rdd: RDD[ArrowColumnarBatchRow], sampleSizePerPartition: Int):
  (Long, Array[(Int, Long, ArrowColumnarBatchRow)]) = {
    val shift = rdd.id
    val sketchedRDD = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = ArrowColumnarBatchRow.sampleAndCount(iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }

    val extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) = item => {
      val (idx: Int, n: Long, sample: ArrowColumnarBatchRow) = item
      val bos = new ByteArrayOutputStream()
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val oos = new ObjectOutputStream(codec.compressedOutputStream(bos))

      oos.writeInt(idx)
      oos.writeLong(n)
      oos.flush()
      oos.close()
      (bos.toByteArray, sample)
    }

    val extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (array: Array[Byte], batch: ArrowColumnarBatchRow) => {
      val bis = new ByteArrayInputStream(array)
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val ois = new ObjectInputStream(codec.compressedInputStream(bis))

      val idx = ois.readInt()
      val n = ois.readLong()
      ois.close()
      bis.close()

      (idx, n, batch)
    }

    val extraTaker: Any => (Any, ArrowColumnarBatchRow) = item => {
      val (idx: Int, n: Long, sample: ArrowColumnarBatchRow) = item
      ((idx, n), sample)
    }

    val extraCollector: (Any, Option[Any]) => Any = (item, collection) => {
      val (idx: Int, n: Long) = item
      val prev = collection.asInstanceOf[Option[(Int, Long)]]
      prev.fold( (idx, n) ) { case (jdx, m) =>
        assert (idx == jdx)
        (idx, n + m)
      }
    }

    val sketched = ArrowRDD.collect(
      sketchedRDD,
      extraEncoder = extraEncoder,
      extraDecoder = extraDecoder,
      extraTaker = extraTaker,
      extraCollector = extraCollector
    ).map { case (extra: (Int, Long), batch: ArrowColumnarBatchRow) => (extra._1, extra._2, batch) }
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /** Note: inspiration from: org.apache.spark.RangePartitioner::determineBounds */
  private def determineBounds(
     candidates: ArrayBuffer[(ArrowColumnarBatchRow, Float)],
     partitions: Int): ArrowColumnarBatchRow = {
    // Checks if we have non-empty batches
    if (candidates.length < 1) new ArrowColumnarBatchRow(Array.empty, 0)
    var allocator: Option[BufferAllocator] = None
    candidates.takeWhile { case (batch, _) =>
      assert(batch.numRows <= Integer.MAX_VALUE)
      allocator = batch.getFirstAllocator
      allocator.isDefined
    }
    if (allocator.isEmpty) new ArrowColumnarBatchRow(Array.empty, 0)

    // we start by sorting the batches, and making the rows unique
    // we keep the weights by adding them as an extra column to the batch
    var totalRows = 0
    val batches = candidates map { case (batch, weight) =>
      val weights = new Float4Vector("weights", allocator.get)
      weights.setValueCount(batch.numRows.toInt)
      0 until batch.numRows.toInt foreach { index => weights.set(index, weight) }
      totalRows += batch.numRows.toInt
      batch.appendColumns( Array(new ArrowColumnVector(weights)) )
    }
    val grouped = new ArrowColumnarBatchRow(ArrowColumnarBatchRow.take(batches.toIterator)._2, totalRows)
    val sorted = ArrowColumnarBatchRow.multiColumnSort(grouped, orders)
    val (unique, weighted) = ArrowColumnarBatchRow.unique(sorted, orders).splitColumns(grouped.numFields-1)

    // now we gather our bounds
    assert(weighted.numFields == 1)
    val weights = weighted.getArray(0)
    val step = (0 until weights.numElements() map weights.getFloat).sum / partitions
    var cumWeight = 0.0
    var target = step
    var bounds: Option[ArrowColumnarBatchRow] = None
    0 until unique.numRows.toInt takeWhile { index =>
      cumWeight += weights.getFloat(index)
      if (cumWeight >= target) {
        bounds = Option(bounds.fold(unique.take( index until index+1 )) ( batch =>
          new ArrowColumnarBatchRow(
            ArrowColumnarBatchRow.take(Iterator(batch, unique.take( index until index+1 ) ))._2,
            batch.numRows + 1)
        ))
        target += step
      }

      bounds.forall( _.numRows < partitions -1)
    }

    assert(bounds.forall(_.numRows < Integer.MAX_VALUE))
    bounds.getOrElse(new ArrowColumnarBatchRow(Array.empty, 0))
  }

  // an array of upper bounds for the first (partitions-1) partitions
  // inspired by: org.apache.spark.RangePartitioner::rangeBounds
  // encoded ArrowBatchColumnarRow that represents the rangeBounds
  private val rangeBounds: Array[Byte] = {
    if (partitions <= 1) Array.empty

    // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
    // Cast to double to avoid overflowing ints or longs
    val sampleSize = math.min(samplePointsPerPartitionHint.toDouble * partitions, 1e6)
    // Assume the input partitions are roughly balanced and over-sample a little bit.
    val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.partitions.length).toInt

    // 'sketch' the distribution from a sample
    val decoded = rdd.map { iter =>
      ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(iter._1))._2)
    }
    val (numItems, sketched) = sketch(decoded, sampleSizePerPartition)
    if (numItems == 0L) Array.empty

    // If the partitions are imbalanced, we re-sample from it
    val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
    val candidates = ArrayBuffer.empty[(ArrowColumnarBatchRow, Float)]
    val imbalancedPartitions = mutable.Set.empty[Int]
    sketched foreach[Unit] { case (idx, n, sample) =>
      if (fraction * n > sampleSizePerPartition) {
        imbalancedPartitions += idx
      } else {
        val weight = (n.toDouble / sample.length).toFloat
        candidates += ((sample, weight))
      }
    }
    if (imbalancedPartitions.nonEmpty) {
      val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
      val seed = byteswap32(-rdd.id -1)
      val reSampledRDD = imbalanced.mapPartitionsInternal{ iter =>
        val batches = iter.map {
          array => ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(array))._2)
        }
        val sample = ArrowColumnarBatchRow.sample(batches, fraction, seed)
        Iterator(sample)}
      val reSampled = ArrowRDD.collect(reSampledRDD)
      val weight = (1.0 / fraction).toFloat
      candidates ++= reSampled.map( x => (x._2, weight))
    }

    // determine bounds and encode them
    // since we only provide a single Iterator, we can be sure to return the 'first' item from the generated iterator
    val bounds = determineBounds(candidates, math.min(partitions, candidates.size))
    rangeBoundsLength = Option(bounds.length.toInt)
    ArrowColumnarBatchRow.encode(Iterator(bounds)).toArray.apply(0)
  }

  private var rangeBoundsLength: Option[Int] = None
  override def numPartitions: Int = rangeBoundsLength.getOrElse(0) + 1


  /** Note: below two functions are directly copied from
   * org.apache.spark.Partitioner */
  override def equals(other: Any): Boolean = other match {
    case r: ArrowRangePartitioner[_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + rangeBounds.hashCode()
    result = prime * result + ascending.hashCode
    result
  }

  override def getPartitions(key: ArrowColumnarBatchRow): Array[Int] =
    ArrowColumnarBatchRow.bucketDistributor(
      key,
      ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(rangeBounds))._2),
      orders)
}
