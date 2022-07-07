package org.apache.spark

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.Float4Vector
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers}
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


  /** Note: inspiration from: org.apache.spark.RangePartitioner::sketch
   * TODO: Collects and cleans the RDD
   * TODO: Callers should close returned batch */
  private def sketch(rdd: RDD[ArrowColumnarBatchRow], sampleSizePerPartition: Int):
  (Long, Array[(Int, Long, ArrowColumnarBatchRow)]) = {
    val shift = rdd.id
    val sketchedRDD = rdd.mapPartitionsWithIndex { (idx, iter) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = ArrowColumnarBatchRow.sampleAndCount(iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    }

    // TODO: Caller is responsible for closing
    val extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) = item => {
      // TODO: Close?
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

    // TODO: Caller is responsible for closing batch
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

    // TODO: Caller is responsible for closing
    val extraTaker: Any => (Any, ArrowColumnarBatchRow) = item => {
      // TODO: Close?
      val (idx: Int, n: Long, sample: ArrowColumnarBatchRow) = item
      ((idx, n), sample)
    }

    /** wrapper for case in map */
    case class extraClass(value: (Int, Long))

    // TODO: in map(...), close?
    val sketched = ArrowRDD.collect(
      sketchedRDD,
      extraEncoder = extraEncoder,
      extraDecoder = extraDecoder,
      extraTaker = extraTaker
    ).map { case (extra: extraClass, batch: ArrowColumnarBatchRow) => (extra.value._1, extra.value._2, batch) }
    val numItems = sketched.map(_._2).sum
    (numItems, sketched)
  }

  /** Note: inspiration from: org.apache.spark.RangePartitioner::determineBounds
   * TODO: Closes candidates
   * TODO: Caller is responsible for closing returned batch */
  private def determineBounds(
     candidates: ArrayBuffer[(ArrowColumnarBatchRow, Float)],
     partitions: Int): Array[ArrowColumnarBatchRow] = {
    assert(partitions - 1 < Integer.MAX_VALUE)

    // Checks if we have non-empty batches
    if (candidates.length < 1) new ArrowColumnarBatchRow(Array.empty, 0)
    var allocatorOption: Option[BufferAllocator] = None
    var i = 0
    while (allocatorOption.isEmpty && i < candidates.size) {
      val batch = candidates(i)._1
      allocatorOption = batch.getFirstAllocator
      i += 1
    }

    val allocator = allocatorOption
      .getOrElse(throw new RuntimeException("[ArrowPartitioner::determineBounds] cannot get allocator"))
      .newChildAllocator("ArrowPartitioner::determineBounds", 0, Integer.MAX_VALUE)

    // we start by sorting the batches, and making the rows unique
    // we keep the weights by adding them as an extra column to the batch
    var totalRows = 0
    val batches = candidates map { case (batch, weight) =>
      try {
        // TODO: close weights?
        val weights = new Float4Vector("weights", allocator)
        weights.setValueCount(batch.numRows)
        0 until batch.numRows foreach { index => weights.set(index, weight) }
        totalRows += batch.numRows
        // consumes batch
        ArrowColumnarBatchRowTransformers.appendColumns(batch, Array(new ArrowColumnVector(weights)))
      } finally {
        batch.close()
      }
    }

    val grouped: ArrowColumnarBatchRow = new ArrowColumnarBatchRow(ArrowColumnarBatchRow.take(batches.toIterator)._2, totalRows)
    val sorted: ArrowColumnarBatchRow = ArrowColumnarBatchRow.multiColumnSort(grouped, orders)
    val (unique, weighted) = ArrowColumnarBatchRowConverters.splitColumns(ArrowColumnarBatchRow.unique(sorted, orders), grouped.numFields-1)
    try {
      // now we gather our bounds
      assert(weighted.numFields == 1)
      val weights = weighted.getArray(0)
      val step = (0 until weights.numElements() map weights.getFloat).sum / partitions
      var cumWeight = 0.0
      var cumSize = 0
      var target = step
      // TODO: Close?
      val bounds =  new Array[ArrowColumnarBatchRow](partitions -1)
      0 until unique.numRows takeWhile { index =>
        cumWeight += weights.getFloat(index)
        if (cumWeight >= target) {
          bounds(cumSize) = unique.copy(index until index +1)
          cumSize += 1
          target += step
        }

        cumSize < partitions -1
      }

      rangeBoundsLength = Option(cumSize)
      bounds
    } finally {
      unique.close()
      weighted.close()
    }
  }

  private var rangeBoundsLength: Option[Int] = None

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
    // TODO: Close?
    val decoded: RDD[ArrowColumnarBatchRow] = rdd.map { iter =>
      ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(iter._1))._2)
    }
    val (numItems, sketched) = sketch(decoded, sampleSizePerPartition)
    if (numItems == 0L) Array.empty

    // If the partitions are imbalanced, we re-sample from it
    val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
    // TODO: Close?
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
    candidates foreach (_._1.close()) // TODO test
    ArrowColumnarBatchRow.encode(bounds.toIterator).toArray.apply(0)
  }

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

  override def getPartitions(key: ArrowColumnarBatchRow): Array[Int] = {
    val ranges = ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(rangeBounds))._2)
    val partitionIds = ArrowColumnarBatchRow.bucketDistributor(key, ranges, orders)
    ranges.close()
    partitionIds
  }
}
