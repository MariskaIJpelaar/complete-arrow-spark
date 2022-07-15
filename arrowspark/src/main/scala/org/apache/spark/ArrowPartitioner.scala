package org.apache.spark

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float4Vector, ValueVector}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.column.AllocationManager.{createAllocator, newRoot}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils._
import org.apache.spark.sql.column.utils.algorithms.{ArrowColumnarBatchRowDeduplicators, ArrowColumnarBatchRowDistributors, ArrowColumnarBatchRowSamplers, ArrowColumnarBatchRowSorters}
import org.apache.spark.sql.rdd.ArrowRDD

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
   * Callers should close returned batches and the returned [[RootAllocator]] */
  private def sketch(rdd: RDD[ArrowColumnarBatchRow], sampleSizePerPartition: Int):
  (Long, Iterator[(Int, Long, ArrowColumnarBatchRow)], RootAllocator) = {
    val shift = rdd.id

    val sketchedRDD = ArrowRDD.mapPartitionsWithIndex(rdd, { (idx, root, iter: Iterator[ArrowColumnarBatchRow]) =>
      val seed = byteswap32(idx ^ (shift << 16))
      val (sample, n) = ArrowColumnarBatchRowSamplers.sampleAndCount(root, iter, sampleSizePerPartition, seed)
      Iterator((idx, n, sample))
    })

    // Caller is responsible for closing
    val extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) = item => {
      val (idx: Int, n: Long, sample: ArrowColumnarBatchRow) = item
      Resources.closeOnFailGet(sample) { sample =>
        val bos = new ByteArrayOutputStream()
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        val oos = new ObjectOutputStream(codec.compressedOutputStream(bos))

        oos.writeInt(idx)
        oos.writeLong(n)
        oos.flush()
        oos.close()
        (bos.toByteArray, sample)
      }
    }

    // Caller is responsible for closing batch
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

    // Caller is responsible for closing
    val extraTaker: Any => (Any, ArrowColumnarBatchRow) = item => {
      val (idx: Int, n: Long, sample: ArrowColumnarBatchRow) = item
      ((idx, n), sample)
    }

    // FIXME: For now, we assume the map does not go wrong
    val allocator = newRoot()
    val collected = ArrowRDD.collect(
      sketchedRDD,
      rootAllocator = Option(allocator),
      extraEncoder = extraEncoder,
      extraDecoder = extraDecoder,
      extraTaker = extraTaker)
    val sketched = collected.map { case (extra: (Int, Long), batch: ArrowColumnarBatchRow) => (extra._1, extra._2, batch) }
    val numItems = sketched.map(_._2).sum
    (numItems, sketched.toIterator, allocator)
  }

  /** Note: inspiration from: org.apache.spark.RangePartitioner::determineBounds
   * Closes candidates
   * Caller is responsible for closing returned batches */
  private def determineBounds(
     rootAllocator: RootAllocator,
     candidates: ArrayBuffer[(ArrowColumnarBatchRow, Float)],
     partitions: Int): ArrowColumnarBatchRow = {
    // FIXME: candidates be autoclosable?
    try {
      assert(partitions - 1 < Integer.MAX_VALUE)

      // Checks if we have non-empty batches
      if (candidates.length < 1) return ArrowColumnarBatchRow.empty(rootAllocator)

      // we start by sorting the batches, and making the rows unique
      // we keep the weights by adding them as an extra column to the batch
      val batches = candidates map { case (batch, weight) =>
        Resources.autoCloseTryGet(batch) { batch =>
         Resources.autoCloseTryGet(new Float4Vector("weights", rootAllocator)) { weightsVector =>
           // allocate at root
           weightsVector.setInitialCapacity(batch.numRows)
           weightsVector.allocateNew()
           weightsVector.setValueCount(batch.numRows)
           0 until batch.numRows foreach { index => weightsVector.set(index, weight) }
           // transfer to batch
           val weightBatch = ArrowColumnarBatchRow.transfer(rootAllocator, "ArrowPartitioner::determineBounds::weightBatch", Array(weightsVector.asInstanceOf[ValueVector]))
           ArrowColumnarBatchRowTransformers.appendColumns(batch, weightBatch, createAllocator(rootAllocator, "ArrowPartitioner::append"))
         }
        }
      }

      if (batches.isEmpty) return ArrowColumnarBatchRow.empty(rootAllocator)

      val grouped: ArrowColumnarBatchRow = ArrowColumnarBatchRow.create(rootAllocator, batches.toIterator)
      val sorted: ArrowColumnarBatchRow = ArrowColumnarBatchRowSorters.multiColumnSort(grouped, orders)
      val (unique, weighted) = ArrowColumnarBatchRowConverters.splitColumns(ArrowColumnarBatchRowDeduplicators.unique(sorted, orders), grouped.numFields-1)
      Resources.autoCloseTryGet(unique)( unique => Resources.autoCloseTryGet(weighted) { weighted =>
        // now we gather our bounds
        assert(weighted.numFields == 1)
        val weights = weighted.getArray(0)
        val step = (0 until weights.numElements() map weights.getFloat).sum / partitions
        var cumWeight = 0.0
        var cumSize = 0
        var target = step
        var boundBuilder: Option[ArrowColumnarBatchRowBuilder] = None
        try {
          0 until unique.numRows takeWhile { index =>
            cumWeight += weights.getFloat(index)
            if (cumWeight >= target) {
              boundBuilder = Some(boundBuilder.fold
              ( new ArrowColumnarBatchRowBuilder(unique.copyFromCaller(s"ArrowPartitioner::boundBuilder::first::$index", index until index +1)))
              ( builder => builder.append(unique.copyFromCaller(s"ArrowPartitioner::boundBuilder::$index", index until index +1))) )
              cumSize += 1
              target += step
            }

            cumSize < partitions -1
          }
          rangeBoundsLength = Option(cumSize)
          boundBuilder.map( _.build(createAllocator(rootAllocator, "ArrowPartitioner::determineBounds::return")) )
            .getOrElse(ArrowColumnarBatchRow.empty(rootAllocator))
        } finally {
          boundBuilder.foreach(_.close())
        }
      })
    } finally {
      candidates.foreach(_._1.close())
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
    val decoded = ArrowRDD.map(rdd, (root: RootAllocator, item: Product2[Array[Byte], V]) => {
      val decoded = ArrowColumnarBatchRowUtils.take(root, ArrowColumnarBatchRowEncoders.decode(root, item._1))
      ArrowColumnarBatchRow.create(decoded._3, decoded._2)
    })

    // FIXME: make closeable?
    val (numItems, sketched, allocator) = sketch(decoded, sampleSizePerPartition)
    Resources.autoCloseTryGet(allocator) { _ =>
      try {
        if (numItems == 0L) Array.empty

        // If the partitions are imbalanced, we re-sample from it
        val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
        // FIXME: make closeable?
        val candidates = ArrayBuffer.empty[(ArrowColumnarBatchRow, Float)]
        try {
          val imbalancedPartitions = mutable.Set.empty[Int]
          sketched foreach[Unit] { case (idx, n, sample) =>
            try {
              if (fraction * n > sampleSizePerPartition) {
                imbalancedPartitions += idx
              } else {
                val weight = (n.toDouble / sample.length).toFloat
                candidates += ((sample.copyFromCaller("ArrowPartitioner::candidatesBuilder"), weight))
              }
            } finally {
              sample.close()
            }
          }
          if (imbalancedPartitions.nonEmpty) {
            val imbalanced = new PartitionPruningRDD(rdd.map(_._1), imbalancedPartitions.contains)
            val seed = byteswap32(-rdd.id -1)
            val reSampledRDD = ArrowRDD.mapPartitionsInternal(imbalanced, (root: RootAllocator, iter: Iterator[Array[Byte]]) => {
              val batches = iter.map { array => {
                val decoded = ArrowColumnarBatchRowUtils.take(root, ArrowColumnarBatchRowEncoders.decode(root, array))
                ArrowColumnarBatchRow.create(decoded._3, decoded._2)
              }}
              Iterator(ArrowColumnarBatchRowSamplers.sample(allocator, batches, fraction, seed))
            })
            val weight = (1.0 / fraction).toFloat
            candidates ++= ArrowRDD.collect(reSampledRDD, rootAllocator = Option(allocator)).map( x => (x._2, weight))
          }

          // determine bounds and encode them
          val bounds = determineBounds(allocator, candidates, math.min(partitions, candidates.size))
          ArrowColumnarBatchRowEncoders.encode(Iterator(bounds)).toArray.apply(0)
        } finally {
          candidates.foreach(_._1.close())
        }
      } finally {
        sketched.foreach(_._3.close())
      }
    }
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
    Resources.autoCloseTryGet(key) { key =>
      Resources.autoCloseTryGet(newRoot()) { root =>
        val decoded = ArrowColumnarBatchRowUtils.take(root, ArrowColumnarBatchRowEncoders.decode(root, rangeBounds))
        Resources.autoCloseTryGet(ArrowColumnarBatchRow.create(decoded._3, decoded._2)) { ranges =>
          ArrowColumnarBatchRowDistributors.bucketDistributor(key, ranges, orders)
        }
      }
    }
  }
}
