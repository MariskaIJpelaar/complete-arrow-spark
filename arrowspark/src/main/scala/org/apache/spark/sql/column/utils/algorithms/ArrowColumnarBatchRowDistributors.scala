package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.search.BucketSearcher
import org.apache.arrow.algorithm.sort.IndexIntSorters
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

import scala.collection.mutable

object ArrowColumnarBatchRowDistributors {
  var totalTimeBucketDistributor = 0L

  /**
   * @param key ArrowColumnarBatchRow to define distribution for, and close
   * @param rangeBounds ArrowColumnarBatchRow containing ranges on which distribution is based, and close
   * @param sortOrders SortOrders on which distribution is based
   * @return Indices containing the distribution for the key given the rangeBounds and sortOrders
   */
  def bucketDistributor(key: ArrowColumnarBatchRow, rangeBounds: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): Array[Int] = {
    Resources.autoCloseTryGet(key) (key => Resources.autoCloseTryGet(rangeBounds) { rangeBounds =>
      if (key.numFields < 1 || rangeBounds.numFields < 1)
        return Array.empty

      val t1 = System.nanoTime()
      val names: Array[String] = sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name ).toArray
      val (keyUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(key.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::keyUnion"), names)
      )
      Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(keyUnion) { keyUnion =>
        val comparator = ArrowColumnarBatchRowUtils.getComparator(keyUnion, sortOrders)
        val (rangeUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
          ArrowColumnarBatchRowTransformers.getColumns(rangeBounds.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::rangeUnion"), names))
        val ret = Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(rangeUnion)
          { rangeUnion => new BucketSearcher(keyUnion, rangeUnion, comparator).distributeParallel() }
        )
        val t2 = System.nanoTime()
        totalTimeBucketDistributor += (t2-t1)
        ret
      })
    })
  }

  var totalTimeDistributeBySort = 0L

  /**
   * Distributes a batch to a mapping (partitionId, Batch), according to the provided Array of Ints
   * @param batch [[ArrowColumnarBatchRow]] to distribute, and close
   * @param partitionIds Array of Ints, which represent which index should go to which partition
   * @return [[Map]] mapping partitionIds ([[Int]]) to batches ([[ArrowColumnarBatchRow]])
   */
  def distributeBySort(batch: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, ArrowColumnarBatchRow]  = {
    Resources.autoCloseTryGet(batch) { _ =>
      val t1 = System.nanoTime()
      Resources.autoCloseTryGet(createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowDistributors::distributeBySort::indices")) { indexAllocator =>
        val (indices, borders) = IndexIntSorters.sortManyDuplicates(partitionIds, indexAllocator)
        Resources.autoCloseTryGet(indices) { _ =>
          Resources.autoCloseTryGet(ArrowColumnarBatchRowTransformers.applyIndices(batch, indices)) { sorted =>
            val ret = borders.map { case (partitionId, range) =>
              (partitionId, sorted.copyFromCaller(s"ArrowColumnarBatchRowDistributors::distributeBySort::copy::$partitionId", range))
            }
            val t2 = System.nanoTime()
            totalTimeBucketDistributor += (t2-t1)
            ret
          }
        }
      }
    }
  }

  var totalTimeDistribute = 0L

  /**
   * @param key ArrowColumnarBatchRow to distribute and close
   * @param partitionIds Array containing which row corresponds to which partition
   * @return A map from partitionId to its corresponding ArrowColumnarBatchRow
   *
   * Caller should close the batches in the returned map
   */
  def distribute(key: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, ArrowColumnarBatchRow] = {
    Resources.autoCloseTryGet(key) { key =>
      val t1 = System.nanoTime()
      // FIXME: close builder if we return earlier than expected
      val distributed = mutable.Map[Int, ArrowColumnarBatchRowBuilder]()
      try {
        partitionIds.zipWithIndex foreach { case (partitionId, index) =>
          if (distributed.contains(partitionId))
            distributed(partitionId).append(key
              .copyFromCaller("ArrowColumnarBatchRowDistributors::distribute", index until index+1))
          else
            distributed(partitionId) = new ArrowColumnarBatchRowBuilder(key
              .copyFromCaller("ArrowColumnarBatchRowDistributors::distribute::first", index until index+1))
        }

        distributed.map ( items =>
          (items._1, items._2.build(createAllocator(key.allocator.getRoot, "ArrowColumnarBatchRowDistributors::distribute::build"))) ).toMap
      } finally {
        distributed.foreach ( item => item._2.close() )
        val t2 = System.nanoTime()
        totalTimeDistribute += (t2 - t1)
      }
    }
  }
}
