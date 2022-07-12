package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.search.BucketSearcher
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

import scala.collection.mutable

object ArrowColumnarBatchRowDistributors {
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

      val names: Array[String] = sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name ).toArray
      val (keyUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(key.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::keyUnion"), names)
      )
      Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(keyUnion) { keyUnion =>
        val comparator = ArrowColumnarBatchRowUtils.getComparator(keyUnion, sortOrders)
        val (rangeUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
          ArrowColumnarBatchRowTransformers.getColumns(rangeBounds.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::rangeUnion"), names))
        Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(rangeUnion)
          { rangeUnion => new BucketSearcher(keyUnion, rangeUnion, comparator).distribute() }
        )
      })
    })
  }

  /**
   * @param key ArrowColumnarBatchRow to distribute and close
   * @param partitionIds Array containing which row corresponds to which partition
   * @return A map from partitionId to its corresponding ArrowColumnarBatchRow
   *
   * Caller should close the batches in the returned map
   */
  def distribute(key: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, ArrowColumnarBatchRow] = {
    Resources.autoCloseTryGet(key) { key =>
      // FIXME: close builder if we return earlier than expected
      val distributed = mutable.Map[Int, ArrowColumnarBatchRowBuilder]()

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
    }
  }
}
