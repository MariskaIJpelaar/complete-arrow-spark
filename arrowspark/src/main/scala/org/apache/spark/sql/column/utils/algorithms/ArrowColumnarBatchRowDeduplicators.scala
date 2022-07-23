package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.deduplicate.VectorDeduplicator
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

import java.util.concurrent.atomic.AtomicLong

object ArrowColumnarBatchRowDeduplicators {
  var totalTime: AtomicLong = new AtomicLong(0)

  /**
   * @param batch batch to gather unique values from, and close
   * @param sortOrders order to define unique-ness
   * @return a fresh batch with the unique values from the previous
   *
   * Caller is responsible for closing the new batch
   */
  def unique(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch


    val t1 = System.nanoTime()
    Resources.autoCloseTryGet(batch) { batch =>
      // UnionVector representing our batch
      val (union, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(batch.copyFromCaller("ArrowColumnarBatchRowDeduplicator::unique"),
          sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name).toArray))
      Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(union) { union =>
        val comparator = ArrowColumnarBatchRowUtils.getComparator(union, sortOrders)
        val ret = Resources.autoCloseTryGet(createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowDeduplicator::indexVector")) { indexAllocator =>
          ArrowColumnarBatchRowTransformers.applyIndices(batch, VectorDeduplicator.uniqueIndices(indexAllocator, comparator, union))
        }
        val t2 = System.nanoTime()
        totalTime.addAndGet(t2 - t1)
        ret
      })
    }
  }
}
