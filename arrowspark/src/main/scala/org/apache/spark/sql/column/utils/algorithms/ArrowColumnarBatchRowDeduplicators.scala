package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.deduplicate.VectorDeduplicator
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, createAllocator}
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

object ArrowColumnarBatchRowDeduplicators {

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

    Resources.autoCloseTryGet(batch) { batch =>
      // UnionVector representing our batch
      val (union, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(batch.copy(createAllocator("ArrowColumnarBatchRowDeduplicator::unique")),
          sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name).toArray))
      Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(union) { union =>
        val comparator = ArrowColumnarBatchRowUtils.getComparator(union, sortOrders)
        ArrowColumnarBatchRowTransformers.applyIndices(batch, VectorDeduplicator.uniqueIndices(comparator, union))
      })
    }
  }
}
