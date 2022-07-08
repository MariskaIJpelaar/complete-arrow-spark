package org.apache.spark.sql.column.utils.algorithms

import org.apache.arrow.algorithm.deduplicate.VectorDeduplicator
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

object ArrowColumnarBatchRowDeduplicators {

  /**
   * @param batch batch to gather unique values from
   * @param sortOrders order to define unique-ness
   * @return a fresh batch with the unique values from the previous
   *
   * Closes the passed batch
   * TODO: Caller is responsible for closing the new batch
   */
  def unique(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    try {
      // UnionVector representing our batch
      val union = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(batch.copy(),
          sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name).toArray)
      )
      try {
        val comparator = ArrowColumnarBatchRowUtils.getComparator(union, sortOrders)
        ArrowColumnarBatchRowTransformers.applyIndices(batch, VectorDeduplicator.uniqueIndices(comparator, union))
      } finally {
        union.close()
      }
    } finally {
      batch.close()
    }
  }

}
