package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, IndexSorter, SparkComparator}
import org.apache.arrow.vector.IntVector
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, createAllocator}
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

object ArrowColumnarBatchRowSorters {

  /**
   * Performs a multi-columns sort on a batch
   * @param batch batch to sort
   * @param sortOrders orders to sort on
   * @return a new, sorted, batch
   *
   * Closes the passed batch
   * Caller is responsible for closing returned batch
   */
  def multiColumnSort(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    Resources.autoCloseTryGet(batch) { batch =>
      // Indices for permutations
      Resources.autoCloseTryGet(new IntVector("indexHolder",
        createAllocator("ArrowColumnarBatchRowSorters::multiColumnSort::indices"))) { indices =>
        // UnionVector representing our batch with columns from sortOrder
        Resources.autoCloseTryGet(ArrowColumnarBatchRowConverters.toUnionVector(
          ArrowColumnarBatchRowTransformers.getColumns(batch.copy(
            createAllocator("ArrowColumnarBatchRowSorters::multiColumnSort::union")),
            sortOrders.map(order => order.child.asInstanceOf[AttributeReference].name).toArray))) { union =>
          val comparator = ArrowColumnarBatchRowUtils.getComparator(union, sortOrders)

          // prepare indices
          indices.allocateNew(batch.numRows)
          indices.setValueCount(batch.numRows)

          // compute the index-vector
          (new IndexSorter).sort(union, indices, comparator)

          /** from IndexSorter: the following relations hold: v(indices[0]) <= v(indices[1]) <= ... */
          ArrowColumnarBatchRowTransformers.applyIndices(batch, indices)
        }
      }
    }
  }

  /** Should we ever need to implement an in-place sorting algorithm (with numRows more space), then we can do
   * the normal sort with:  */
  //    (vec zip indices).zipWithIndices foreach { case (elem, index), i) =>
  //      if (i == index)
  //        continue
  //
  //      val realIndex = index
  //      while (realIndex < i) realIndex = indices(realIndex)
  //
  //      vec.swap(i, realIndex)
  //    }
  // Note: worst case: 0 + 1 + 2 + ... + (n-1) = ((n-1) * n) / 2 = O(n*n) + time to sort (n log n)

  /**
   * @param batch an ArrowColumnarBatchRow to be sorted
   * @param col the column to sort on
   * @param sortOrder order settings to pass to the comparator
   * @return a fresh ArrowColumnarBatchRows with the sorted columns from batch
   *         Note: if col is out of range, returns the batch
   *
   * Closes the passed batch
   * Caller is responsible for closing returned batch
   */
  def sort(batch: ArrowColumnarBatchRow, col: Int, sortOrder: SortOrder): ArrowColumnarBatchRow = {
    if (col < 0 || col > batch.numFields)
      return batch

    Resources.autoCloseTryGet(batch) { batch =>
      val vector = batch.columns(col).getValueVector
      Resources.autoCloseTryGet(new IntVector("indexHolder",
        createAllocator("ArrowColumnarBatchRow::sort::indices"))) { indices =>
        assert(vector.getValueCount > 0)

        indices.allocateNew(vector.getValueCount)
        indices.setValueCount(vector.getValueCount)
        val comparator = new SparkComparator(sortOrder, DefaultVectorComparators.createDefaultComparator(vector))
        (new IndexSorter).sort(vector, indices, comparator)

        // sort by permutation
        /** from IndexSorter: the following relations hold: v(indices[0]) <= v(indices[1]) <= ... */
        ArrowColumnarBatchRowTransformers.applyIndices(batch, indices)
      }
    }
  }
}
