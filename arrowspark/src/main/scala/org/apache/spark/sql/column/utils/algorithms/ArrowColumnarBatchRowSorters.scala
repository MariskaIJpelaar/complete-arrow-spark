package org.apache.spark.sql.column.utils.algorithms

import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, IndexSorter, SparkComparator, SparkUnionComparator}
import org.apache.arrow.vector.{FieldVector, IntVector, ValueVector}
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

object ArrowColumnarBatchRowSorters {

  /**
   * Performs a multi-columns sort on a batch
   * @param batch batch to sort
   * @param sortOrders orders to sort on
   * @return a new, sorted, batch
   *
   * Closes the passed batch
   * TODO: Caller is responsible for closing returned batch
   */
  def multiColumnSort(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    try {
      // TODO: function for making an UnionVector
      // UnionVector representing our batch
      val firstAllocator = batch.getFirstAllocator
        .getOrElse( throw new RuntimeException("[ArrowColumnarBatchRow::multiColumnSort] cannot get allocator ") )
      val unionAllocator = firstAllocator
        .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::union", 0, Integer.MAX_VALUE)
      val union = new UnionVector("Combiner", unionAllocator, FieldType.nullable(Struct.INSTANCE), null)

      // Indices for permutations
      val indexAllocator = firstAllocator
        .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::indices", 0, Integer.MAX_VALUE)
      val indices = new IntVector("indexHolder", indexAllocator)

      try {
        // prepare comparator and UnionVector
        val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
        sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
          val name = sortOrder.child.asInstanceOf[AttributeReference].name
          batch.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
            val valueVector = vector.getValueVector
            val tp = valueVector.getTransferPair(unionAllocator
              .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::union::transfer", 0, Integer.MAX_VALUE))
            tp.splitAndTransfer(0, batch.numRows.toInt)
            union.addVector(tp.getTo.asInstanceOf[FieldVector])
            comparators(index) = (
              name,
              new SparkComparator[ValueVector](sortOrder, DefaultVectorComparators.createDefaultComparator(valueVector))
            )
          })
        }
        val comparator = new SparkUnionComparator(comparators)
        union.setValueCount(batch.numRows.toInt)

        // prepare indices
        indices.allocateNew(batch.numRows.toInt)
        indices.setValueCount(batch.numRows.toInt)

        // compute the index-vector
        (new IndexSorter).sort(union, indices, comparator)

        // sort all columns by permutation according to indices
        new ArrowColumnarBatchRow( batch.columns map { column =>
          val vector = column.getValueVector
          assert(vector.getValueCount == indices.getValueCount)

          // transfer type
          val tp = vector.getTransferPair(vector.getAllocator
            .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::permutation", 0, Integer.MAX_VALUE))
          tp.splitAndTransfer(0, vector.getValueCount)
          val new_vector = tp.getTo

          new_vector.setInitialCapacity(indices.getValueCount)
          new_vector.allocateNew()
          assert(indices.getValueCount > 0)
          assert(indices.getValueCount.equals(vector.getValueCount))
          /** from IndexSorter: the following relations hold: v(indices[0]) <= v(indices[1]) <= ... */
          0 until indices.getValueCount foreach { index => new_vector.copyFromSafe(indices.get(index), index, vector) }
          new_vector.setValueCount(indices.getValueCount)

          new ArrowColumnVector(new_vector)
        }, batch.numRows)
      } finally {
        union.close()
        indices.close()
      }
    } finally {
      batch.close()
    }
  }


}
