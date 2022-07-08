package org.apache.spark.sql.column.utils

import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, SparkComparator, SparkUnionComparator}
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.UnionVector
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}

/** Methods that do not fit into the other categories */
object ArrowColumnarBatchRowUtils {

  /**
   * Get the SparkUnionComparator corresponding to given UnionVector and SortOrders
   * @param union UnionVector to make comparator from. Vector is not closed.
   * @param sortOrders SortOrders to make comparator from
   * @return a fresh Comparator, does not require any closing
   */
  def getComparator(union: UnionVector, sortOrders: Seq[SortOrder]): SparkUnionComparator = {
    val comparators = union.getChildrenFromFields.toArray.map { vector =>
      val valueVector = vector.asInstanceOf[ValueVector]
      val name = valueVector.getName
      val sortOrder = sortOrders.find( order => order.child.asInstanceOf[AttributeReference].name.equals(name) )
      (name,
        new SparkComparator(
          sortOrder.getOrElse(throw new RuntimeException("ArrowColumnarBatchRowUtils::getComparator SortOrders do not match UnionVector")),
          DefaultVectorComparators.createDefaultComparator(valueVector)))
    }
    new SparkUnionComparator(comparators)
  }

}
