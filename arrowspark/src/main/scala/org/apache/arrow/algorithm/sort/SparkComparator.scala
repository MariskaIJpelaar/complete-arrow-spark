package org.apache.arrow.algorithm.sort

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.catalyst.expressions.{NullsFirst, SortOrder}

import scala.reflect.ClassTag

class SparkComparator[T <: ValueVector : ClassTag](val sortOrder: SortOrder, val baseComparator: VectorValueComparator[T] ) extends VectorValueComparator[T] {
  override def compare(index1: Int, index2: Int): Int = {
    val isNull1 = vector1.isNull(index1)
    val isNull2 = vector2.isNull(index2)

    if (!isNull1 && !isNull2)
      return compareNotNull(index1, index2)
    if (isNull1 && isNull2)
      return 0
    if (isNull1)
      return if (sortOrder.nullOrdering == NullsFirst) -1 else 1
    if (sortOrder.nullOrdering == NullsFirst) 1 else -1
  }

  override def compareNotNull(index1: Int, index2: Int): Int = {
    if (sortOrder.isAscending)
      baseComparator.compareNotNull(index1, index2)
    else
      -baseComparator.compareNotNull(index1, index2)
  }

  override def createNew(): VectorValueComparator[T] = { new SparkComparator[T](sortOrder, baseComparator) }
}
