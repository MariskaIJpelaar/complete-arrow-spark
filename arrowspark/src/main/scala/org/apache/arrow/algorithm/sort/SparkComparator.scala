package org.apache.arrow.algorithm.sort

import org.apache.arrow.vector.ValueVector
import org.apache.spark.sql.catalyst.expressions.SortOrder

import scala.reflect.ClassTag

// TODO: implement
class SparkComparator[T <: ValueVector : ClassTag](val sortOrder: SortOrder, val baseComparator: VectorValueComparator[T] ) extends VectorValueComparator[T] {
  override def compare(index1: Int, index2: Int): Int = ???

  override def compareNotNull(index1: Int, index2: Int): Int = ???

  override def createNew(): VectorValueComparator[T] = ???
}
