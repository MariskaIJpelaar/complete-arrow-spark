package org.apache.spark.sql.column.expressions

import org.apache.spark.sql.column.TColumn

import scala.reflect.ClassTag


class GenericColumn[T](protected[sql] val values: Array[T])(implicit tag: ClassTag[T]) extends TColumn {
  /** No-arg constructor for serialization */
  protected def this()(implicit tag: ClassTag[T]) = this(null)

  def this(size: Int)(implicit tag: ClassTag[T]) = this(new Array[T](size))

  /** Number of elements in the Column */
  override def length: Int = values.length

  /** Make a copy of the current Column object */
  override def copy(): TColumn = this

  /** Type of the column */
  override def colType: Class[_] = tag.getClass

  /** Returns the value at position i. If the value is null, None is returned */
  override protected def getInternal(i: Int): Option[T] = {
    if (i < 0 || i >= length) return None
    Some(values(i))
  }
}
