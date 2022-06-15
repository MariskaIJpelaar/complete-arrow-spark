package org.apache.spark.sql.column.expressions

import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.column.{ColumnBatch, TColumn}
import org.apache.spark.sql.types.{ArrayType, DataType}

import scala.reflect.ClassTag.Nothing


class GenericColumn(protected[sql] val values: Array[Any]) extends TColumn {
  /** No-arg constructor for serialization */
  protected def this() = this(new Array[Any](0))

  def this(size: Int) = this(new Array[Any](size))

  def this(arrayData: ArrayData) = this(arrayData.array)

  /** Number of elements in the Column */
  override def length: Int = values.length

  /** Make a copy of the current Column object */
  override def copy(): TColumn = this

  /** Type of the column */
  override def colType: Class[_] = {
    val firstValid = values.find( item => item != null)
    if (firstValid.isEmpty) return Nothing.getClass
    firstValid.get.getClass
  }

  /** Returns the value at position i. If the value is null, None is returned */
  override protected def getInternal(i: Int): Option[Any] = {
    if (i < 0 || i >= length) return None
    Some(values(i))
  }

  /** Concats this TColumn with an other TColumn and returns the result */
  override def concat(other: TColumn): TColumn = {
    if (!other.isInstanceOf[GenericColumn])
      return this
    if (values.length == 0)
      return other
    if (other.colType != colType)
      return this
    new GenericColumn(this.values ++ other.asInstanceOf[GenericColumn].values)
  }
}

class GenericColumnBatch(protected[sql] val columns: Array[TColumn]) extends ColumnBatch {
  /** No-arg constructor for serialization */
  protected def this() = this(null)

  def this(size: Int) = this(new Array[TColumn](size))

  /** Number of Columns */
  override def length: Int = columns.length

  /** Returns the i-th column. If the value is null, None is returned */
  override protected def getInternal(i: Int): Option[TColumn] = {
    if (i < 0 || i >= length) return None
    Some(columns(i))
  }

  /** Make a copy of the current object */
  override def copy(): ColumnBatch = this
}
