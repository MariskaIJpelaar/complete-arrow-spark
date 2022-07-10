package org.apache.spark.sql.vectorized

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import scala.language.implicitConversions

object ArrowColumnarArray {
  implicit def toColumnarArray(array: ArrowColumnarArray): ColumnarArray = array.columnar
}

/**
 * Wrapper class around [[ColumnarArray]] to give access to its internal [[ColumnVector]] data
 * as [[ArrowColumnVector]]
 * @param columnar the ColumnarArray to wrap
 */
class ArrowColumnarArray(private val columnar: ColumnarArray) extends ArrayData {
  def getData: ArrowColumnVector = {
    val field = classOf[ColumnarArray].getDeclaredField("data")
    field.setAccessible(true)
    field.get(columnar).asInstanceOf[ColumnVector] match {
      case vector: ArrowColumnVector => vector
    }
  }

  override def copy(): ArrayData = {
    val vector = getData.getValueVector
    val allocator = vector.getAllocator
      .newChildAllocator(s"ArrowColumnarArray::copy::", 0, org.apache.spark.sql.column.perAllocatorSize)
    val tp = vector.getTransferPair(allocator)

    tp.splitAndTransfer(0, vector.getValueCount)
    new ArrowColumnarArray(new ColumnarArray(new ArrowColumnVector(tp.getTo), 0, vector.getValueCount))
  }

  override def numElements(): Int = columnar.numElements()
  override def array: Array[Any] = columnar.array().asInstanceOf[Array[Any]]
  override def setNullAt(i: Int): Unit = columnar.setNullAt(i)
  override def update(i: Int, value: Any): Unit = columnar.update(i, value)
  override def isNullAt(ordinal: Int): Boolean = columnar.isNullAt(ordinal)
  override def getBoolean(ordinal: Int): Boolean = columnar.getBoolean(ordinal)
  override def getByte(ordinal: Int): Byte = columnar.getByte(ordinal)
  override def getShort(ordinal: Int): Short = columnar.getShort(ordinal)
  override def getInt(ordinal: Int): Int = columnar.getInt(ordinal)
  override def getLong(ordinal: Int): Long = columnar.getLong(ordinal)
  override def getFloat(ordinal: Int): Float = columnar.getFloat(ordinal)
  override def getDouble(ordinal: Int): Double = columnar.getDouble(ordinal)
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    columnar.getDecimal(ordinal, precision, scale)
  override def getUTF8String(ordinal: Int): UTF8String = columnar.getUTF8String(ordinal)
  override def getBinary(ordinal: Int): Array[Byte] = columnar.getBinary(ordinal)
  override def getInterval(ordinal: Int): CalendarInterval = columnar.getInterval(ordinal)
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = columnar.getStruct(ordinal, numFields)
  override def getArray(ordinal: Int): ArrayData = columnar.getArray(ordinal)
  override def getMap(ordinal: Int): MapData = columnar.getMap(ordinal)
  override def get(ordinal: Int, dataType: DataType): AnyRef = columnar.get(ordinal, dataType)
}
