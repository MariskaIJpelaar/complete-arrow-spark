package org.apache.spark.sql.column

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowUtils
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.io._

// TODO: memory management
// TODO: removed 'with Serializable', if this should become an unsolvable issue, put it back! Otherwise,
//  it is probably better as we do not want to actually allow Serialization...

/** Note: while ArrowColumnVector is technically AutoCloseable and not Closeable (which means you should not close more than once), the implemented close does not produce
 * weird side effects, so we are going to ignore this restriction. Plus, the underlying ValueVector is Closeable.
 * It is important to verify this for every Spark-version-update! */
class ArrowColumnarBatchRow(@transient protected[column] val columns: Array[ArrowColumnVector], val numRows: Int) extends InternalRow with Closeable {
  override def numFields: Int = columns.length

  def length: Long = numRows

  override def isNullAt(ordinal: Int): Boolean = ordinal < 0 || ordinal >= numFields

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = {
    val column = columns(ordinal)
    new ArrowColumnarArray(new ColumnarArray(column, 0, column.getValueVector.getValueCount))
  }

  // unsupported getters
  override def get(ordinal: Int, dataType: DataType): AnyRef = throw new UnsupportedOperationException()
  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()
  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()
  override def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException()
  override def getLong(ordinal: Int): Long = throw new UnsupportedOperationException()
  override def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException()
  override def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException()
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = throw new UnsupportedOperationException()
  override def getUTF8String(ordinal: Int): UTF8String = throw new UnsupportedOperationException()
  override def getBinary(ordinal: Int): Array[Byte] = throw new UnsupportedOperationException()
  override def getInterval(ordinal: Int): CalendarInterval = throw new UnsupportedOperationException()
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = throw new UnsupportedOperationException()
  override def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException()

  // unsupported setters
  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()
  override def setNullAt(i: Int): Unit = update(i, null)

  /** Get first available allocator */
  def getFirstAllocator: Option[BufferAllocator] =
    if (numFields > 0) Option(columns(0).getValueVector.getAllocator) else None

  /** Uses slicing instead of complete copy,
   * according to: https://arrow.apache.org/docs/java/vector.html#slicing
   * Caller is responsible for both this batch and copied-batch */
  override def copy(): ArrowColumnarBatchRow = {
    copy(0 until numRows)
  }

  /** Uses slicing instead of complete copy,
   * according to: https://arrow.apache.org/docs/java/vector.html#slicing
   * Caller is responsible for both this batch and copied-batch */
  def copy(range: Range = 0 until numRows): ArrowColumnarBatchRow = {
    new ArrowColumnarBatchRow(columns map { v =>
      val vector = v.getValueVector
      val allocator = vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::copy", 0, Integer.MAX_VALUE)
      val tp = vector.getTransferPair(allocator)

      tp.splitAndTransfer(range.head, range.length)
      new ArrowColumnVector(tp.getTo)
    }, range.length)
  }

  /**
   * Copy the values at the row at index thatIndex from the given batch to the row of this batch
   * at index thisIndex
   * @param from batch to copy from
   * @param thisIndex index to copy to
   * @param thatIndex index to copy from
   */
  def copyAtIndex(from: ArrowColumnarBatchRow, thisIndex: Int, thatIndex: Int): Unit = {
    if (thisIndex > numRows-1) return
    if (thatIndex > from.numRows -1) return
    columns zip from.columns foreach { case (ours, theirs) => ours.getValueVector.copyFrom(thatIndex, thisIndex, theirs.getValueVector)}
  }

  override def close(): Unit = columns foreach(column => column.close())

  def getSizeInBytes: Int = columns.map(column => column.getValueVector.getBufferSize ).sum

  override def hashCode(): Int = {
    val prime = 67
    var result = 1
    columns foreach { c =>
      val column = c.getValueVector
      0 until column.getValueCount foreach { index =>result = prime * result + column.hashCode(index) }
    }
    result = prime * result + numRows.hashCode()
    result
  }

  /** Note: does not close either of the batches */
  override def equals(o: Any): Boolean = o match {
    case other: ArrowColumnarBatchRow =>
      if (other.numRows != numRows) return false
      if (other.numFields != numFields) return false

      other.columns zip columns foreach { case (a, b) =>
        val ours = a.getValueVector
        val theirs = b.getValueVector
        if (ours.getField != theirs.getField)
          return false
        if (ours.getValueCount != theirs.getValueCount)
          return false

        0 until numRows foreach { index =>
          if (ours.hashCode(index) != theirs.hashCode(index))
            return false
        }
      }
      true
    case _ => false
  }
}

object ArrowColumnarBatchRow {
  /** Creates an empty ArrowColumnarBatchRow */
  def empty: ArrowColumnarBatchRow = new ArrowColumnarBatchRow(Array.empty, 0)

  /** Creates a fresh ArrowColumnarBatchRow from an iterator of ArrowColumnarBatchRows
   * Closes the batches in the provided iterator
   * WARNING: uses 'take', a very expensive operation. Use with care!
   * Caller is responsible for closing generated batch */
  def create(iter: Iterator[ArrowColumnarBatchRow]): ArrowColumnarBatchRow = {
    ArrowColumnarBatchRow.create(ArrowColumnarBatchRowUtils.take(iter)._2)
  }

  /** Creates a fresh ArrowColumnarBatchRow from an array of ArrowColumnVectors
   * Caller is responsible for closing the generated batch */
  def create(cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow = {
    val length = if (cols.length > 0) cols(0).getValueVector.getValueCount else 0
    new ArrowColumnarBatchRow(cols, length)
  }
}