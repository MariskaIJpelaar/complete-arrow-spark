package org.apache.spark.sql.column

import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._

class ArrowColumnarBatchRow(protected val columns: Array[ArrowColumnVector]) extends InternalRow with AutoCloseable with Serializable {
  override def numFields: Int = columns.length

  // getters
  override def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException()
  override def get(ordinal: Int, dataType: DataType): AnyRef = throw new UnsupportedOperationException()
  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()
  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()
  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()
  override def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException()
  override def getLong(ordinal: Int): Long = throw new UnsupportedOperationException()
  override def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException()
  override def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException()
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = throw new UnsupportedOperationException()
  override def getUTF8String(ordinal: Int): UTF8String = throw new UnsupportedOperationException()
  override def getBinary(ordinal: Int): Array[Byte] = throw new UnsupportedOperationException()
  override def getInterval(ordinal: Int): CalendarInterval = throw new UnsupportedOperationException()
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = throw new UnsupportedOperationException()
  override def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException()
  override def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException()

  // setters
  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()
  override def setNullAt(i: Int): Unit = update(i, null)

  /** Note: uses slicing instead of complete copy,
   * according to: https://arrow.apache.org/docs/java/vector.html#slicing */
  override def copy(): InternalRow = {
    new ArrowColumnarBatchRow( columns map { v =>
      val vector = v.getValueVector
      val allocator = vector.getAllocator
      val tp = vector.getTransferPair(allocator)

      tp.transfer()
      new ArrowColumnVector(tp.getTo)
    })
  }

  override def close(): Unit = columns foreach(column => column.close())

  /** according to: https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format */
  private def writeExternal(bos: ByteArrayOutputStream): Unit = {
    // TODO: array to vararg?
    val root = VectorSchemaRoot.of( columns.toStream map { column => column.getValueVector} )

  }

  private def readExternal(objectInput: ObjectInput): Unit = ???
}

object ArrowColumnarBatchRow {
  /**  Note: similar to getByteArrayRdd(...)
   * Encodes the first n columns of a series of ArrowColumnarBatchRows*/
  def encode(n: Int, iter: Iterator[ArrowColumnarBatchRow]): Iterator[(Long, Array[Byte])] = {
//    var count: Long = 0
//    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bos = new ByteArrayOutputStream()





//    val oos = new ObjectOutputStream(codec.compressedOutputStream(bos))
//
//    while ((n < 0 || count < n) && iter.hasNext) {
//      oos.writeInt(1)
//      iter.next().writeExternal(oos)
//      count += 1
//    }
//
//    oos.writeInt(0)
//    oos.flush()
//    oos.close()
//    Iterator((count, bos.toByteArray))
  }

  /** Note: similar to decodeUnsafeRows */
  def decode(bytes: Array[Byte]): Iterator[ArrowColumnarBatchRow] = {
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(codec.compressedInputStream(bis))

    new NextIterator[ArrowColumnarBatchRow] {
      override protected def getNext(): ArrowColumnarBatchRow = {
        if (ois.readInt() == 0) {
          finished = true
          return null
        }
        val wrapper = new ArrowColumnarBatchRow(Array.empty)
        wrapper.readExternal(ois)
        wrapper
      }

      override protected def close(): Unit = ois.close()
    }
  }
}