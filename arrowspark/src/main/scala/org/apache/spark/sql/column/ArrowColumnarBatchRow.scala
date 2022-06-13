package org.apache.spark.sql.column

import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

// TODO: replace whatever is required by ArrowColumnarBatchRow!

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

  def copyNToRoot(n: Int, root: VectorSchemaRoot): Unit = {
    for ( (vec, col) <- root.getFieldVectors.slice(0, n) zip columns.slice(0, n)) {
      vec.reset()
      vec.copyFrom(0, 0, col.getValueVector)
    }
  }

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
}

object ArrowColumnarBatchRow {
  /**  Note: similar to getByteArrayRdd(...)
   * Encodes the first n columns of a series of ArrowColumnarBatchRows
   * according to: https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format
   *
   * Note: "The recommended usage for VectorSchemaRoot is creating a single VectorSchemaRoot
   * based on the known schema and populated data over and over into the same VectorSchemaRoot
   * in a stream of batches rather than creating a new VectorSchemaRoot instance each time"
   * source: https://jorisvandenbossche.github.io/arrow-docs-preview/html-option-1/java/vector_schema_root.html#vectorschemaroot */
  def encode(n: Int, iter: Iterator[ArrowColumnarBatchRow]): Iterator[Array[Byte]] = {
    new NextIterator[Array[Byte]] {
      var root: Option[VectorSchemaRoot] = None
      lazy val bos = new ByteArrayOutputStream()
      var writer: Option[ArrowStreamWriter] = None

      private def init(first: ArrowColumnarBatchRow): Unit = {
        root = Some(VectorSchemaRoot.of( first.columns.slice(0, n).map(
          column => column.getValueVector.asInstanceOf[FieldVector]
        ).toSeq :_* ))
        writer = Some( new ArrowStreamWriter(root.get, null, Channels.newChannel(bos)) )

        writer.get.start()
        writer.get.writeBatch()
      }

      override protected def getNext(): Array[Byte] = {
        if (!iter.hasNext) {
          finished = true
          return null
        }

        val batch = iter.next()
        if (root.isEmpty || writer.isEmpty)
          init(batch)
        else
          batch.copyNToRoot(n, root.get)


        bos.toByteArray
      }

      override protected def close(): Unit = {
        if (writer.isDefined)
          writer.get.end()
        bos.close()
      }
    }
  }

  /** Note: similar to decodeUnsafeRows */
    // TODO: implement
  def decode(bytes: Array[Byte]): Iterator[ArrowColumnarBatchRow] = ???
//    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
//    val bis = new ByteArrayInputStream(bytes)
//    val ois = new ObjectInputStream(codec.compressedInputStream(bis))
//
//    new NextIterator[ArrowColumnarBatchRow] {
//      override protected def getNext(): ArrowColumnarBatchRow = {
//        if (ois.readInt() == 0) {
//          finished = true
//          return null
//        }
//        val wrapper = new ArrowColumnarBatchRow(Array.empty)
//        wrapper.readExternal(ois)
//        wrapper
//      }
//
//      override protected def close(): Unit = ois.close()
//    }
//  }
}