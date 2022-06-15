package org.apache.spark.sql.column

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ArrowColumnarBatchRow(@transient protected val columns: Array[ArrowColumnVector]) extends InternalRow with AutoCloseable with Serializable {
  /** Option 1: batch is each column concatenated to make a big 1D Array */
//override def numFields: Int = sizes.sum
//  private lazy val sizes: Array[Int] = columns.map( column => column.getValueVector.getValueCount )
//
//  // TODO: make test?
//  private def mapOrdinalToIndexPair(ordinal: Int): ArrowColumnarBatchRow.IndexPair = {
//    var row = ordinal
//    var col = 0
//    var sum = 0
//    sizes.iterator.takeWhile( size => sum + size <= ordinal ).foreach { size => col += 1; sum += size; row -= size; }
//    ArrowColumnarBatchRow.IndexPair(row, col)
//  }
//  override def isNullAt(ordinal: Int): Boolean = {
//    if (ordinal < 0 || ordinal >= columns.length)
//      return true
//    val mappedPair: ArrowColumnarBatchRow.IndexPair = mapOrdinalToIndexPair(ordinal)
//    columns(mappedPair.colIndex).isNullAt(mappedPair.rowIndex)
//  }
//
//  override def getInt(ordinal: Int): Int = {
//    val mappedPair: ArrowColumnarBatchRow.IndexPair = mapOrdinalToIndexPair(ordinal)
//    columns(mappedPair.colIndex).getInt(mappedPair.rowIndex)
//  }
//override def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException()

  /** Option 2: batch is a row that contains Array-typed elements (= columns) */

  override def numFields: Int = columns.length

  override def isNullAt(ordinal: Int): Boolean = ordinal < 0 || ordinal >= numFields

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = {
    val column = columns(ordinal)
    new ColumnarArray(column, 0, column.getValueVector.getValueCount)
  }


  // unsupported getters
//  override def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException()
  override def get(ordinal: Int, dataType: DataType): AnyRef = throw new UnsupportedOperationException()
  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()
  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()
//  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()
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
  private case class IndexPair(rowIndex: Int, colIndex: Int)

  /**  Note: similar to getByteArrayRdd(...)
   * Encodes the first n columns of a series of ArrowColumnarBatchRows
   * according to: https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format
   *
   * Note: "The recommended usage for VectorSchemaRoot is creating a single VectorSchemaRoot
   * based on the known schema and populated data over and over into the same VectorSchemaRoot
   * in a stream of batches rather than creating a new VectorSchemaRoot instance each time"
   * source: https://arrow.apache.org/docs/6.0/java/vector_schema_root.html */
  def encode(n: Int, iter: Iterator[ArrowColumnarBatchRow]): Iterator[Array[Byte]] = {
    new NextIterator[Array[Byte]] {
      private var root: Option[VectorSchemaRoot] = None
      private lazy val bos = new ByteArrayOutputStream()
      private var writer: Option[ArrowStreamWriter] = None

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
  def decode(bytes: Array[Byte]): Iterator[ArrowColumnarBatchRow] = {
    new NextIterator[ArrowColumnarBatchRow] {
      private lazy val bis = new ByteArrayInputStream(bytes)
      private lazy val allocator = new RootAllocator()
      private lazy val reader = new ArrowStreamReader(bis, allocator)


      override protected def getNext(): ArrowColumnarBatchRow = {
        val hasNext = reader.loadNextBatch()
        if (!hasNext) {
          finished = true
          return null
        }

        val columns = reader.getVectorSchemaRoot.getFieldVectors
        new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator
          val tp = vector.getTransferPair(allocator)

          tp.transfer()
          new ArrowColumnVector(tp.getTo)
        }).toArray)
      }

      override protected def close(): Unit = {
        reader.close()
        bis.close()
      }
    }

  }
}