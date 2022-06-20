package org.apache.spark.sql.column

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, ZeroVector}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class ArrowColumnarBatchRow(@transient protected val columns: Array[ArrowColumnVector], val numRows: Long) extends InternalRow with AutoCloseable with Serializable {
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

  def copyNToRoot(root: VectorSchemaRoot, numCols: Option[Int] = None, numRows: Option[Int] = None): Unit = {
    val fieldVecs = if (numCols.isDefined) root.getFieldVectors.slice(0, numCols.get) else root.getFieldVectors.slice(0, root.getFieldVectors.size())
    val cols = if (numCols.isDefined) columns.slice(0, numCols.get) else columns
    for ( (vec, col) <- fieldVecs zip cols) {
      vec.reset()

      val tp = col.getValueVector.getTransferPair(col.getValueVector.getAllocator)
      if (numRows.isDefined)
        tp.splitAndTransfer(0, numRows.get)
      else
        tp.transfer()
      vec.copyFrom(0, 0, tp.getTo)
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
    }, numRows)
  }

  override def close(): Unit = columns foreach(column => column.close())
}

object ArrowColumnarBatchRow {
  private case class IndexPair(rowIndex: Int, colIndex: Int)

  /**
   * Returns the merged arrays from multiple ArrowColumnarBatchRows
   * @param numCols the number of columns to take
   * @param batches batches to create array from
   * @return array of merged columns
   */
  def take(batches: Iterator[ArrowColumnarBatchRow], numCols: Option[Int] = None, numRows: Option[Int] = None): Array[ArrowColumnVector] = {
    if (!batches.hasNext) {
      if (numCols.isDefined)
        return Array.tabulate[ArrowColumnVector](numCols.get)(i => new ArrowColumnVector( new ZeroVector(i.toString) ) )
      return new Array[ArrowColumnVector](0)
    }

    val first = batches.next()
    if (numCols.isEmpty && first.numRows > Integer.MAX_VALUE)
      throw new RuntimeException("ArrowColumnarBatchRow::take() First batch is too large")

    val N = if (numCols.isDefined) numCols.get else first.numFields
    val array = Array.tabulate[ArrowColumnVector](N) { i =>
      val column = first.columns(i)
      val vector = column.getValueVector
      val allocator = vector.getAllocator
      val tp = vector.getTransferPair(allocator)

      if (numRows.isDefined)
        tp.splitAndTransfer(0, numRows.get)
      else
        tp.transfer()
      new ArrowColumnVector(tp.getTo)
    }

    // Note: until we get any problems, we are going to assume the batches are in-order :)
    var size = first.numRows

    batches.foreach { batch =>
      val columns = if (numCols.isDefined) batch.columns.slice(0, numCols.get) else batch.columns
      (array, columns).zipped foreach { case (output, input) =>
        // TODO: it could be that the valuevector is not updated...
        if (size + batch.numRows > Integer.MAX_VALUE)
          throw new RuntimeException("[ArrowColumnarBatchRow::take() batches are too big to be combined!")
        output.getValueVector.copyFromSafe(0, size.toInt, input.getValueVector)
        size += batch.numRows
      }
    }

    array
  }

  /**  Note: similar to getByteArrayRdd(...) -- works like a 'flatten'
   * Encodes the first numRows rows of the first numCols columns of a series of ArrowColumnarBatchRows
   * according to: https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format
   *
   * Note: "The recommended usage for VectorSchemaRoot is creating a single VectorSchemaRoot
   * based on the known schema and populated data over and over into the same VectorSchemaRoot
   * in a stream of batches rather than creating a new VectorSchemaRoot instance each time"
   * source: https://arrow.apache.org/docs/6.0/java/vector_schema_root.html */
  def encode(iter: Iterator[org.apache.spark.sql.column.ArrowColumnarBatchRow],
             numCols: Option[Int] = None,
             numRows: Option[Int] = None): Iterator[Array[Byte]] = {
    if (!iter.hasNext)
      return Iterator(Array.emptyByteArray)

    // how many rows are left to read?
    var left = numRows

    // Prepare first batch
    // This needs to be done separately as we need the schema for the VectorSchemaRoot
    val first = iter.next()
    val first_length = first.numRows.min(left.getOrElse(Int.MaxValue).toLong)
    if (first_length > Integer.MAX_VALUE)
      throw new RuntimeException("[ArrowColumnarBatchRow] Cannot encode more than Integer.MAX_VALUE rows")
    val columns = if (numCols.isDefined) first.columns.slice(0, numCols.get) else first.columns
    val root = VectorSchemaRoot.of(columns.map(column => {
      if (left.isEmpty) column.getValueVector.asInstanceOf[FieldVector]
      val vector = column.getValueVector
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(0, first_length.toInt)
      tp.getTo.asInstanceOf[FieldVector]
    }).toSeq: _*)

    // Setup the streams and writers
    val bos = new ByteArrayOutputStream()
    val oos = {
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      new ObjectOutputStream(codec.compressedOutputStream(bos))
    }
    val writer: ArrowStreamWriter = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
    writer.start()

    // write first batch
    writer.writeBatch()
    oos.writeLong(first_length)
    if (left.isDefined) left = Some((left.get - first_length).toInt)


    // while we still have some reading to do
    while (iter.hasNext && (left.isEmpty || left.get > 0)) {
      val batch = iter.next()
      val batch_length = batch.numRows.min(left.getOrElse(Int.MaxValue).toLong)
      if (batch_length > Integer.MAX_VALUE)
        throw new RuntimeException("[ArrowColumnarBatchRow] Cannot encode more than Integer.MAX_VALUE rows")
      batch.copyNToRoot(root, numCols, numRows = Some(batch_length.toInt))
      writer.writeBatch()
      oos.writeLong(batch_length)
      if (left.isDefined) left = Some((left.get-batch_length).toInt)
    }

    // clean up and return the singleton-iterator
    writer.close()
    oos.flush()
    oos.close()
    Iterator(bos.toByteArray)
  }

  /** Note: similar to decodeUnsafeRows */
  def decode(bytes: Array[Byte]): Iterator[ArrowColumnarBatchRow] = {
    new NextIterator[ArrowColumnarBatchRow] {
      private lazy val bis = new ByteArrayInputStream(bytes)
      private lazy val ois = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectInputStream(codec.compressedInputStream(bis))
      }
      private lazy val allocator = new RootAllocator()
      private lazy val reader = new ArrowStreamReader(ois, allocator)


      override protected def getNext(): ArrowColumnarBatchRow = {
        val hasNext = reader.loadNextBatch()
        if (!hasNext) {
          finished = true
          return null
        }

        val columns = reader.getVectorSchemaRoot.getFieldVectors
        val length = ois.readLong()
        new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator
          val tp = vector.getTransferPair(allocator)

          tp.transfer()
          new ArrowColumnVector(tp.getTo)
        }).toArray, length)
      }

      override protected def close(): Unit = {
        reader.close()
        ois.close()
        bis.close()
      }
    }

  }
}