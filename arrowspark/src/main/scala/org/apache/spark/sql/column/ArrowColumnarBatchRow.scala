package org.apache.spark.sql.column

import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, IndexSorter, SparkComparator}
import org.apache.arrow.memory.{ArrowBuf, RootAllocator}
import org.apache.arrow.vector.compression.{CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{BitVectorHelper, FieldVector, IntVector, TypeLayout, VectorLoader, VectorSchemaRoot, ZeroVector}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.column.ArrowColumnarBatchRow.intermediates
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.channels.Channels
import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.NumericRange

// TODO: at some point, we might have to split up functionalities into more files

// TODO: split the code which is called from the serializer and the ones from encoding/ decoding
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

  def length: Long = numRows

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

  /** copied from org.apache.arrow.vector.VectorUnloader */
  def toArrowRecordBatch(numCols: Int, numRows: Option[Int] = None): ArrowRecordBatch = {
    val nodes = new util.ArrayList[ArrowFieldNode]
    val buffers = new util.ArrayList[ArrowBuf]
    val codec = NoCompressionCodec.INSTANCE

    val rowCount = if (numRows.isEmpty) this.numRows else numRows.get
    if (rowCount > Integer.MAX_VALUE)
      throw new RuntimeException("[ArrowColumnarBatchRow::copyNToRoot] too many rows")

    /** copied from org.apache.arrow.vector.VectorUnloader::appendNodes(...) */
    def appendNodes(vector: FieldVector, nodes: util.List[ArrowFieldNode], buffers: util.List[ArrowBuf]): Unit = {
      nodes.add(new ArrowFieldNode(rowCount, vector.getNullCount))
      val fieldBuffers = vector.getFieldBuffers
      val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField.getType)
      if (fieldBuffers.size != expectedBufferCount)
        throw new IllegalArgumentException(String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField, vector.getClass.getSimpleName, fieldBuffers))
      for (buf <- fieldBuffers)
        buffers.add(codec.compress(vector.getAllocator, buf))
      for (child <- vector.getChildrenFromFields)
        appendNodes(child, nodes, buffers)
    }

    columns.slice(0, numCols) foreach( column => appendNodes(column.getValueVector.asInstanceOf[FieldVector], nodes, buffers) )
    new ArrowRecordBatch(rowCount.toInt, nodes, buffers, CompressionUtil.createBodyCompression(codec), true)
  }

  /** serializes this batch to an OutputStream*/
  def writeToStream(stream: OutputStream): Unit = {
    stream.write(intermediates)
    stream.flush()
    val root = VectorSchemaRoot.of(columns.map( column => column.getValueVector.asInstanceOf[FieldVector]).toSeq: _*)
    val oos = {
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      new ObjectOutputStream(codec.compressedOutputStream(stream))
    }
    val writer: ArrowStreamWriter = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
    writer.start()
    writer.writeBatch()
    oos.writeLong(numRows)

    writer.close()
    oos.close()
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

  def getSizeInBytes: Int = columns.map(column => column.getValueVector.getBufferSize ).sum
}

object ArrowColumnarBatchRow {
  /** Intermediate number to determine when we expect another batch in a stream */
  private val intermediates = 42

  /** Note: inspiration from org.apache.arrow.vector.BitVectorHelper::setBit */
  private def validityRangeSetter(validityBuffer: ArrowBuf, bytes: NumericRange[Long]): Unit = {
    if (bytes.isEmpty)
      return

    val start_byte = BitVectorHelper.byteIndex(bytes.head)
    val last_byte = BitVectorHelper.byteIndex(bytes.last)
    val start_bit = BitVectorHelper.bitIndex(bytes.head)
    val last_bit = BitVectorHelper.bitIndex(bytes.last)
    val num_bytes = last_byte - start_byte + 1
    val largest_bit = 8

    if (num_bytes > Integer.MAX_VALUE)
      throw new RuntimeException("[ArrowColumnarBatchRow::validityRangeSetter] too many bytes to set")

    val old: Array[Byte] = new Array[Byte](num_bytes.toInt)
    validityBuffer.getBytes(start_byte, old, 0, num_bytes.toInt)

    old.zipWithIndex foreach { case (byte, index) =>
      val msb = if (index == 0) last_bit+1 else largest_bit
      var bitMask = (1 << msb) -1 // everything is valid, from msb-1 to the last bit
      val lsb = if (index == old.length) start_bit else 0 // everything is valid from msb-1 to lsb
      bitMask = (bitMask >> lsb) << lsb

      old(index) = (byte | bitMask).toByte
    }

    validityBuffer.setBytes(start_byte, old, 0, num_bytes)
  }

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

    // number of bytes written
    var num_bytes: Long = 0L
    // Note: until we get any problems, we are going to assume the batches are in-order :)
    var size = first.numRows.min(numRows.getOrElse(Integer.MAX_VALUE).toLong)

    // The first batch should be separate, so we can determine the vector-types
    val array = Array.tabulate[ArrowColumnVector](numCols.getOrElse(first.numFields)) { i =>
      val vector = first.columns(i).getValueVector
      val tp = vector.getTransferPair(vector.getAllocator)
      numRows.fold( tp.transfer() )( num => tp.splitAndTransfer(0, num))
      // we 'copy' the content of the first batch ...
      val old_vec = tp.getTo

      // ... and re-use the ValueVector so we do not have to determine vector types :)
      vector.clear()
      numRows.foreach( nums => vector.setInitialCapacity(nums))
      vector.allocateNew()
      num_bytes = old_vec.getDataBuffer.readableBytes()
      // make sure we have enough space
      while (vector.getBufferSizeFor(vector.getValueCapacity) < num_bytes) vector.reAlloc()
      // copy contents
      validityRangeSetter(vector.getValidityBuffer, 0L until size)
      vector.getDataBuffer.setBytes(0, old_vec.getDataBuffer)

      new ArrowColumnVector(vector)
    }

    batches.takeWhile( _ => numRows.forall( num => size < num)).foreach { batch =>
      // the columns we want
      val columns = numCols.fold(batch.columns)( nums => batch.columns.slice(0, nums) )
      // the rows that are left to read
      val current_size = batch.numRows.min( numRows.getOrElse(Integer.MAX_VALUE) - size )

      var readableBytes = 0L
      (array, columns).zipped foreach { case (output, input) =>
        if (size + current_size > Integer.MAX_VALUE)
          throw new RuntimeException("[ArrowColumnarBatchRow::take() batches are too big to be combined!")
        val ivector = input.getValueVector
        readableBytes = ivector.getDataBuffer.readableBytes().max(readableBytes)
        val ovector = output.getValueVector
        // make sure we have enough space
        while (ovector.getBufferSizeFor(ovector.getValueCapacity) < num_bytes+readableBytes) ovector.reAlloc()
        // copy contents
        validityRangeSetter(ovector.getValidityBuffer, size until size+current_size)
        output.getValueVector.getDataBuffer.setBytes(num_bytes, ivector.getDataBuffer)
      }
      num_bytes += readableBytes
      size += current_size
    }

    array foreach { vector =>
      vector.getValueVector.setValueCount(size.toInt)
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
    bos.write(intermediates)
    val oos = {
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      new ObjectOutputStream(codec.compressedOutputStream(bos))
    }
    val writer: ArrowStreamWriter = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
    writer.start()
//    oos.writeInt(0)

    // write first batch
    writer.writeBatch()
    oos.writeLong(first_length)
    if (left.isDefined) left = Option((left.get - first_length).toInt)


    // while we still have some reading to do
    while (iter.hasNext && (left.isEmpty || left.get > 0)) {
      val batch = iter.next()
      val batch_length = batch.numRows.min(left.getOrElse(Int.MaxValue).toLong)
      if (batch_length > Integer.MAX_VALUE)
        throw new RuntimeException("[ArrowColumnarBatchRow] Cannot encode more than Integer.MAX_VALUE rows")
      new VectorLoader(root).load(batch.toArrowRecordBatch(root.getFieldVectors.size(), numRows = Option(batch_length.toInt)))
//      oos.writeInt(0)
      writer.writeBatch()
      oos.writeLong(batch_length)
      if (left.isDefined) left = Option((left.get-batch_length).toInt)
    }

    // clean up and return the singleton-iterator
    writer.close()
    oos.flush()
    oos.close()
    Iterator(bos.toByteArray)
  }

  def fromStream(stream: InputStream): Iterator[ArrowColumnarBatchRow] = {
    new NextIterator[ArrowColumnarBatchRow] {
      stream.read()
      private var ois = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectInputStream(codec.compressedInputStream(stream))
      }
      private lazy val allocator = new RootAllocator()
      private var reader = {
        new ArrowStreamReader(ois, allocator)
      }


      override protected def getNext(): ArrowColumnarBatchRow = {
        if (!reader.loadNextBatch()) {
          // TODO: fix:
          // https://stackoverflow.com/questions/2393179/streamcorruptedexception-invalid-type-code-ac
          ois.close()
          if (stream.read() == -1) {
            finished = true
            return null
          }
          val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
          ois = new ObjectInputStream(codec.compressedInputStream(stream))
          reader = new ArrowStreamReader(ois, allocator)
          if (!reader.loadNextBatch()) {
            throw new RuntimeException("[ArrowColumnarBatchRow::fromStream] Malformed Stream")
          }
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
      }
    }
  }

  /** Note: similar to decodeUnsafeRows */
  def decode(bytes: Array[Byte]): Iterator[ArrowColumnarBatchRow] = {
    new NextIterator[ArrowColumnarBatchRow] {
      private lazy val bis = new ByteArrayInputStream(bytes)
      private lazy val iter = fromStream(bis)

      override protected def getNext(): ArrowColumnarBatchRow = {
        if (!iter.hasNext) {
          finished = true
          return null
        }
        iter.next()
      }
      override protected def close(): Unit = bis.close()
    }

  }

  /** Should we ever need to implement an in-place sorting algorithm (with numRows more space), then we can do
   * the normal sort with:  */
  //    (vec zip indices).zipWithIndices foreach { case (elem, index), i) =>
  //      if (i == index)
  //        continue
  //
  //      val realIndex = index
  //      while (realIndex < i) realIndex = indices(realIndex)
  //
  //      vec.swap(i, realIndex)
  //    }
  // Note: worst case: 0 + 1 + 2 + ... + (n-1) = ((n-1) * n) / 2 = O(n*n) + time to sort (n log n)

  /**
   * @param batch an ArrowColumnarBatchRow to be sorted
   * @param col the column to sort on
   * @param sortOrder order settings to pass to the comparator
   * @return a fresh ArrowColumnarBatchRows with the sorted columns from batch
   *         Note: if col is out of range, returns the batch
   */
  def sort(batch: ArrowColumnarBatchRow, col: Int, sortOrder: SortOrder): ArrowColumnarBatchRow = {
    if (col < 0 || col > batch.numFields)
      return batch

    val vector = batch.columns(col).getValueVector
    val indices = new IntVector("indexHolder", vector.getAllocator)
    assert(vector.getValueCount > 0)
    indices.allocateNew(vector.getValueCount)
    indices.setValueCount(vector.getValueCount)
    val comparator = new SparkComparator(sortOrder, DefaultVectorComparators.createDefaultComparator(vector))
    (new IndexSorter).sort(vector, indices, comparator)
//    indices.setValueCount(vector.getValueCount)

    new ArrowColumnarBatchRow( batch.columns map { column =>
      val vector = column.getValueVector
      assert(vector.getValueCount == indices.getValueCount)

      // transfer type
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(0, vector.getValueCount)
      val new_vector = tp.getTo

//      new_vector.allocateNew()
      new_vector.setInitialCapacity(indices.getValueCount)
      new_vector.allocateNew()
      assert(indices.getValueCount > 0)
      assert(indices.getValueCount.equals(vector.getValueCount))
      /** from IndexSorter: the following relations hold: v(indices[0]) <= v(indices[1]) <= ... */
      0 until indices.getValueCount foreach { index => new_vector.copyFromSafe(indices.get(index), index, vector) }
      new_vector.setValueCount(indices.getValueCount)

      new ArrowColumnVector(new_vector)
    }, batch.numRows)
  }
}