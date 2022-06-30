package org.apache.spark.sql.column

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.deduplicate.VectorDeduplicator
import org.apache.arrow.algorithm.search.BucketSearcher
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, IndexSorter, SparkComparator, SparkUnionComparator}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.compression.{CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.{BitVectorHelper, FieldVector, IntVector, TypeLayout, ValueVector, VectorLoader, VectorSchemaRoot, ZeroVector}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator
import org.apache.spark.util.random.XORShiftRandom

import java.io._
import java.nio.channels.Channels
import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.NumericRange
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

// TODO: at some point, we might have to split up functionalities into more files
// TODO: memory management
// TODO: change numRows to Int?
// TODO: create sorts by Iterators?, Create sample by iterators?
// TODO: difference between 'take' and 'merge'?

class ArrowColumnarBatchRow(@transient protected val columns: Array[ArrowColumnVector], val numRows: Long) extends InternalRow with AutoCloseable with Serializable {
  override def numFields: Int = columns.length

  def length: Long = numRows

  override def isNullAt(ordinal: Int): Boolean = ordinal < 0 || ordinal >= numFields

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = {
    val column = columns(ordinal)
    new ColumnarArray(column, 0, column.getValueVector.getValueCount)
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

  /** Creates a VectorSchemaRoot from this batch */
  def toRoot: VectorSchemaRoot = VectorSchemaRoot.of(columns.map(column => column.getValueVector.asInstanceOf[FieldVector]).toSeq: _*)

  /** Note: uses slicing instead of complete copy,
   * according to: https://arrow.apache.org/docs/java/vector.html#slicing */
  override def copy(): InternalRow = {
    new ArrowColumnarBatchRow( columns map { v =>
      val vector = v.getValueVector
      val allocator = vector.getAllocator
      val tp = vector.getTransferPair(allocator)

      tp.splitAndTransfer(0, numRows.toInt)
      new ArrowColumnVector(tp.getTo)
    }, numRows)
  }

  /**
   * Performs a projection on the batch given some expressions
   * @param indices the sequence of indices which define the projection
   * @return a fresh batch projected from the current batch
   */
  def projection(indices: Seq[Int]): ArrowColumnarBatchRow = {
    new ArrowColumnarBatchRow( indices.toArray map ( index => {
      val vector = columns(index).getValueVector
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(0, numRows.toInt)
      new ArrowColumnVector(tp.getTo)
    }), numRows)
  }

  /**
   * Takes a range of rows from the batch
   * @param range the range to take
   * @return the split batch
   * NOTE: assumes 0 <= range < numRows
   */
  def take(range: Range): ArrowColumnarBatchRow = {
    new ArrowColumnarBatchRow( columns map ( column => {
      val vector = column.getValueVector
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(range.head, range.length)
      new ArrowColumnVector(tp.getTo)
    }), range.length)
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

  /**
   * @param cols columns to append
   * @return a fresh batch with appended columns
   * Note: we assume the columns have as many rows as the batch
   */
  def appendColumns(cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow =
    new ArrowColumnarBatchRow(columns ++ cols, numRows)

  /**
   * Splits the current batch on its columns into two batches
   * @param col column index to split on
   * @return a pair of the two batches containing the split columns from this batch
   */
  def splitColumns(col: Int): (ArrowColumnarBatchRow, ArrowColumnarBatchRow) =
    (new ArrowColumnarBatchRow(columns.slice(0, col), numRows),
      new ArrowColumnarBatchRow(columns.slice(col, numFields), numRows))

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

        0 until numRows.toInt foreach { index =>
          if (ours.hashCode(index) != theirs.hashCode(index))
            return false
        }
      }
      true
    case _ => false
  }
}

object ArrowColumnarBatchRow {
  // TODO: apply to all places where this is required! Perhaps make more wrappers...

  /** Creates a fresh ArrowColumnarBatchRow from an iterator of ArrowColumnarBatchRows
   * WARNING: uses 'take', a very expensive operation. Use with care! */
  def create(iter: Iterator[ArrowColumnarBatchRow]): ArrowColumnarBatchRow = {
    ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(iter)._2)
  }

  /** Creates a fresh ArrowColumnarBatchRow from an array of ArrowColumnVectors */
  def create(cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow = {
    val length = if (cols.length > 0) cols(0).getValueVector.getValueCount else 0
    new ArrowColumnarBatchRow(cols, length)
  }

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
      val msb = if (index == old.length) last_bit+1 else largest_bit
      var bitMask = (1 << msb) -1 // everything is valid, from msb-1 to the last bit
      val lsb = if (index == 0) start_bit else 0 // everything is valid from msb-1 to lsb
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
   * Closes the batches from the iterator
   * WARNING: this is an expensive operation, because it copies all data. Use with care!
   *
   * Users can define their own functions to process custom data:
   * @param extraTaker split the item from the iterator into (customData, batch)
   * @param extraCollector collect a new item from custom-data, first parameter is new item, second parameter is
   *                       the result from previous calls, None if there were no previous calls. Result of this function
   *                       is passed to other calls of extraCollector.
   */
  def take(batches: Iterator[Any], numCols: Option[Int] = None, numRows: Option[Int] = None,
           extraTaker: (Any) => (Any, ArrowColumnarBatchRow) = batch => (None, batch.asInstanceOf[ArrowColumnarBatchRow]),
           extraCollector: (Any, Option[Any]) => Any = (_: Any, _: Option[Any]) => None): (Any, Array[ArrowColumnVector]) = {
    if (!batches.hasNext) {
      if (numCols.isDefined)
        return (None, Array.tabulate[ArrowColumnVector](numCols.get)(i => new ArrowColumnVector( new ZeroVector(i.toString) ) ))
      return (None, new Array[ArrowColumnVector](0))
    }

    var extraCollected: Any = None
    var size = 0L
    var array: Array[ArrowColumnVector] = Array.empty
    // number of bytes written
    var num_bytes: Long = 0L

    // get first batch
    val (extra, first) = extraTaker(batches.next())
    Resources.autoCloseTry(first) { batch =>
      extraCollected = extraCollector(extra, None)
      if (numCols.isEmpty && batch.numRows > Integer.MAX_VALUE)
        throw new RuntimeException("ArrowColumnarBatchRow::take() First batch is too large")

      // Note: until we get any problems, we are going to assume the batches are in-order :)
      size = batch.numRows.min(numRows.getOrElse(Integer.MAX_VALUE).toLong)

      // The first batch should be separate, so we can determine the vector-types
      array = Array.tabulate[ArrowColumnVector](numCols.getOrElse(batch.numFields)) { i =>
        val vector = batch.columns(i).getValueVector
        val tp = vector.getTransferPair(vector.getAllocator)
        numRows.fold( tp.splitAndTransfer(0, batch.numRows.toInt) )( num =>
          tp.splitAndTransfer(0, num)
        )
        // we 'copy' the content of the first batch ...
        val newVec = tp.getTo

        // ... and re-use the ValueVector so we do not have to determine vector types :)
        newVec.clear()
        numRows.foreach( nums => newVec.setInitialCapacity(nums))
        newVec.allocateNew()
        num_bytes = vector.getDataBuffer.readableBytes()
        // make sure we have enough space
        while (newVec.getBufferSizeFor(newVec.getValueCapacity) < num_bytes) newVec.reAlloc()
        // copy contents
        validityRangeSetter(newVec.getValidityBuffer, 0L until size)
        newVec.getDataBuffer.setBytes(0, vector.getDataBuffer)

        new ArrowColumnVector(newVec)
      }
    }


    batches.takeWhile( _ => numRows.forall( num => size < num)).foreach { item =>
      var readableBytes = 0L
      var current_size = 0L
      val (extra, batch) = extraTaker(item)
      Resources.autoCloseTry(batch) { _ =>
        extraCollected = extraCollector(extra, Option(extraCollected))
        // the columns we want
        val columns = numCols.fold(batch.columns)( nums => batch.columns.slice(0, nums) )
        // the rows that are left to read
        current_size = batch.numRows.min( numRows.getOrElse(Integer.MAX_VALUE) - size )

        (array, columns).zipped foreach { case (output, input) =>
          if (size + current_size > Integer.MAX_VALUE)
            throw new RuntimeException("[ArrowColumnarBatchRow::take() batches are too big to be combined!")
          val ivector = input.getValueVector
          readableBytes = ivector.getDataBuffer.readableBytes().max(readableBytes)
          val ovector = output.getValueVector
          // make sure we have enough space
          while (ovector.getValueCapacity < size + current_size) ovector.reAlloc()
          // copy contents
          validityRangeSetter(ovector.getValidityBuffer, size until size+current_size)
          output.getValueVector.getDataBuffer.setBytes(num_bytes, ivector.getDataBuffer)
        }

      }
      num_bytes += readableBytes
      size += current_size
    }

    array foreach { vector =>
      vector.getValueVector.setValueCount(size.toInt)
    }

    (extraCollected, array)
  }

  /**  Note: similar to getByteArrayRdd(...) -- works like a 'flatten'
   * Encodes the first numRows rows of the first numCols columns of a series of ArrowColumnarBatchRows
   * according to: https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format
   *
   * Note: "The recommended usage for VectorSchemaRoot is creating a single VectorSchemaRoot
   * based on the known schema and populated data over and over into the same VectorSchemaRoot
   * in a stream of batches rather than creating a new VectorSchemaRoot instance each time"
   * source: https://arrow.apache.org/docs/6.0/java/vector_schema_root.html
   *
   * Users may add additional encoding by providing the 'extraEncoder' function
   *
   * Closes the batches found in the iterator */
  def encode(iter: Iterator[Any],
             numCols: Option[Int] = None,
             numRows: Option[Int] = None,
             extraEncoder: (Any => (Array[Byte], ArrowColumnarBatchRow)) =
                batch => (Array.emptyByteArray, batch.asInstanceOf[ArrowColumnarBatchRow])): Iterator[Array[Byte]] = {
    if (!iter.hasNext)
      return Iterator(Array.emptyByteArray)

    // how many rows are left to read?
    var left = numRows
    // Setup the streams and writers
    val bos = new ByteArrayOutputStream()
    val oos = {
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      new ObjectOutputStream(codec.compressedOutputStream(bos))
    }

    // Prepare first batch
    // This needs to be done separately as we need the schema for the VectorSchemaRoot
    val (extra, first) = extraEncoder(iter.next())
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
    first.close()

    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
    writer.start()
    writer.writeBatch()
    oos.writeLong(first_length)
    oos.writeInt(extra.length)
    oos.write(extra)
    if (left.isDefined) left = Option((left.get - first_length).toInt)


    // while we still have some reading to do
    while (iter.hasNext && (left.isEmpty || left.get > 0)) {
      val (extra, batch) = extraEncoder(iter.next())
      val batch_length = batch.numRows.min(left.getOrElse(Int.MaxValue).toLong)
      if (batch_length > Integer.MAX_VALUE)
        throw new RuntimeException("[ArrowColumnarBatchRow] Cannot encode more than Integer.MAX_VALUE rows")
      new VectorLoader(root).load(batch.toArrowRecordBatch(root.getFieldVectors.size(), numRows = Option(batch_length.toInt)))
      writer.writeBatch()
      oos.writeLong(batch_length)
      oos.writeInt(extra.length)
      oos.write(extra)
      if (left.isDefined) left = Option((left.get-batch_length).toInt)
    }

    // clean up and return the singleton-iterator
    oos.flush()
    writer.close()
    oos.close()
    Iterator(bos.toByteArray)
  }

  /** Note: similar to decodeUnsafeRows
   *
   * Users may add additional decoding by providing the 'extraDecoder' function*/
  def decode(bytes: Array[Byte],
             extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (_, batch) => batch): Iterator[Any] = {
    new NextIterator[Any] {
      private val bis = new ByteArrayInputStream(bytes)
      private val ois = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectInputStream(codec.compressedInputStream(bis))
      }
      private val allocator = new RootAllocator()
      private val reader = new ArrowStreamReader(ois, allocator)

      override protected def getNext(): Any = {
        if (!reader.loadNextBatch()) {
          finished = true
          return null
        }

        val columns = reader.getVectorSchemaRoot.getFieldVectors
        val length = ois.readLong()
        val arr_length = ois.readInt()
        val array = new Array[Byte](arr_length)
        ois.readFully(array)

        extraDecoder(array, new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator
          val tp = vector.getTransferPair(allocator)

          tp.transfer()
          new ArrowColumnVector(tp.getTo)
        }).toArray, length))
      }

      override protected def close(): Unit = {
        reader.close()
        ois.close()
        bis.close()
      }
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
   * Performs a multi-columns sort on a batch
   * @param batch batch to sort
   * @param sortOrders orders to sort on
   * @return a new, sorted, batch
   */
  def multiColumnSort(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    // prepare comparator and UnionVector
    val union = new UnionVector("Combiner", batch.columns(0).getValueVector.getAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
    sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
      val name = sortOrder.child.asInstanceOf[AttributeReference].name
      batch.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
        val valueVector = vector.getValueVector
        val tp = valueVector.getTransferPair(valueVector.getAllocator)
        tp.splitAndTransfer(0, batch.numRows.toInt)
        union.addVector(tp.getTo.asInstanceOf[FieldVector])
        comparators(index) = (
          name,
          new SparkComparator[ValueVector](sortOrder, DefaultVectorComparators.createDefaultComparator(valueVector))
        )
      })
    }
    val comparator = new SparkUnionComparator(comparators)
    union.setValueCount(batch.numRows.toInt)

    // compute the index-vector
    val first_vector = batch.columns(0).getValueVector
    val indices = new IntVector("indexHolder", first_vector.getAllocator)
    assert(first_vector.getValueCount > 0)
    indices.allocateNew(first_vector.getValueCount)
    indices.setValueCount(first_vector.getValueCount)
    (new IndexSorter).sort(union, indices, comparator)

    // sort all columns
    new ArrowColumnarBatchRow( batch.columns map { column =>
      val vector = column.getValueVector
      assert(vector.getValueCount == indices.getValueCount)

      // transfer type
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(0, vector.getValueCount)
      val new_vector = tp.getTo

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


  def unique(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    // prepare comparator and UnionVector
    val union = new UnionVector("Combiner", batch.columns(0).getValueVector.getAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
    sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
      val name = sortOrder.child.asInstanceOf[AttributeReference].name
      batch.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
        val valueVector = vector.getValueVector
        val tp = valueVector.getTransferPair(valueVector.getAllocator)
        tp.splitAndTransfer(0, batch.numRows.toInt)
        union.addVector(tp.getTo.asInstanceOf[FieldVector])
        comparators(index) = (
          name,
          new SparkComparator[ValueVector](sortOrder, DefaultVectorComparators.createDefaultComparator(valueVector))
        )
      })
    }
    val comparator = new SparkUnionComparator(comparators)
    union.setValueCount(batch.numRows.toInt)

    // compute the index-vector
    val indices = new VectorDeduplicator(comparator, union).uniqueIndices()


    new ArrowColumnarBatchRow( batch.columns map { column =>
      val vector = column.getValueVector
      assert(indices.getValueCount > 0)

      // transfer type
      val tp = vector.getTransferPair(vector.getAllocator)
      tp.splitAndTransfer(0, indices.getValueCount)
      val new_vector = tp.getTo

      new_vector.setInitialCapacity(indices.getValueCount)
      new_vector.allocateNew()
      0 until indices.getValueCount foreach { index => new_vector.copyFromSafe(indices.get(index), index, vector) }
      new_vector.setValueCount(indices.getValueCount)

      new ArrowColumnVector(new_vector)
    }, indices.getValueCount)
  }

  /**
   * Sample rows from batches where the sample-size is determined by probability
   * @param input the batches to sample from
   * @param fraction the probability of a sample being taken
   * @param seed a seed for the "random"-generator
   * @return a fresh batch with the sampled rows
   */
  def sample(input: Iterator[ArrowColumnarBatchRow], fraction: Double, seed: Long): ArrowColumnarBatchRow = {
    if (!input.hasNext) new ArrowColumnarBatchRow(Array.empty, 0)

    // The first batch should be separate, so we can determine the vector-types
    val first = input.next()

    val array = Array.tabulate[ArrowColumnVector](first.numFields) { i =>
      val vector = first.columns(i).getValueVector
      val tp = vector.getTransferPair(vector.getAllocator)
      // we 'copy' the content of the first batch ...
      tp.splitAndTransfer(0, first.numRows.toInt)
      // ... and re-use the ValueVector so we do not have to determine vector types :)
      vector.clear()
      vector.allocateNew()
      new ArrowColumnVector(vector)
    }

    val iter = Iterator(first) ++ input
    val rand = new XORShiftRandom(seed)
    var i = 0
    while (iter.hasNext) {
      val batch = iter.next()
      0 until batch.numRows.toInt foreach { index =>
        // do sample
        if (rand.nextDouble() <= fraction) {
          array zip batch.columns foreach { case (ours, theirs) => ours.getValueVector.copyFromSafe(index, i, theirs.getValueVector)}
          i += 1
        }
      }
    }
    array foreach ( column => column.getValueVector.setValueCount(i) )
    new ArrowColumnarBatchRow(array, i)
  }

  /**
   * Reservoir sampling implementation that also returns the input size
   * Note: inspiration from org.apache.spark.util.random.RandomUtils::reservoirSampleAndCount
   * @param input input batches
   * @param k reservoir size
   * @param seed random seed
   * @return array of sampled batches and size of the input
   * Note: closes the batches in the iterator
   */
  def sampleAndCount(input: Iterator[ArrowColumnarBatchRow], k: Int, seed: Long = Random.nextLong()):
      (ArrowColumnarBatchRow, Long) = {
    if (k < 1) (Array.empty[ArrowColumnarBatchRow], 0)

    // First, we fill the reservoir with k elements
    var inputSize = 0L
    var nrBatches = 0
    var length = 0L
    var remainderBatch: Option[ArrowColumnarBatchRow] = None
    val reservoirBuf = new ArrayBuffer[ArrowColumnarBatchRow](k)
    while (inputSize < k) {
      if (!input.hasNext) return (ArrowColumnarBatchRow.create(reservoirBuf.slice(0, nrBatches).toIterator), inputSize)
      val batch = input.next()
      assert(batch.numRows < Integer.MAX_VALUE)
      length = math.min(k-inputSize, batch.numRows)
      reservoirBuf += batch
      inputSize += length
      nrBatches += 1

      // do we have elements remaining?
      if (length < batch.numRows)
        remainderBatch = Option(batch)
    }

    var iter: Iterator[ArrowColumnarBatchRow] = input
    // add our remainder to the iterator, if there is any
    remainderBatch.foreach { batch =>
      iter = Iterator(batch.take( (length.toInt) until batch.numRows.toInt )) ++ iter
    }

    val reservoir = ArrowColumnarBatchRow.create(reservoirBuf.toIterator)

    // we now have a reservoir with length k, in which we will replace random elements
    val rand = new XORShiftRandom(seed)
    def generateRandomNumber(start: Int = 0, end: Int = Integer.MAX_VALUE-1) : Int = {
      start + rand.nextInt( (end-start) + 1)
    }

    while (input.hasNext) {
      Resources.autoCloseTry(input.next()) { batch =>
        val start: Int = generateRandomNumber(end = (batch.numRows-1).toInt)
        val end = generateRandomNumber(start, batch.numRows.toInt)
        val sample = batch.take(start until end)
        0 until sample.numRows.toInt foreach { index =>
          reservoir.copyAtIndex(batch, generateRandomNumber(end = k-1), index)
        }
        inputSize += sample.numRows
      }
    }

    (reservoir, inputSize)
  }

  def bucketDistributor(key: ArrowColumnarBatchRow, rangeBounds: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): Array[Int] = {
    // prepare comparator and UnionVectors
    val keyUnion = new UnionVector("KeyCombiner", key.columns(0).getValueVector.getAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val rangeUnion = new UnionVector("rangeCombiner", rangeBounds.columns(0).getValueVector.getAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
    sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
      val name = sortOrder.child.asInstanceOf[AttributeReference].name
      // prepare keyUnion and comparator
      key.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
        val valueVector = vector.getValueVector
        val tp = valueVector.getTransferPair(valueVector.getAllocator)
        tp.splitAndTransfer(0, key.numRows.toInt)
        keyUnion.addVector(tp.getTo.asInstanceOf[FieldVector])
        comparators(index) = (
          name,
          new SparkComparator[ValueVector](sortOrder, DefaultVectorComparators.createDefaultComparator(valueVector))
        )
      })
      // prepare rangeUnion
      rangeBounds.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
        val valueVector = vector.getValueVector
        val tp = valueVector.getTransferPair(valueVector.getAllocator)
        tp.splitAndTransfer(0, rangeBounds.numRows.toInt)
        rangeUnion.addVector(tp.getTo.asInstanceOf[FieldVector])
      })
    }
    val comparator = new SparkUnionComparator(comparators)
    keyUnion.setValueCount(key.numRows.toInt)
    rangeUnion.setValueCount(rangeBounds.numRows.toInt)

    // find partition-ids
    new BucketSearcher(keyUnion, rangeUnion, comparator).distribute()
  }

  def distribute(key: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, Array[ArrowColumnarBatchRow]] = {
    val distributed = mutable.Map[Int, ArrayBuffer[ArrowColumnarBatchRow]]()

    partitionIds.zipWithIndex foreach { case (partitionId, index) =>
      val newRow = key.take(index until index+1)
      if (distributed.contains(partitionId))
        distributed(partitionId) += newRow
      else
        distributed(partitionId) = ArrayBuffer(newRow)
    }

    distributed.map ( items => (items._1, items._2.toArray) ).toMap
  }
}