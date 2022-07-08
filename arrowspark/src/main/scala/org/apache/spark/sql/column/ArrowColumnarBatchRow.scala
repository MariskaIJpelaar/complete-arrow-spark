package org.apache.spark.sql.column

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.column
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

// TODO: at some point, we might have to split up functionalities into more files
// TODO: memory management
// TODO: create sorts by Iterators?, Create sample by iterators?
// TODO: difference between 'take' and 'merge'?

/** Note: while ArrowColumnVector is technically AutoCloseable and not Closeable (which means you should not close more than once), the implemented close does not produce
 * weird side effects, so we are going to ignore this restriction. Plus, the underlying ValueVector is Closeable.
 * It is important to verify this for every Spark-version-update! */
class ArrowColumnarBatchRow(@transient protected[column] val columns: Array[ArrowColumnVector], val numRows: Int) extends InternalRow with Closeable with Serializable {
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

      tp.splitAndTransfer(range.head, range.last)
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
  /** Creates an empty ArrowColumnarBatchRow */
  def empty: ArrowColumnarBatchRow = new ArrowColumnarBatchRow(Array.empty, 0)

  // TODO: apply to all places where this is required! Perhaps make more wrappers...

  /** Creates a fresh ArrowColumnarBatchRow from an iterator of ArrowColumnarBatchRows
   * Closes the batches in the provided iterator
   * WARNING: uses 'take', a very expensive operation. Use with care!
   * TODO: Caller is responsible for closing generated batch */
  def create(iter: Iterator[ArrowColumnarBatchRow]): ArrowColumnarBatchRow = {
    ArrowColumnarBatchRow.create(ArrowColumnarBatchRow.take(iter)._2)
  }

  /** Creates a fresh ArrowColumnarBatchRow from an array of ArrowColumnVectors
   * TODO: Caller is responsible for closing the generated batch */
  def create(cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow = {
    val length = if (cols.length > 0) cols(0).getValueVector.getValueCount else 0
    new ArrowColumnarBatchRow(cols, length)
  }

  /**
   * Returns the merged arrays from multiple ArrowColumnarBatchRows
   * @param numCols the number of columns to take
   * @param batches batches to create array from
   * @return array of merged columns
   * Closes the batches from the iterator
   * WARNING: this is an expensive operation, because it copies all data. Use with care!
   *
   * Callers can define their own functions to process custom data:
   * @param extraTaker split the item from the iterator into (customData, batch)
   * @param extraCollector collect a new item from custom-data, first parameter is new item, second parameter is
   *                       the result from previous calls, None if there were no previous calls. Result of this function
   *                       is passed to other calls of extraCollector.
   * Note: user should close the batch if it consumes it (does not return it)
   *
   * TODO: Caller is responsible for closing returned vectors
   */
  def take(batches: Iterator[Any], numCols: Option[Int] = None, numRows: Option[Int] = None,
           extraTaker: Any => (Any, ArrowColumnarBatchRow) = batch => (None, batch.asInstanceOf[ArrowColumnarBatchRow]),
           extraCollector: (Any, Option[Any]) => Any = (_: Any, _: Option[Any]) => None): (Any, Array[ArrowColumnVector]) = {
    if (!batches.hasNext) {
      if (numCols.isDefined)
        return (None, Array.tabulate[ArrowColumnVector](numCols.get)(i => new ArrowColumnVector( new ZeroVector(i.toString) ) ))
      return (None, new Array[ArrowColumnVector](0))
    }

    try {
      // get first batch and its extra
      val (extra, first) = extraTaker(batches.next())
      // builder consumes batch
      val builder = new ArrowColumnarBatchRowBuilder(first, numCols, numRows)
      try {
        var extraCollected = extraCollector(extra, None)
        while (batches.hasNext && numRows.forall( num => builder.length < num)) {
          val (extra, batch) = extraTaker(batches.next())
          builder.append(batch) // builder consumes batch
          extraCollected = extraCollector(extra, Option(extraCollected))
        }
        (extraCollected, builder.buildColumns())
      } finally {
        builder.close()
      }
    } finally {
      /** clear up the remainder */
      batches.foreach( extraTaker(_)._2.close() )
    }
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
   * TODO: Closes the batches found in the iterator */
  def encode(iter: Iterator[Any],
             numCols: Option[Int] = None,
             numRows: Option[Int] = None,
             extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) =
                batch => (Array.emptyByteArray, batch.asInstanceOf[ArrowColumnarBatchRow])): Iterator[Array[Byte]] = {
    if (!iter.hasNext)
      return Iterator(Array.emptyByteArray)

    try {
      // how many rows are left to read?
      var left = numRows
      // Setup the streams and writers
      val bos = new ByteArrayOutputStream()
      val oos = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectOutputStream(codec.compressedOutputStream(bos))
      }

      try {
        // Prepare first batch
        // This needs to be done separately as we need the schema for the VectorSchemaRoot
        val (extra, first): (Array[Byte], ArrowColumnarBatchRow) = extraEncoder(iter.next())
        // consumes first
        val (root, firstLength) = ArrowColumnarBatchRowConverters.toRoot(first, numCols, numRows)
        try {
          val writer = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
          try {
            // write first batch
            writer.start()
            writer.writeBatch()
            root.close()
            oos.writeInt(firstLength)
            oos.writeInt(extra.length)
            oos.write(extra)
            left = left.map( numLeft => numLeft-firstLength )

            // while we still have some reading to do
            while (iter.hasNext && (left.isEmpty || left.get > 0)) {
              val (extra, batch): (Array[Byte], ArrowColumnarBatchRow) = extraEncoder(iter.next())
              // consumes batch
              val (recordBatch, batchLength): (ArrowRecordBatch, Int) =
                ArrowColumnarBatchRowConverters.toArrowRecordBatch(batch, root.getFieldVectors.size(), numRows = left)
              try {
                new VectorLoader(root).load(recordBatch)
                writer.writeBatch()
                root.close()
                oos.writeLong(batchLength)
                oos.writeInt(extra.length)
                oos.write(extra)
                left = left.map( numLeft => numLeft-batchLength )
              } finally {
                recordBatch.close()
              }
            }
            // clean up and return the singleton-iterator
            oos.flush()
            Iterator(bos.toByteArray)
          } finally {
            writer.close()
          }
        } finally {
          root.close()
        }
      } finally {
        oos.close()
      }
    } finally {
      iter.foreach( extraEncoder(_)._2.close() )
    }
  }

  /** Note: similar to decodeUnsafeRows
   *
   * Callers may add additional decoding by providing the 'extraDecoder' function. They are responsible for
   * closing the provided ArrowColumnarBatch if they consume it (do not return it)
   *
   * TODO: Callers are responsible for closing the returned 'Any' containing the batch */
  def decode(bytes: Array[Byte],
             extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (_, batch) => batch): Iterator[Any] = {
    if (bytes.length == 0)
      return Iterator.empty
    new NextIterator[Any] {
      private val bis = new ByteArrayInputStream(bytes)
      private val ois = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectInputStream(codec.compressedInputStream(bis))
      }
      private val allocator = column.rootAllocator.newChildAllocator("ArrowColumnarBatchRow::decode", 0, Integer.MAX_VALUE)
      private val reader = new ArrowStreamReader(ois, allocator)

      override protected def getNext(): Any = {
        if (!reader.loadNextBatch()) {
          finished = true
          return null
        }

        val columns = reader.getVectorSchemaRoot.getFieldVectors
        val length = ois.readInt()
        val arr_length = ois.readInt()
        val array = new Array[Byte](arr_length)
        ois.readFully(array)

        // Note: vector is transferred and is thus implicitly closed
        extraDecoder(array, new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::decode::extraDecoder", 0, Integer.MAX_VALUE)
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
}