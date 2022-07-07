package org.apache.spark.sql.column

import org.apache.arrow.algorithm.deduplicate.VectorDeduplicator
import org.apache.arrow.algorithm.search.BucketSearcher
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, IndexSorter, SparkComparator, SparkUnionComparator}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector._
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.column
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray, ColumnarArray}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.NextIterator
import org.apache.spark.util.random.XORShiftRandom

import java.io._
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

// TODO: at some point, we might have to split up functionalities into more files
// TODO: memory management
// TODO: create sorts by Iterators?, Create sample by iterators?
// TODO: difference between 'take' and 'merge'?

class ArrowColumnarBatchRow(@transient protected[column] val columns: Array[ArrowColumnVector], val numRows: Int) extends InternalRow with AutoCloseable with Serializable {
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
    new ArrowColumnarBatchRow(columns map { v =>
      val vector = v.getValueVector
      val allocator = vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::copy", 0, Integer.MAX_VALUE)
      val tp = vector.getTransferPair(allocator)

      tp.splitAndTransfer(0, numRows)
      new ArrowColumnVector(tp.getTo)
    }, numRows)
  }

  /**
   * Performs a projection on the batch given some expressions
   * @param indices the sequence of indices which define the projection
   * @return a fresh batch projected from the current batch
   *
   * TODO: Note: Caller should close returned batch
   */
  def projection(indices: Seq[Int]): ArrowColumnarBatchRow = {
    new ArrowColumnarBatchRow( indices.toArray map ( index => {
      val vector = columns(index).getValueVector
      val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::projection", 0, Integer.MAX_VALUE))
      tp.splitAndTransfer(0, numRows)
      new ArrowColumnVector(tp.getTo)
    }), numRows)
  }

  /**
   * Takes a range of rows from the batch
   * @param range the range to take
   * @return the split batch
   * NOTE: assumes 0 <= range < numRows
   *
   * TODO: Caller is responsible for closing returned batch
   */
  def take(range: Range): ArrowColumnarBatchRow = {
    new ArrowColumnarBatchRow( columns map ( column => {
      val vector = column.getValueVector
      val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::take(Range)", 0, Integer.MAX_VALUE))
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
   *
   * TODO: Caller is responsible for both closing this batch and the new
   */
  def appendColumns(cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow =
    new ArrowColumnarBatchRow(columns ++ cols, numRows)

  /**
   * Splits the current batch on its columns into two batches
   * @param col column index to split on
   * @return a pair of the two batches containing the split columns from this batch
   *
   * TODO: Caller is responsible for closing all batches (also this one)
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
        try {
          val first_length: Int = first.numRows.min(left.getOrElse(Int.MaxValue))
          val columns = first.columns.slice(0, numCols.getOrElse(first.numFields))
          // TODO: use ArrowCOlumnarBatchRowConverter
          val root = VectorSchemaRoot.of(columns.map(column => {
            if (left.isEmpty) column.getValueVector.asInstanceOf[FieldVector]
            val vector = column.getValueVector
            val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::encode", 0, Integer.MAX_VALUE))
            tp.splitAndTransfer(0, first_length.toInt)
            tp.getTo.asInstanceOf[FieldVector]
          }).toSeq: _*)
          try {
            val writer = new ArrowStreamWriter(root, null, Channels.newChannel(oos))
            try {
              // write first batch
              writer.start()
              writer.writeBatch()
              root.close()
              oos.writeInt(first_length)
              oos.writeInt(extra.length)
              oos.write(extra)
              left = left.map( numLeft => (numLeft-first_length).toInt )

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
                  left = left.map( numLeft => (numLeft-batchLength).toInt )
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
          first.close()
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
   *
   * Closes the passed batch
   * TODO: Caller is responsible for closing returned batch
   */
  def multiColumnSort(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    try {
      // UnionVector representing our batch
      val firstAllocator = batch.getFirstAllocator
        .getOrElse( throw new RuntimeException("[ArrowColumnarBatchRow::multiColumnSort] cannot get allocator ") )
      val unionAllocator = firstAllocator
        .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::union", 0, Integer.MAX_VALUE)
      val union = new UnionVector("Combiner", unionAllocator, FieldType.nullable(Struct.INSTANCE), null)

      // Indices for permutations
      val indexAllocator = firstAllocator
        .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::indices", 0, Integer.MAX_VALUE)
      val indices = new IntVector("indexHolder", indexAllocator)

      try {
        // prepare comparator and UnionVector
        val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
        sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
          val name = sortOrder.child.asInstanceOf[AttributeReference].name
          batch.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
            val valueVector = vector.getValueVector
            val tp = valueVector.getTransferPair(unionAllocator
              .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::union::transfer", 0, Integer.MAX_VALUE))
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

        // prepare indices
        indices.allocateNew(batch.numRows.toInt)
        indices.setValueCount(batch.numRows.toInt)

        // compute the index-vector
        (new IndexSorter).sort(union, indices, comparator)

        // sort all columns by permutation according to indices
        new ArrowColumnarBatchRow( batch.columns map { column =>
          val vector = column.getValueVector
          assert(vector.getValueCount == indices.getValueCount)

          // transfer type
          val tp = vector.getTransferPair(vector.getAllocator
            .newChildAllocator("ArrowColumnarBatchRow::multiColumnSort::permutation", 0, Integer.MAX_VALUE))
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
      } finally {
        union.close()
        indices.close()
      }
    } finally {
      batch.close()
    }
  }

  /**
   * @param batch an ArrowColumnarBatchRow to be sorted
   * @param col the column to sort on
   * @param sortOrder order settings to pass to the comparator
   * @return a fresh ArrowColumnarBatchRows with the sorted columns from batch
   *         Note: if col is out of range, returns the batch
   *
   * Closes the passed batch
   * TODO: Caller is responsible for closing returned batch
   */
  def sort(batch: ArrowColumnarBatchRow, col: Int, sortOrder: SortOrder): ArrowColumnarBatchRow = {
    if (col < 0 || col > batch.numFields)
      return batch

    try {
      val vector = batch.columns(col).getValueVector
      val indices =
        new IntVector("indexHolder", vector.getAllocator
          .newChildAllocator("ArrowColumnarBatchRow::sort::indices", 0, Integer.MAX_VALUE))
      assert(vector.getValueCount > 0)

      try {
        indices.allocateNew(vector.getValueCount)
        indices.setValueCount(vector.getValueCount)
        val comparator = new SparkComparator(sortOrder, DefaultVectorComparators.createDefaultComparator(vector))
        (new IndexSorter).sort(vector, indices, comparator)

        // sort by permutation
        new ArrowColumnarBatchRow( batch.columns map { column =>
          val vector = column.getValueVector
          assert(vector.getValueCount == indices.getValueCount)

          // transfer type
          val tp = vector.getTransferPair(vector.getAllocator
            .newChildAllocator("ArrowColumnarBatchRow::sort::permutations", 0, Integer.MAX_VALUE))
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
      } finally {
        indices.close()
      }
    } finally {
      batch.close()
    }
  }


  /**
   * @param batch batch to gather unique values from
   * @param sortOrders order to define unique-ness
   * @return a fresh batch with the unique values from the previous
   *
   * Closes the passed batch
   * TODO: Caller is responsible for closing the new batch
   */
  def unique(batch: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): ArrowColumnarBatchRow = {
    if (batch.numFields < 1)
      return batch

    try {
      // UnionVector representing our batch
      val allocator = batch.getFirstAllocator
        .getOrElse( throw new RuntimeException("[ArrowColumnarBatchRow::unique] cannot get allocator ") )
        .newChildAllocator("ArrowColumnarBatchRow::unique::union", 0, Integer.MAX_VALUE)
      val union = new UnionVector("Combiner", allocator, FieldType.nullable(Struct.INSTANCE), null)

      try {
        // prepare comparator and UnionVector
        val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
        sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
          val name = sortOrder.child.asInstanceOf[AttributeReference].name
          batch.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
            val valueVector = vector.getValueVector
            val tp = valueVector.getTransferPair(allocator)
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
        val indices = VectorDeduplicator.uniqueIndices(comparator, union)
        try {
          // grab the unique values
          new ArrowColumnarBatchRow( batch.columns map { column =>
            val vector = column.getValueVector
            assert(indices.getValueCount > 0)

            // transfer type
            val tp = vector.getTransferPair(vector.getAllocator
              .newChildAllocator("ArrowColumnarBatchRow::unique::make_unique", 0, Integer.MAX_VALUE))
            tp.splitAndTransfer(0, indices.getValueCount)
            val new_vector = tp.getTo

            new_vector.setInitialCapacity(indices.getValueCount)
            new_vector.allocateNew()
            0 until indices.getValueCount foreach { index => new_vector.copyFromSafe(indices.get(index), index, vector) }
            new_vector.setValueCount(indices.getValueCount)

            new ArrowColumnVector(new_vector)
          }, indices.getValueCount)
        } finally {
          indices.close()
        }
      } finally {
        union.close()
      }
    } finally {
      batch.close()
    }
  }

  /**
   * Sample rows from batches where the sample-size is determined by probability
   * @param input the batches to sample from
   * @param fraction the probability of a sample being taken
   * @param seed a seed for the "random"-generator
   * @return a fresh batch with the sampled rows
   *
   * TODO: Note: closes the batches
   * TODO: Caller is responsible for closing returned batch
   */
  def sample(input: Iterator[ArrowColumnarBatchRow], fraction: Double, seed: Long): ArrowColumnarBatchRow = {
    if (!input.hasNext) new ArrowColumnarBatchRow(Array.empty, 0)

    // The first batch should be separate, so we can determine the vector-types
    val first = input.next()

    val array = Array.tabulate[ArrowColumnVector](first.numFields) { i =>
      val vector = first.columns(i).getValueVector
      val tp = vector.getTransferPair(vector.getAllocator
        .newChildAllocator("ArrowColumnarBatchRow::sample", 0, Integer.MAX_VALUE))
      // we 'copy' the content of the first batch ...
      tp.splitAndTransfer(0, first.numRows.toInt)
      // ... and re-use the ValueVector so we do not have to determine vector types :)
      val new_vec = tp.getTo
      new_vec.clear()
      new_vec.allocateNew()
      new ArrowColumnVector(new_vec)
    }

    val iter = Iterator(first) ++ input
    val rand = new XORShiftRandom(seed)
    var i = 0
    while (iter.hasNext) {
      val batch = iter.next()
      batch.columns.foreach( col => assert(col.getValueVector.getValueCount == batch.numRows.toInt) )

      0 until batch.numRows.toInt foreach { index =>
        // do sample
        if (rand.nextDouble() <= fraction) {
          array zip batch.columns foreach { case (ours, theirs) => ours.getValueVector.copyFromSafe(index, i, theirs.getValueVector)}
          i += 1
        }
      }
      batch.close()
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
   *
   * TODO: Caller is responsible for closing the returned batch
   */
  def sampleAndCount(input: Iterator[ArrowColumnarBatchRow], k: Int, seed: Long = Random.nextLong()):
      (ArrowColumnarBatchRow, Long) = {
    try {
      if (k < 1) (Array.empty[ArrowColumnarBatchRow], 0)

      // First, we fill the reservoir with k elements
      var inputSize = 0L
      var nrBatches = 0
      var remainderBatch: Option[ArrowColumnarBatchRow] = None
      val reservoirBuf = new ArrayBuffer[ArrowColumnarBatchRow](k)
      try {
        while (inputSize < k) {
          var length = 0L
          if (!input.hasNext) return (ArrowColumnarBatchRow.create(reservoirBuf.slice(0, nrBatches).toIterator), inputSize)
          val batch: ArrowColumnarBatchRow = input.next()
          try {
            assert(batch.numRows < Integer.MAX_VALUE)
            length = math.min(k-inputSize, batch.numRows)
            reservoirBuf += batch.take(0 until length.toInt)

            // do we have elements remaining?
            if (length < batch.numRows)
              remainderBatch = Option(batch.take(length.toInt until batch.numRows.toInt))
          } finally {
            batch.close()
          }
          nrBatches += 1
          inputSize += length
        }
      } catch {
        // if we somehow triggered an exception, close everything and throw the exception again
        case e: Throwable =>
          remainderBatch.foreach( _.close() )
          reservoirBuf.foreach( _.close() )
          throw e
      }

      // closes reservoirBuf
      val reservoir = ArrowColumnarBatchRow.create(reservoirBuf.toIterator)
      // add our remainder to the iterator, if there is any
      val iter: Iterator[ArrowColumnarBatchRow] = remainderBatch.fold(input)( Iterator(_) ++ input )
      try {
        // make sure we do not use this batch anymore
        remainderBatch = None

        // we now have a reservoir with length k, in which we will replace random elements
        val rand = new XORShiftRandom(seed)
        def generateRandomNumber(start: Int = 0, end: Int = Integer.MAX_VALUE-1) : Int = {
          start + rand.nextInt( (end-start) + 1)
        }

        while (iter.hasNext) {
          val batch: ArrowColumnarBatchRow = iter.next()
          try {
            val start: Int = generateRandomNumber(end = (batch.numRows-1).toInt)
            val end = generateRandomNumber(start, batch.numRows.toInt)
            val sample: ArrowColumnarBatchRow = batch.take(start until end)
            try {
              0 until sample.numRows.toInt foreach { index =>
                reservoir.copyAtIndex(batch, generateRandomNumber(end = k-1), index)
              }
              inputSize += sample.numRows
            } finally {
              sample.close()
            }
          } finally {
            batch.close()
          }
        }

        // we have to copy since we want to guarantee to always close reservoir
        (reservoir.copy(), inputSize)
      } finally {
        // if we returned earlier than expected, close the batches in the Iterator
        iter.foreach( _.close() )
        reservoir.close()
      }
    } finally {
      // if we somehow returned earlier then expected, close all other batches as well!
      input.foreach( _.close() )
    }
  }

  /**
   * @param key ArrowColumnarBatchRow to define distribution for
   * @param rangeBounds ArrowColumnarBatchRow containing ranges on which distribution is based
   * @param sortOrders SortOrders on which distribution is based
   * @return Indices containing the distribution for the key given the rangeBounds and sortOrders
   *
   * TODO: Note: closes both the key and rangeBounds
   */
  def bucketDistributor(key: ArrowColumnarBatchRow, rangeBounds: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): Array[Int] = {
    if (key.numFields < 1 || rangeBounds.numFields < 1)
      return Array.empty
    // prepare comparator and UnionVectors
    val keyAllocator = key.getFirstAllocator
      .getOrElse( throw new RuntimeException("[ArrowColumnarBatchRow::bucketDistributor] cannot get keyAllocator ") )
      .newChildAllocator("ArrowColumnarBatchRow::bucketDistributor::union", 0, Integer.MAX_VALUE)
    val rangeAllocator = rangeBounds.getFirstAllocator
      .getOrElse( throw new RuntimeException("[ArrowColumnarBatchRow::bucketDistributor] cannot get rangeAllocator ") )
      .newChildAllocator("ArrowColumnarBatchRow::bucketDistributor::range", 0, Integer.MAX_VALUE)
    val keyUnion = new UnionVector("KeyCombiner", keyAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val rangeUnion = new UnionVector("rangeCombiner", rangeAllocator, FieldType.nullable(Struct.INSTANCE), null)
    val comparators = new Array[(String, SparkComparator[ValueVector])](sortOrders.length)
    sortOrders.zipWithIndex foreach { case (sortOrder, index) =>
      val name = sortOrder.child.asInstanceOf[AttributeReference].name
      // prepare keyUnion and comparator
      key.columns.find( vector => vector.getValueVector.getName.equals(name)).foreach( vector => {
        val valueVector = vector.getValueVector
        val tp = valueVector.getTransferPair(keyAllocator)
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
        val tp = valueVector.getTransferPair(rangeAllocator)
        tp.splitAndTransfer(0, rangeBounds.numRows.toInt)
        rangeUnion.addVector(tp.getTo.asInstanceOf[FieldVector])
      })
    }
    val comparator = new SparkUnionComparator(comparators)
    keyUnion.setValueCount(key.numRows.toInt)
    rangeUnion.setValueCount(rangeBounds.numRows.toInt)

    // find partition-ids
    val partitionIds = new BucketSearcher(keyUnion, rangeUnion, comparator).distribute()
    keyUnion.close()
    rangeUnion.close()
    partitionIds
  }

  /**
   * @param key ArrowColumnarBatchRow to distribute
   * @param partitionIds Array containing which row corresponds to which partition
   * @return A map from partitionId to its corresponding ArrowColumnarBatchRow
   *
   * TODO: closes the key
   * TODO: Caller should close the batches in the returned map
   */
  def distribute(key: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, ArrowColumnarBatchRow] = {
    val distributed = mutable.Map[Int, ArrowColumnarBatchRowBuilder]()

    partitionIds.zipWithIndex foreach { case (partitionId, index) =>
      val newRow = key.take(index until index+1)
      if (distributed.contains(partitionId))
        distributed(partitionId).append(newRow)
      else
        distributed(partitionId) = new ArrowColumnarBatchRowBuilder(newRow)
    }

    distributed.map ( items => (items._1, items._2.build()) ).toMap
  }
}