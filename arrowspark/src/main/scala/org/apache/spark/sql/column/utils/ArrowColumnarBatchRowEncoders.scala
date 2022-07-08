package org.apache.spark.sql.column.utils

import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.column
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.NextIterator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ArrowColumnarBatchRowEncoders {
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
          } finally {
            writer.close()
          }
        } finally {
          root.close()
        }
      } finally {
        oos.close()
      }
      Iterator(bos.toByteArray)
    } finally {
      iter.foreach( extraEncoder(_)._2.close() )
    }
  }

  /** Note: similar to decodeUnsafeRows
   *
   * Callers may add additional decoding by providing the 'extraDecoder' function. They are responsible for
   * closing the provided ArrowColumnarBatch if they consume it (do not return it)
   *
   * Callers are responsible for closing the returned 'Any' containing the batch */
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
