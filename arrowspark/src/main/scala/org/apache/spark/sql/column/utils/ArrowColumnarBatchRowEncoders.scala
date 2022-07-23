package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorLoader
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.NextIterator

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.channels.Channels
import java.util.concurrent.atomic.AtomicLong
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

object ArrowColumnarBatchRowEncoders {
  var totalTimeEncode: AtomicLong = new AtomicLong(0)

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
   * Closes the batches found in the iterator
   */
  def encode(iter: Iterator[Any],
             numCols: Option[Int] = None,
             numRows: Option[Int] = None,
             extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) =
             batch => (Array.emptyByteArray, batch.asInstanceOf[ArrowColumnarBatchRow])): Iterator[Array[Byte]] = {
    if (!iter.hasNext)
      return Iterator(Array.emptyByteArray)

    // FIXME: make iter an Iterator of Closeables?
    val t1 = System.nanoTime()
    try {
      // how many rows are left to read?
      var left = numRows
      // Setup the streams and writers
      val bos = new ByteArrayOutputStream()
      val oos = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectOutputStream(codec.compressedOutputStream(bos))
      }

      Resources.autoCloseTryGet(oos) { oos =>
        // Prepare first batch
        // This needs to be done separately as we need the schema for the VectorSchemaRoot
        val (extra, first): (Array[Byte], ArrowColumnarBatchRow) = extraEncoder(iter.next())
        // consumes first
        val (root, allocator, firstLength) = ArrowColumnarBatchRowConverters.toRoot(first, numCols, numRows)
        Resources.autoCloseTryGet(allocator) ( _ =>
          Resources.autoCloseTryGet(root) ( root => Resources.autoCloseTryGet(new ArrowStreamWriter(root, null, Channels.newChannel(oos))) { writer =>
          // write first batch
          writer.start()
          writer.writeBatch()
          root.close()
          oos.writeInt(firstLength)
          oos.writeInt(extra.length)
          oos.write(extra)
          left = left.map( numLeft => numLeft-firstLength )

          // while we still have some reading to do
          while (iter.hasNext && left.forall( _ > 0)) {
            val (extra, batch): (Array[Byte], ArrowColumnarBatchRow) = extraEncoder(iter.next())
            Resources.autoCloseTryGet(batch){ batch =>
              // consumes batch
              val (recordBatch, batchLength): (ArrowRecordBatch, Int) =
                ArrowColumnarBatchRowConverters.toArrowRecordBatch(batch, root.getFieldVectors.size(), numRows = left)
              Resources.autoCloseTryGet(recordBatch) { recordBatch =>
                new VectorLoader(root).load(recordBatch)
                writer.writeBatch()
                root.close()
                oos.writeInt(batchLength)
                oos.writeInt(extra.length)
                oos.write(extra)
                left = left.map( numLeft => numLeft-batchLength )
              }
            }
          }
          oos.flush()
        }))
      }
      Iterator(bos.toByteArray)
    } finally {
      iter.foreach( extraEncoder(_)._2.close() )
      val t2 = System.nanoTime()
      totalTimeEncode.addAndGet(t2 - t1)
    }
  }

  var totalTimeDecode: AtomicLong = new AtomicLong(0)

  /** Note: similar to decodeUnsafeRows
   *
   * Callers may add additional decoding by providing the 'extraDecoder' function. They are responsible for
   * closing the provided ArrowColumnarBatch if they consume it (do not return it)
   *
   * Callers are responsible for closing the returned 'Any' containing the batch
   */
  def decode(rootAllocator: RootAllocator, bytes: Array[Byte],
             extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (_, batch) => batch): Iterator[Any] = {
    if (bytes.length == 0)
      return Iterator.empty
    new NextIterator[Any] {
      private val bis = new ByteArrayInputStream(bytes)
      private val ois = {
        val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
        new ObjectInputStream(codec.compressedInputStream(bis))
      }
      private val allocator = rootAllocator
      private val reader = new ArrowStreamReader(ois, allocator)

      override protected def getNext(): Any = {
        val t1 = System.nanoTime()
        if (!reader.loadNextBatch()) {
          finished = true
          return null
        }

        Resources.autoCloseTryGet(reader.getVectorSchemaRoot) { root =>
          Resources.autoCloseTraversableTryGet(root.getFieldVectors.toIterator) { columns =>
            val batchAllocator = createAllocator(allocator, "ArrowColumnarBatchRowEncoders::decode")
            val length = ois.readInt()
            val arr_length = ois.readInt()
            val array = new Array[Byte](arr_length)
            ois.readFully(array)

            val ret = extraDecoder(array, new ArrowColumnarBatchRow(batchAllocator, (columns map { vector =>
              val tp = vector.getTransferPair(createAllocator(batchAllocator, vector.getName))
              tp.splitAndTransfer(0, vector.getValueCount)
              new ArrowColumnVector(tp.getTo)
            }).toArray, length))
            val t2 = System.nanoTime()
            totalTimeDecode.addAndGet(t2 - t1)
            ret
          }
        }
      }

      override protected def close(): Unit = {
        reader.close()
        ois.close()
        bis.close()
      }
    }
  }
}
