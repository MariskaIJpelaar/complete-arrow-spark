package org.apache.spark.sql.column.utils

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.column
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.NextIterator

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// TODO: check memory managment
/** Note: copied and adapted from org.apache.spark.sql.execution.UnsafeRowSerializer
 * Also note: getting this right took quite some effort, if you want to improve/ change it,
 * please know what you are doing :) */
class ArrowColumnarBatchRowSerializer(dataSize: Option[SQLMetric] = None) extends Serializer with Serializable {
  // Caller should close whatever is deserialized
  override def newInstance(): SerializerInstance = new ArrowColumnarBatchRowSerializerInstance(dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class ArrowColumnarBatchRowSerializerInstance(dataSize: Option[SQLMetric]) extends SerializerInstance {
  private val intermediate = 'B'

  override def serializeStream(s: OutputStream): SerializationStream = new SerializationStream {
    private var root: Option[VectorSchemaRoot] = None
    private var oos: Option[ObjectOutputStream] = None
    private var writer: Option[ArrowStreamWriter] = None

    /** Does not consume batch */
    private def getRoot(batch: ArrowColumnarBatchRow): VectorSchemaRoot = {
      if (root.isEmpty) root = Option(ArrowColumnarBatchRowConverters.toRoot(batch.copy(allocatorHint = "ArrowColumnarBatchRowSerializer::getRoot"))._1)
      root.get
    }

    private def getOos: ObjectOutputStream = {
      if (oos.isDefined) return oos.get

      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val cos = codec.compressedOutputStream(s)
      cos.write(intermediate.toByte)
      oos = Option(new ObjectOutputStream(cos))
      oos.get
    }

    /** Does not consume batch */
    private def getWriter(batch: ArrowColumnarBatchRow): ArrowStreamWriter = {
      if (writer.isEmpty) {
        writer = Option(new ArrowStreamWriter(getRoot(batch), null, Channels.newChannel(getOos)))
        val recordBatch: ArrowRecordBatch =
          ArrowColumnarBatchRowConverters.toArrowRecordBatch(batch.copy(allocatorHint = "ArrowColumnarBatchRowSerializer::getWriter"), batch.numFields)._1
        try {
          new VectorLoader(root.get).load(recordBatch)
          writer.get.start()
          return writer.get
        } finally {
          recordBatch.close()
        }
      }

      val recordBatch: ArrowRecordBatch =
        ArrowColumnarBatchRowConverters.toArrowRecordBatch(batch.copy(allocatorHint = "ArrowColumnarBatchRowSerializer::getWriter::recordBatch"), batch.numFields)._1
      try {
        new VectorLoader(root.get).load(recordBatch)
        writer.get
      } finally {
        recordBatch.close()
      }
    }

    override def writeValue[T](value: T)(implicit evidence$6: ClassTag[T]): SerializationStream = {
      val batch = value.asInstanceOf[ArrowColumnarBatchRow]
      try {
        dataSize.foreach( metric => metric.add(batch.getSizeInBytes))

        getWriter(batch).writeBatch()
        getOos.writeInt(batch.numRows)
      } finally {
        batch.close()
      }
      this
    }
    override def writeKey[T](key: T)(implicit evidence$5: ClassTag[T]): SerializationStream = this
    override def flush(): Unit = s.flush()
    override def close(): Unit = {
      writer.foreach( writer => writer.close() )
      oos.foreach( oos => oos.close() )
      root.foreach( vectorSchemaRoot => vectorSchemaRoot.close() )
    }

    /** The following methods are never called by shuffle-code (according to UnsafeRowSerializer) */
    override def writeObject[T](t: T)(implicit evidence$4: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
    override def writeAll[T](iter: Iterator[T])(implicit evidence$7: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
  }

  override def deserializeStream(s: InputStream): DeserializationStream = new DeserializationStream {
    /** Currently, we read in everything.
     * FIXME: read in batches :) */
    private val all = new ArrayBuffer[Byte]()
    private val batchSizes = 65536 // 64k
    private val batch = new Array[Byte](batchSizes)
    private var reader = s.read(batch)
    while (reader != -1) {
      all ++= batch.slice(0, reader)
      reader = s.read(batch)
    }


    /** Caller should close batches in iterator */
    override def asKeyValueIterator: Iterator[(Int, ArrowColumnarBatchRow)] = new NextIterator[(Int, ArrowColumnarBatchRow)] {
      private val bis = new ByteArrayInputStream(all.toArray)
      private val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      private var ois: Option[ObjectInputStream] = None
      private def initOis(): Unit = {
        val cis = codec.compressedInputStream(bis)
        val check = new Array[Byte](1)
        if (cis.read(check) == -1) {
          ois = None
          return
        }
        while (check(0).toChar != intermediate) {
          if (cis.read(check) == -1) {
            ois = None
            return
          }
        }
        ois = Option(new ObjectInputStream(cis))
      }
      private lazy val allocator = column.rootAllocator.newChildAllocator("ArrowColumnarBatchRowSerializer", 0, org.apache.spark.sql.column.perAllocatorSize)
      private var reader: Option[ArrowStreamReader] = None
      private def initReader(): Unit = {
        initOis()
        ois.fold( (reader = None) )( stream => reader = Option(new ArrowStreamReader(stream, allocator)))
      }

      initReader()

      // Caller should close
      override protected def getNext(): (Int, ArrowColumnarBatchRow) = {
        if (reader.isEmpty) {
          finished = true
          return null
        }

        if (!reader.get.loadNextBatch()) {
          reader.get.close(false)
          initReader()
          if (reader.isEmpty) {
            finished = true
            return null
          }
          if (!reader.get.loadNextBatch())
            throw new RuntimeException("[ArrowColumnarBatchRowSerializer] Corrupted Stream")
        }

        val columns = reader.get.getVectorSchemaRoot.getFieldVectors
        val length = ois.get.readInt()
        (0, new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator
            .newChildAllocator("ArrowColumnarBatchRowSerializer::getNext", 0, org.apache.spark.sql.column.perAllocatorSize)
          val tp = vector.getTransferPair(allocator)

          tp.transfer()
          new ArrowColumnVector(tp.getTo)
        }).toArray, length))
      }


      override protected def close(): Unit = { ois.foreach (_.close()) }

    }

    /** returning a dummy */
    override def readKey[T]()(implicit evidence$9: ClassTag[T]): T = null.asInstanceOf[T]
    override def readValue[T]()(implicit evidence$10: ClassTag[T]): T = null.asInstanceOf[T]

    /** The following methods are never called by shuffle-code (according to UnsafeRowSerializer) */
    override def readObject[T]()(implicit evidence$8: ClassTag[T]): T = throw new UnsupportedOperationException
    override def asIterator: Iterator[Any] = throw new UnsupportedOperationException

    override def close(): Unit = s.close()
  }


  /** The following methods are not called by Shuffle Code (according to UnsafeRowSerializer) */
  override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]): T = throw new UnsupportedOperationException()
  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit evidence$3: ClassTag[T]): T = throw new UnsupportedOperationException()
}
