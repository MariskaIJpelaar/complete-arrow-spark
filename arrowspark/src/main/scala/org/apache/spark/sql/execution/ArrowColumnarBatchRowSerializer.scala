package org.apache.spark.sql.execution

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.NextIterator

import java.io.{ByteArrayInputStream, InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/** Note: copied and adapted from org.apache.spark.sql.execution.UnsafeRowSerializer */
class ArrowColumnarBatchRowSerializer(dataSize: Option[SQLMetric] = None) extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = new ArrowColumnarBatchRowSerializerInstance(dataSize)
  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class ArrowColumnarBatchRowSerializerInstance(dataSize: Option[SQLMetric]) extends SerializerInstance {
  private val intermediate = 42

  override def serializeStream(s: OutputStream): SerializationStream = new SerializationStream {
    private var root: Option[VectorSchemaRoot] = None
    private var oos: Option[ObjectOutputStream] = None
    private var writer: Option[ArrowStreamWriter] = None

    private def getRoot(batch: ArrowColumnarBatchRow): VectorSchemaRoot = {
      if (root.isEmpty) root = Option(batch.toRoot)
      root.get
    }

    private def getOos: ObjectOutputStream = {
      if (oos.isDefined) return oos.get

      s.write(intermediate)
      s.flush()
      val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      oos = Option(new ObjectOutputStream(codec.compressedOutputStream(s)))
      oos.get
    }

    private def getWriter(batch: ArrowColumnarBatchRow): ArrowStreamWriter = {
      if (writer.isEmpty) {
        writer = Option(new ArrowStreamWriter(getRoot(batch), null, Channels.newChannel(getOos)))
        new VectorLoader(root.get).load(batch.toArrowRecordBatch(root.get.getFieldVectors.size()))
        writer.get.start()
        return writer.get
      }

      new VectorLoader(root.get).load(batch.toArrowRecordBatch(root.get.getFieldVectors.size()))
      writer.get
    }

    override def writeValue[T](value: T)(implicit evidence$6: ClassTag[T]): SerializationStream = {
      val batch = value.asInstanceOf[ArrowColumnarBatchRow]
      dataSize.foreach( metric => metric.add(batch.getSizeInBytes))

      getWriter(batch).writeBatch()
      getOos.writeLong(batch.numRows)

      this
    }
    override def writeKey[T](key: T)(implicit evidence$5: ClassTag[T]): SerializationStream = this
    override def flush(): Unit = s.flush()
    override def close(): Unit = {
      writer.foreach( writer => writer.close() )
      oos.foreach( oos => oos.close() )
    }

    /** The following methods are never called by shuffle-code (according to UnsafeRowSerializer) */
    override def writeObject[T](t: T)(implicit evidence$4: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
    override def writeAll[T](iter: Iterator[T])(implicit evidence$7: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
  }

  /** TODO: close readers and such? */
  override def deserializeStream(s: InputStream): DeserializationStream = new DeserializationStream {
    /** Currently, we read in everything.
     * FIXME: read in batches :) */
    private val all = new ArrayBuffer[Byte]()
    private val batchSizes = 65536 // 65k
    private val batch = new Array[Byte](batchSizes)
    private var readin = s.read(batch)
    while (readin != -1) {
      all ++= batch.slice(0, readin)
      readin = s.read(batch)
    }


    override def asKeyValueIterator: Iterator[(Int, ArrowColumnarBatchRow)] = new NextIterator[(Int, ArrowColumnarBatchRow)] {
      private val bis = new ByteArrayInputStream(all.toArray)
      private val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
      private var ois: Option[ObjectInputStream] = None
      private def initOis(): Unit = {
        val check = new Array[Byte](1)
        if (bis.read(check) == -1) {
          ois = None
          return
        }
        while (check(0) != intermediate) {
          if (bis.read(check) == -1) {
            ois = None
            return
          }
        }
        ois = Option(new ObjectInputStream(codec.compressedInputStream(bis)))
      }
      private lazy val allocator = new RootAllocator()
      private var reader: Option[ArrowStreamReader] = None
      private def initReader(): Unit = {
        initOis()
        ois.fold( (reader = None) )( stream => reader = Option(new ArrowStreamReader(stream, allocator)))
      }

      initReader()

      override protected def getNext(): (Int, ArrowColumnarBatchRow) = {
        if (reader.isEmpty) {
          finished = true
          return null
        }

        if (!reader.get.loadNextBatch()) {
          initReader()
          if (reader.isEmpty) {
            finished = true
            return null
          }
        }

        val columns = reader.get.getVectorSchemaRoot.getFieldVectors
        val length = ois.get.readLong()
        (0, new ArrowColumnarBatchRow((columns map { vector =>
          val allocator = vector.getAllocator
          val tp = vector.getTransferPair(allocator)

          tp.transfer()
          new ArrowColumnVector(tp.getTo)
        }).toArray, length))
      }


      override protected def close(): Unit = {}

    }

    /** returning a dummy */
    override def readKey[T]()(implicit evidence$9: ClassTag[T]): T = null.asInstanceOf[T]

    /** TODO: we first want to know when this is called... */
    override def readValue[T]()(implicit evidence$10: ClassTag[T]): T = throw new UnsupportedOperationException()

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

