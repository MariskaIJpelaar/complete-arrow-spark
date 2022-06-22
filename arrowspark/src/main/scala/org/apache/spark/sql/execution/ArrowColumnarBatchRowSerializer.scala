package org.apache.spark.sql.execution

import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.metric.SQLMetric

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

/** Note: copied and adapted from org.apache.spark.sql.execution.UnsafeRowSerializer */
class ArrowColumnarBatchRowSerializer {

}

private class ArrowColumnarBatchRowSerializerInstance(dataSize: Option[SQLMetric]) extends SerializerInstance {
  override def serializeStream(s: OutputStream): SerializationStream = new SerializationStream {
    override def writeValue[T](value: T)(implicit evidence$6: ClassTag[T]): SerializationStream = {
      val batch = value.asInstanceOf[ArrowColumnarBatchRow]
      dataSize.foreach( metric => metric.add(batch.getSizeInBytes))
      batch.writeToStream(s)
      this
    }
    override def writeKey[T](key: T)(implicit evidence$5: ClassTag[T]): SerializationStream = this
    override def flush(): Unit = s.flush()
    override def close(): Unit = {}

    /** The following methods are never called by shuffle-code (according to UnsafeRowSerializer) */
    override def writeObject[T](t: T)(implicit evidence$4: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
    override def writeAll[T](iter: Iterator[T])(implicit evidence$7: ClassTag[T]): SerializationStream =
      throw new UnsupportedOperationException()
  }

  override def deserializeStream(s: InputStream): DeserializationStream = new DeserializationStream {

    override def asKeyValueIterator: Iterator[(Int, ArrowColumnarBatchRow)] = ???

    /** returning a dummy */
    override def readKey[T]()(implicit evidence$9: ClassTag[T]): T = null.asInstanceOf[T]

    override def readValue[T]()(implicit evidence$10: ClassTag[T]): T = ArrowColumnarBatchRow.fromStream(s).asInstanceOf[T]
    override def close(): Unit = {}

    /** The following methods are never called by shuffle-code (according to UnsafeRowSerializer) */
    override def readObject[T]()(implicit evidence$8: ClassTag[T]): T = throw new UnsupportedOperationException
    override def asIterator: Iterator[Any] = throw new UnsupportedOperationException
  }

  /** The following methods are not called by Shuffle Code (according to UnsafeRowSerializer) */
  override def serialize[T](t: T)(implicit evidence$1: ClassTag[T]): ByteBuffer = throw new UnsupportedOperationException
  override def deserialize[T](bytes: ByteBuffer)(implicit evidence$2: ClassTag[T]): T = throw new UnsupportedOperationException()
  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader)(implicit evidence$3: ClassTag[T]): T = throw new UnsupportedOperationException()
}

