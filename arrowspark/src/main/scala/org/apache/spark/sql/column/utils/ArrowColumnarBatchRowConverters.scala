package org.apache.spark.sql.column.utils

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.{FieldVector, TypeLayout, VectorSchemaRoot}
import org.apache.arrow.vector.compression.{CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.spark.sql.column.ArrowColumnarBatchRow

import java.util
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

object ArrowColumnarBatchRowConverters {
  /** copied from org.apache.arrow.vector.VectorUnloader::appendNodes(...) */
  private def appendNodes(codec: NoCompressionCodec, rowCount: Long, vector: FieldVector,
                          nodes: util.List[ArrowFieldNode], buffers: util.List[ArrowBuf]): Unit = {
    nodes.add(new ArrowFieldNode(rowCount, vector.getNullCount))
    val fieldBuffers = vector.getFieldBuffers
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField.getType)
    if (fieldBuffers.size != expectedBufferCount)
      throw new IllegalArgumentException(String.format("wrong number of buffers for field %s in vector %s. found: %s", vector.getField, vector.getClass.getSimpleName, fieldBuffers))
    fieldBuffers.forEach( buf =>
      buffers.add(codec.compress(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::appendNodes", 0, Integer.MAX_VALUE), buf))
    )
    vector.getChildrenFromFields.forEach( child =>
      appendNodes(codec, rowCount, child, nodes, buffers)
    )
  }

  /**
   * copied from org.apache.arrow.vector.VectorUnloader
   * TODO: Caller should close the ArrowRecordBatch
   * @param batch batch to convert and close
   * @param numCols (optional) number of columns to convert
   * @param numRows (optional) number of rows to convert
   * @return The generated ArrowRecordBatch together with the number of rows converted
   */
  def toArrowRecordBatch(batch: ArrowColumnarBatchRow, numCols: Int, numRows: Option[Int] = None): (ArrowRecordBatch, Int) = {
    try {
      val nodes = new util.ArrayList[ArrowFieldNode]
      val buffers = new util.ArrayList[ArrowBuf]
      val codec = NoCompressionCodec.INSTANCE

      val rowCount: Int = numRows.getOrElse{
        if (batch.numRows > Integer.MAX_VALUE)
          throw new RuntimeException("[ArrowColumnarBatchRowConverter::toArrowRecordBatch] too many rows")
        batch.numRows.toInt
      }

      batch.columns.slice(0, numCols) foreach( column =>
        appendNodes(codec, rowCount, column.getValueVector.asInstanceOf[FieldVector], nodes, buffers) )
      (new ArrowRecordBatch(rowCount, nodes, buffers, CompressionUtil.createBodyCompression(codec), true), rowCount)
    } finally {
      batch.close()
    }
  }

  /** Creates a VectorSchemaRoot from the provided batch and closes it
   * TODO: Caller should close the root */
  def toRoot(batch: ArrowColumnarBatchRow, numCols: Option[Int] = None, numRows: Option[Int] = None): VectorSchemaRoot = {
    try {
      // TODO: s.thing with numRows
      val columns = batch.columns.slice(0, numCols.getOrElse(batch.numFields))
      VectorSchemaRoot.of(columns.map(column => {
        column.getValueVector.asInstanceOf[FieldVector]
      }).toSeq: _*)
    } finally {
      batch.close()
    }
  }

}
