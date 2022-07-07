package org.apache.spark.sql.column.utils

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.compression.{CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{FieldVector, TypeLayout, VectorSchemaRoot}
import org.apache.spark.sql.column.ArrowColumnarBatchRow

import java.util

/** Methods that convert an ArrowColumnarBatchRows to another type, and taking care of closing of the input */
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

      val rowCount: Int = numRows.getOrElse(batch.numRows)
      batch.columns.slice(0, numCols) foreach( column =>
        appendNodes(codec, rowCount, column.getValueVector.asInstanceOf[FieldVector], nodes, buffers) )
      (new ArrowRecordBatch(rowCount, nodes, buffers, CompressionUtil.createBodyCompression(codec), true), rowCount)
    } finally {
      batch.close()
    }
  }

  /** Creates a VectorSchemaRoot from the provided batch and closes it
   * Returns the root and the number of rows transferred
   * TODO: Caller should close the root */
  def toRoot(batch: ArrowColumnarBatchRow, numCols: Option[Int] = None, numRows: Option[Int] = None): (VectorSchemaRoot, Int) = {
    try {
      val rowCount = numRows.getOrElse(batch.numRows)
      val columns = batch.columns.slice(0, numCols.getOrElse(batch.numFields))
      (VectorSchemaRoot.of(columns.map(column => {
        val vector = column.getValueVector
        val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRowConverters::toRoot", 0, Integer.MAX_VALUE))
        tp.splitAndTransfer(0, rowCount)
        tp.getTo.asInstanceOf[FieldVector]
      }).toSeq: _*), rowCount)
    } finally {
      batch.close()
    }
  }

  /**
   * Splits a single batch into two
   * @param batch ArrowColumnarBatchRow to split and close
   * @param rowIndex index to split on
   * @return two ArrowColumnarBatchRows split on rowIndex from batch
   *         If rowIndex > batch.numRows, returns (original-batch, empty-batch)
   *         TODO: Caller is responsible for closing the batches
   */
  def split(batch: ArrowColumnarBatchRow, rowIndex: Int): (ArrowColumnarBatchRow, ArrowColumnarBatchRow) = {
    try {
      val splitPoint = rowIndex.min(batch.numRows)
      (batch.copy(0 until splitPoint), batch.copy(splitPoint until batch.numRows))
    } finally {
      batch.close()
    }
  }

}
