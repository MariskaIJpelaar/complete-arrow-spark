package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.BitVectorHelper
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.io.Closeable
import scala.collection.immutable.NumericRange


/** Note: closes first
 * Caller should close after use */
class ArrowColumnarBatchRowBuilder(first: ArrowColumnarBatchRow, val numCols: Option[Int] = None, val numRows: Option[Int] = None) extends Closeable {
  protected[column] var size = 0
  protected[column] val columns: Array[ArrowColumnVector] = {
    Resources.autoCloseTryGet(first) { first =>
      size = first.numRows.min(numRows.getOrElse(Integer.MAX_VALUE))

      ArrowColumnarBatchRowTransformers.take(
        ArrowColumnarBatchRowTransformers.projection(first, 0 until numCols.getOrElse(first.numFields)),
        0 until numRows.getOrElse(first.numRows)).columns
    }
  }

  protected val numBytes: Array[Long] = Array.tabulate(columns.length) { index =>
    columns(index).getValueVector.getDataBuffer.readableBytes()
  }

  def length: Int = size
  def numFields: Int = columns.length

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

  /** Note: closes batch */
  def append(batch: ArrowColumnarBatchRow): ArrowColumnarBatchRowBuilder = {
    Resources.autoCloseTryGet(batch) { batch =>
      var current_size = 0
      // the columns we want
      val array = numCols.fold(batch.columns)( nums => batch.columns.slice(0, nums) )
      // the rows that are left to read
      current_size = batch.numRows.min( numRows.getOrElse(Integer.MAX_VALUE) - size )
      if (size + current_size > Integer.MAX_VALUE)
        throw new RuntimeException("[ArrowColumnarBatchRowBuilder batches are too big to be combined!")

      (array, columns, numBytes.indices).zipped foreach { case (input, output, byteIndex) =>
        val iVector = input.getValueVector
        val oVector = output.getValueVector
        // make sure we have enough space
        while (oVector.getValueCapacity < size + current_size) oVector.reAlloc()
        // copy contents
        validityRangeSetter(oVector.getValidityBuffer, size.toLong until (size+current_size).toLong)
        output.getValueVector.getDataBuffer.setBytes(numBytes(byteIndex), iVector.getDataBuffer)
        numBytes(byteIndex) += iVector.getDataBuffer.readableBytes()

      }
      size += current_size
      this
    }
  }

  /** Note: invalidates the Builder
   * Caller is responsible for closing the vectors */
  def buildColumns(parentAllocator: BufferAllocator): Array[ArrowColumnVector] = {
    // transfer ownership to new Array
    columns.map( column => {
      val vector = column.getValueVector
      vector.setValueCount(size)
      val tp = vector.getTransferPair(
        createAllocator(parentAllocator, vector.getName))
      tp.splitAndTransfer(0, vector.getValueCount)
      new ArrowColumnVector(tp.getTo)
    })
  }

  /** Note: invalidates the Builder
   * Caller is responsible for closing the vector */
  def build(batchAllocator: BufferAllocator): ArrowColumnarBatchRow = {
    // transfer ownership to new ArrowColumnarBatchRow
    val transferred = buildColumns(batchAllocator)
    new ArrowColumnarBatchRow(batchAllocator, transferred, size)
  }

  override def close(): Unit = columns.foreach( _.close() )
}
