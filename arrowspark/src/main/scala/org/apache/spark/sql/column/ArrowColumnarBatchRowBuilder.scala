package org.apache.spark.sql.column

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.BitVectorHelper
import org.apache.spark.sql.vectorized.ArrowColumnVector

import scala.collection.immutable.NumericRange

/** Note: closes first  */
class ArrowColumnarBatchRowBuilder(first: ArrowColumnarBatchRow, val numCols: Option[Int] = None, val numRows: Option[Int] = None) {
  protected var num_bytes = 0L
  protected[column] var size = 0
  protected[column] val columns: Array[ArrowColumnVector] = {
    if (numRows.isEmpty && first.numRows > Integer.MAX_VALUE)
      throw new RuntimeException("ArrowColumnarBatchRowBuilder First batch is too large")

    size = first.numRows.toInt.min(numRows.getOrElse(Integer.MAX_VALUE))

    val cols = first.columns.slice(0, numCols.getOrElse(first.columns.length)).map(column => {
      val vector = column.getValueVector
      val tp = vector.getTransferPair(vector.getAllocator
        .newChildAllocator("ArrowColumnarBatchRowBuilder::first", 0, Integer.MAX_VALUE))
      numRows.fold( tp.splitAndTransfer(0, first.numRows.toInt) )( num =>
        tp.splitAndTransfer(0, num)
      )
      // we 'copy' the content of the first batch ...
      val newVec = tp.getTo

      // ... and re-use the ValueVector so we do not have to determine vector types :)
      newVec.clear()
      numRows.foreach( nums => newVec.setInitialCapacity(nums))
      newVec.allocateNew()
      num_bytes = vector.getDataBuffer.readableBytes()
      // make sure we have enough space
      while (newVec.getValueCapacity < size) newVec.reAlloc()
      // copy contents
      validityRangeSetter(newVec.getValidityBuffer, 0L until size)
      newVec.getDataBuffer.setBytes(0, vector.getDataBuffer)

      new ArrowColumnVector(newVec)
    })
    first.close()
    cols
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
  def append(batch: ArrowColumnarBatchRow): Unit = {
    if (numRows.isEmpty && batch.numRows > Integer.MAX_VALUE)
      throw new RuntimeException("ArrowColumnarBatchRowBuilder batch is too large")

    var readableBytes = 0L
    var current_size = 0
    // the columns we want
    val array = numCols.fold(batch.columns)( nums => batch.columns.slice(0, nums) )
    // the rows that are left to read
    current_size = batch.numRows.toInt.min( numRows.getOrElse(Integer.MAX_VALUE) - size )
    if (size + current_size > Integer.MAX_VALUE)
      throw new RuntimeException("[ArrowColumnarBatchRowBuilder batches are too big to be combined!")

    (array, columns).zipped foreach { case (input, output) =>
      val ivector = input.getValueVector
      readableBytes = ivector.getDataBuffer.readableBytes().max(readableBytes)
      val ovector = output.getValueVector
      // make sure we have enough space
      while (ovector.getValueCapacity < size + current_size) ovector.reAlloc()
      // copy contents
      validityRangeSetter(ovector.getValidityBuffer, size.toLong until (size+current_size).toLong)
      output.getValueVector.getDataBuffer.setBytes(num_bytes, ivector.getDataBuffer)
    }
    num_bytes += readableBytes
    size += current_size
    batch.close()
  }

  /** Note: invalidates the Builder
   * Caller is responsible for closing the vectors */
  def buildColumns(): Array[ArrowColumnVector] = {
    // transfer ownership to new Array
    columns.map( column => {
      val vector = column.getValueVector
      vector.setValueCount(size)
      val tp = vector.getTransferPair(vector.getAllocator
        .newChildAllocator("ArrowColumnarBatchRowBuilder::buildColumns", 0, Integer.MAX_VALUE))
      tp.transfer()
      new ArrowColumnVector(tp.getTo)
    })
  }

  /** Note: invalidates the Builder
   * TODO: Caller is responsible for closing the vector */
  def build(): ArrowColumnarBatchRow = {
    // transfer ownership to new ArrowColumnarBatchRow
    val transferred = buildColumns()
    new ArrowColumnarBatchRow(transferred, size)
  }
}
