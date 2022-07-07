package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.RandomUtils
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.random.XORShiftRandom

/** Methods mapping an ArrowColumnarBatchRow to another ArrowColumnarBatchRow, also taking care of closing of the input */
object ArrowColumnarBatchRowTransformers {
  /**
   * Perform a projection on the batch given some expressions
   * @param batch batch to project and close
   * @param indices the sequence of indices which define the projection
   * @return a fresh batch projected from the current batch
   *         TODO: Caller is responsible for closing returned batch
   */
  def projection(batch: ArrowColumnarBatchRow, indices: Seq[Int]): ArrowColumnarBatchRow = {
    try {
      new ArrowColumnarBatchRow( indices.toArray map ( index => {
        val vector = batch.columns(index).getValueVector
        val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRow::projection", 0, Integer.MAX_VALUE))
        tp.splitAndTransfer(0, batch.numRows)
        new ArrowColumnVector(tp.getTo)
      }), batch.numRows)
    } finally {
      batch.close()
    }
  }

  /**
   * Takes a range of rows from the batch
   * @param batch batch to take rows from and close
   * @param range the range to take, assumes: 0 <= range < batch.numRows
   * @return a fresh batch
   *         TODO: Caller is responsible for closing returned batch
   */
  def take(batch: ArrowColumnarBatchRow, range: Range): ArrowColumnarBatchRow = {
    try {
      new ArrowColumnarBatchRow( batch.columns map ( column => {
        val vector = column.getValueVector
        val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("ArrowColumnarBatchRowTransformers::take()", 0, Integer.MAX_VALUE))
        tp.splitAndTransfer(range.head, range.length)
        new ArrowColumnVector(tp.getTo)
      }), range.length)
    } finally {
      batch.close()
    }
  }

  /**
   * Samples a random range from a batch
   * @param batch batch to sample from and close
   * @param seed (optional) seed to generate random numbers with
   * @return a fresh random-subset of batch
   *         TODO: Callers should close returned batch
   */
  def sample(batch: ArrowColumnarBatchRow, seed: Long = System.nanoTime()): ArrowColumnarBatchRow = {
    try {
      val rand = new RandomUtils(new XORShiftRandom(seed))
      val start: Int = rand.generateRandomNumber(end = batch.numRows-1)
      val end = rand.generateRandomNumber(start, batch.numRows)
      take(batch, start until end)
    } finally {
      batch.close()
    }
  }

}