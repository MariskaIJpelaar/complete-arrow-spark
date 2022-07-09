package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.RandomUtils
import org.apache.arrow.vector.IntVector
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

  /**
   * Appends an Array of columns to the provided batch and closes the original batch
   * @param batch batch to append columns to
   * @param cols columns to append to batch
   * @return a fresh batch with the columns of the original-batch and the provided columns
   *         Caller is responsible for closing the batch
   */
  def appendColumns(batch: ArrowColumnarBatchRow, cols: Array[ArrowColumnVector]): ArrowColumnarBatchRow = {
    try {
      // FIXME: better?
      new ArrowColumnarBatchRow(batch.copy().columns ++ new ArrowColumnarBatchRow(cols, batch.numRows).copy().columns, batch.numRows)
    } finally {
      batch.close()
    }
  }

  /**
   * Returns a new batch containing the columns with the given names from the given batch
   * @param batch batch to get columns from and close
   * @param names columns to get
   * @return a fresh batch containing the subset of columns with provided names
   *         TODO: Caller is responsible for closing the batch
   */
  def getColumns(batch: ArrowColumnarBatchRow, names: Array[String]): ArrowColumnarBatchRow = {
    try {
      val cols = names.flatMap { name =>
        batch.columns.find(vector => vector.getValueVector.getName.equals(name))
      }
      new ArrowColumnarBatchRow(cols, batch.numRows)
    } finally {
      batch.close()
    }
  }

  /**
   * Creates a new ArrowColumnarBatchRow from the given ArrowColumnarBatchRow,
   * with rows in order of the provided indices-vector
   * @param batch ArrowColumnarBatchRow to create new batch from, and close
   * @param indices IntVector representing the indices to use
   * @return a new Batch with permuted (subset) of rows from provided batch
   *         TODO: Caller is responsible for closing returned batch
   */
  def applyIndices(batch: ArrowColumnarBatchRow, indices: IntVector): ArrowColumnarBatchRow = {
    try {
      assert(indices.getValueCount > 0)

      new ArrowColumnarBatchRow( batch.columns map { column =>
        val vector = column.getValueVector
        assert(indices.getValueCount <= vector.getValueCount)

        // transfer type
        val tp = vector.getTransferPair(vector.getAllocator
          .newChildAllocator("ArrowColumnarBatchRowTransformers::applyIndices", 0, Integer.MAX_VALUE))
        tp.splitAndTransfer(0, indices.getValueCount)
        val new_vector = tp.getTo

        new_vector.setInitialCapacity(indices.getValueCount)
        new_vector.allocateNew()

        0 until indices.getValueCount foreach { index => new_vector.copyFromSafe(indices.get(index), index, vector) }
        new_vector.setValueCount(indices.getValueCount)

        new ArrowColumnVector(new_vector)
      }, indices.getValueCount)
    } finally {
      batch.close()
    }
  }

}
