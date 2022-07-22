package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.{RandomUtils, Resources}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector
import org.apache.spark.util.random.XORShiftRandom


/** Methods mapping an ArrowColumnarBatchRow to another ArrowColumnarBatchRow, also taking care of closing of the input */
object ArrowColumnarBatchRowTransformers {
  var totalTimeProjection = 0

  /**
   * Perform a projection on the batch given some expressions
   * @param batch batch to project and close
   * @param indices the sequence of indices which define the projection
   * @return a fresh batch projected from the current batch
   *         Caller is responsible for closing returned batch
   */
  def projection(batch: ArrowColumnarBatchRow, indices: Seq[Int]): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(batch) { batch =>
      val batchAllocator = createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowTransformers::projection")
      new ArrowColumnarBatchRow(batchAllocator, indices.toArray map ( index => {
        val vector = batch.columns(index).getValueVector
        val tp = vector.getTransferPair(createAllocator(batchAllocator, vector.getName))
        tp.splitAndTransfer(0, vector.getValueCount)
        new ArrowColumnVector(tp.getTo)
      }), batch.numRows)
    }
    val t2 = System.nanoTime()
    totalTimeProjection += (t2 - t1)
    ret
  }

  var totalTimeTake = 0

  /**
   * Takes a range of rows from the batch
   * @param batch batch to take rows from and close
   * @param range the range to take, assumes: 0 <= range < batch.numRows
   * @return a fresh batch
   *         Caller is responsible for closing returned batch
   */
  def take(batch: ArrowColumnarBatchRow, range: Range): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(batch) { batch =>
      // first we transfer our subset to the RootAllocator
      // but only if we require a strict subset
      val root = batch.allocator.getRoot
      val subset =
        if (range.length == batch.numRows) {
          batch.columns
        } else {
          batch.columns.map { column =>
            val vector = column.getValueVector
            val tp = vector.getTransferPair(root)
            tp.splitAndTransfer(range.head, range.length)
            new ArrowColumnVector(tp.getTo)
          }
        }
      Resources.autoCloseArrayTryGet(subset) { subset =>
        // then we copy this subset to the allocator
        val batchAllocator = createAllocator(root, "ArrowColumnarBatchRowTransformers::take()")
        new ArrowColumnarBatchRow(batchAllocator, subset map ( column => {
          val vector = column.getValueVector
          val tp = vector.getTransferPair(createAllocator(batchAllocator, vector.getName))
          tp.splitAndTransfer(0, vector.getValueCount)
          new ArrowColumnVector(tp.getTo)
        }), range.length)
      }
    }
    val t2 = System.nanoTime()
    totalTimeTake += (t2 - t1)
    ret
  }

  var totalTimeSample = 0

  /**
   * Samples a random range from a batch
   * @param batch batch to sample from and close
   * @param seed (optional) seed to generate random numbers with
   * @return a fresh random-subset of batch
   *        Callers should close returned batch
   */
  def sample(batch: ArrowColumnarBatchRow, seed: Long = System.nanoTime()): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(batch) { batch =>
      val rand = new RandomUtils(new XORShiftRandom(seed))
      val start: Int = rand.generateRandomNumber(end = batch.numRows-1)
      val end = rand.generateRandomNumber(start, batch.numRows)
      take(batch, start until end)
    }
    val t2 = System.nanoTime()
    totalTimeSample += (t2 - t1)
    ret
  }

  var totalTimeAppendColumns = 0

  /**
   * Appends the columns of two batches and creates a new batch with it
   * @param first [[ArrowColumnarBatchRow]] to take first columns from, and close
   * @param second [[ArrowColumnarBatchRow]] to take second columns from, and close
   * @param newAllocator [[BufferAllocator]] as allocator for the new batch
   * @return the newly created batch
   */
  def appendColumns(first: ArrowColumnarBatchRow, second: ArrowColumnarBatchRow, newAllocator: BufferAllocator): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
      val firstCols = first.columns map { column =>
        val vector = column.getValueVector
        val tp = vector.getTransferPair(createAllocator(newAllocator, vector.getName))
        tp.splitAndTransfer(0, vector.getValueCount)
        new ArrowColumnVector(tp.getTo)
      }
      val secondCols = second.columns map { column =>
        val vector = column.getValueVector
        val tp = vector.getTransferPair(createAllocator(newAllocator, vector.getName))
        tp.splitAndTransfer(0, vector.getValueCount)
        new ArrowColumnVector(tp.getTo)
      }
      new ArrowColumnarBatchRow(newAllocator, firstCols ++ secondCols, first.numRows)
    })
    val t2 = System.nanoTime()
    totalTimeAppendColumns += (t2 - t1)
    ret
  }

  var totalTimeGetColumns = 0

  /**
   * Returns a new batch containing the columns with the given names from the given batch
   * @param batch batch to get columns from and close
   * @param names columns to get
   * @return a fresh batch containing the subset of columns with provided names
   *         Caller is responsible for closing the batch
   */
  def getColumns(batch: ArrowColumnarBatchRow, names: Array[String]): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(batch) { batch =>
      val allocator = createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowTransformers::getColumns")
      val cols = names.flatMap { name =>
        batch.columns.find(vector => vector.getValueVector.getName.equals(name))
      }.map { column =>
        val vector = column.getValueVector
        val tp = vector.getTransferPair(createAllocator(allocator, vector.getName))
        tp.splitAndTransfer(0, vector.getValueCount)
        new ArrowColumnVector(tp.getTo)
      }
      new ArrowColumnarBatchRow(allocator, cols, batch.numRows)
    }
    val t2 = System.nanoTime()
    totalTimeGetColumns += (t2 - t1)
    ret
  }

  var totalTimeApplyIndices = 0

  /**
   * Creates a new ArrowColumnarBatchRow from the given ArrowColumnarBatchRow,
   * with rows in order of the provided indices-vector
   * First allocates in the RootAllocator of the provided batch, then with an own Allocator
   * @param batch ArrowColumnarBatchRow to create new batch from, and close
   * @param indices IntVector representing the indices to use, and close
   * @return a new Batch with permuted (subset) of rows from provided batch
   *         Caller is responsible for closing returned batch
   */
  def applyIndices(batch: ArrowColumnarBatchRow, indices: IntVector): ArrowColumnarBatchRow = {
    val t1 = System.nanoTime()
    val ret = Resources.autoCloseTryGet(batch) (batch => Resources.autoCloseTryGet(indices) { indices =>
      assert(indices.getValueCount > 0)

      // FIXME: clean up if exception is thrown within map
      val root = batch.allocator.getRoot
      val allocator = createAllocator(root, "ArrowColumnarBatchRowTransformers::applyIndices")
      new ArrowColumnarBatchRow(allocator, batch.columns map { column =>
        val vector = column.getValueVector
        assert(indices.getValueCount <= vector.getValueCount)

        // transfer type
        val tp = vector.getTransferPair(root)
        tp.splitAndTransfer(0, indices.getValueCount)
        Resources.autoCloseTryGet(tp.getTo) { newVector =>
          newVector.setInitialCapacity(indices.getValueCount)
          newVector.allocateNew()

          0 until indices.getValueCount foreach { index => newVector.copyFromSafe(indices.get(index), index, vector) }
          newVector.setValueCount(indices.getValueCount)

          // transfer to new allocator
          val newTp = newVector.getTransferPair(createAllocator(allocator, newVector.getName))
          newTp.splitAndTransfer(0, indices.getValueCount)
          new ArrowColumnVector(newTp.getTo)
        }
      }, indices.getValueCount)
    })
    val t2 = System.nanoTime()
    totalTimeApplyIndices += (t2 - t1)
    ret
  }
}
