package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.{RandomUtils, Resources}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ArrowColumnarBatchRowSamplers {
  var totalTimeSample = 0

  /**
   * Sample rows from batches where the sample-size is determined by probability
   * @param rootAllocator [[RootAllocator]] to use for allocation
   * @param input the batches to sample from and close
   * @param fraction the probability of a sample being taken
   * @param seed a seed for the "random"-generator
   * @return a fresh batch with the sampled rows
   *
   * Caller is responsible for closing returned batch
   */
  def sample(rootAllocator: RootAllocator, input: Iterator[ArrowColumnarBatchRow], fraction: Double, seed: Long): ArrowColumnarBatchRow = {
    if (!input.hasNext) ArrowColumnarBatchRow.empty(rootAllocator)

    val t1 = System.nanoTime()

    Resources.autoCloseTraversableTryGet(input) { input =>
      val first = input.next()
      Resources.autoCloseTraversableTryGet(Iterator(first) ++ input ) { iter =>
        val freshAllocator = createAllocator(rootAllocator, "ArrowColumnarBatchRowSampler::sample::array")
        Resources.closeArrayOnFailGet(ArrowColumnarBatchRowConverters.makeFresh(freshAllocator,
          first.copyFromCaller("ArrowColumnarBatchRowSampler::sample::array::copy"))) { array =>
          val rand = new XORShiftRandom(seed)
          var i = 0
          while (iter.hasNext) {
            val batch = iter.next()
            batch.columns.foreach( col => assert(col.getValueVector.getValueCount == batch.numRows) )

            0 until batch.numRows foreach { index =>
              // do sample
              if (rand.nextDouble() <= fraction) {
                array zip batch.columns foreach { case (ours, theirs) => ours.getValueVector.copyFromSafe(index, i, theirs.getValueVector)}
                i += 1
              }
            }
          }
          array foreach ( column => column.getValueVector.setValueCount(i) )
          val ret = new ArrowColumnarBatchRow(freshAllocator, array, i)
          val t2 = System.nanoTime()
          totalTimeSample += (t2 - t1)
          ret
        }
      }
    }
  }

  var totalTimeSampleAndCount = 0

  /**
   * Reservoir sampling implementation that also returns the input size
   * Note: inspiration from org.apache.spark.util.random.RandomUtils::reservoirSampleAndCount
   * @param rootAllocator [[RootAllocator]] to allocate with
   * @param input input batches
   * @param k reservoir size
   * @param seed random seed
   * @return array of sampled batches and size of the input
   * Note: closes the batches in the iterator
   *
   * Caller is responsible for closing the returned batch
   */
  def sampleAndCount(rootAllocator: RootAllocator, input: Iterator[ArrowColumnarBatchRow], k: Int, seed: Long = Random.nextLong()):
  (ArrowColumnarBatchRow, Long) = {
    Resources.autoCloseTraversableTryGet(input) { input =>
      if (k < 1) return (ArrowColumnarBatchRow.empty(rootAllocator), 0)

      val t1 = System.nanoTime()

      // First, we fill the reservoir with k elements
      var inputSize = 0L
      var nrBatches = 0
      var remainderBatch: Option[ArrowColumnarBatchRow] = None

      try {
        Resources.closeTraversableOnFailGet(new ArrayBuffer[ArrowColumnarBatchRow](k)) { reservoirBuf =>
          if (!input.hasNext) return (ArrowColumnarBatchRow.empty(rootAllocator), 0)

          while (inputSize < k) {
            // ArrowColumnarBatchRow.create consumes the batches
            if (!input.hasNext) return (ArrowColumnarBatchRow.create(rootAllocator, reservoirBuf.slice(0, nrBatches).toIterator), inputSize)

            val (batchOne, batchTwo): (ArrowColumnarBatchRow, ArrowColumnarBatchRow) =
              ArrowColumnarBatchRowConverters.split(input.next(), (k-inputSize).toInt)
            // consume them
            remainderBatch.foreach(_.close())
            remainderBatch = Option(batchTwo) // should not trigger an exception
            reservoirBuf += batchOne // for now, we assume there will be no exception here...
            // set batch allocator if this was not done before
            nrBatches += 1
            inputSize += batchOne.numRows
          }

          // closes reservoirBuf
          Resources.closeOnFailGet(ArrowColumnarBatchRow.create(rootAllocator, reservoirBuf.toIterator)) { reservoir =>
            // add our remainder to the iterator, if there is any
            Resources.autoCloseTraversableTryGet(remainderBatch.fold(input)( Iterator(_) ++ input )) { iter =>
              // make sure we do not use this batch anymore
              remainderBatch = None

              // we now have a reservoir with length k, in which we will replace random elements
              val rand = new RandomUtils(new XORShiftRandom(seed))

              while (iter.hasNext) {
                Resources.autoCloseTryGet(iter.next()) { batch =>
                  inputSize += batch.numRows
                  Resources.autoCloseTryGet(ArrowColumnarBatchRowTransformers.sample(batch, seed)) { sample =>
                    0 until sample.numRows foreach { index =>
                      reservoir.copyAtIndex(sample, rand.generateRandomNumber(end = k-1), index)
                    }
                  }
                }
              }

              (reservoir, inputSize)
            }
          }
        }
      } finally {
        remainderBatch.foreach(_.close())
        val t2 = System.nanoTime()
        totalTimeSampleAndCount += (t2 - t1)
      }
    }
  }

}
