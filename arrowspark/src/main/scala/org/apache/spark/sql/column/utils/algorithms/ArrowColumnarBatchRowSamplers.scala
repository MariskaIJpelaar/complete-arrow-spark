package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.{RandomUtils, Resources}
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, createAllocator}
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers}
import org.apache.spark.util.random.XORShiftRandom

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ArrowColumnarBatchRowSamplers {
  /**
   * Sample rows from batches where the sample-size is determined by probability
   * @param input the batches to sample from and close
   * @param fraction the probability of a sample being taken
   * @param seed a seed for the "random"-generator
   * @return a fresh batch with the sampled rows
   *
   * Caller is responsible for closing returned batch
   */
  def sample(input: Iterator[ArrowColumnarBatchRow], fraction: Double, seed: Long): ArrowColumnarBatchRow = {
    if (!input.hasNext) ArrowColumnarBatchRow.empty

    Resources.autoCloseTryGet(input) { input =>
      val first = input.next()
      Resources.autoCloseTryGet(Iterator(first) ++ input ) { iter =>
        val batchAllocator = createAllocator("ArrowColumnarBatchRowSampler::sample::array")
        Resources.closeOnFailGet(ArrowColumnarBatchRowConverters.makeFresh(first.copy(batchAllocator))) { array =>
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
          new ArrowColumnarBatchRow(batchAllocator, array.toArray, i)
        }
      }
    }
  }

  /**
   * Reservoir sampling implementation that also returns the input size
   * Note: inspiration from org.apache.spark.util.random.RandomUtils::reservoirSampleAndCount
   * @param input input batches
   * @param k reservoir size
   * @param seed random seed
   * @return array of sampled batches and size of the input
   * Note: closes the batches in the iterator
   *
   * Caller is responsible for closing the returned batch
   */
  def sampleAndCount(input: Iterator[ArrowColumnarBatchRow], k: Int, seed: Long = Random.nextLong()):
  (ArrowColumnarBatchRow, Long) = {
    Resources.autoCloseTryGet(input) { input =>
      if (k < 1) (Array.empty[ArrowColumnarBatchRow], 0)

      // First, we fill the reservoir with k elements
      var inputSize = 0L
      var nrBatches = 0
      var remainderBatch: Option[ArrowColumnarBatchRow] = None

      Resources.autoCloseTryGet(remainderBatch) { _ =>
        Resources.closeOnFailGet(new ArrayBuffer[ArrowColumnarBatchRow](k)) { reservoirBuf =>

          val batchAllocator = createAllocator("ArrowColumnarBatchRow::sampleAndCount")
          while (inputSize < k) {
            // ArrowColumnarBatchRow.create consumes the batches
            if (!input.hasNext) return (ArrowColumnarBatchRow.create(batchAllocator, reservoirBuf.slice(0, nrBatches).toIterator), inputSize)

            val (batchOne, batchTwo): (ArrowColumnarBatchRow, ArrowColumnarBatchRow) =
              ArrowColumnarBatchRowConverters.split(input.next(), (k-inputSize).toInt)
            // consume them
            remainderBatch = Option(batchTwo) // should not trigger an exception
            reservoirBuf += batchOne // for now, we assume there will be no exception here...
            nrBatches += 1
            inputSize += batchOne.numRows
          }

          // closes reservoirBuf
          Resources.closeOnFailGet(ArrowColumnarBatchRow.create(batchAllocator, reservoirBuf.toIterator)) { reservoir =>
            // add our remainder to the iterator, if there is any
            Resources.autoCloseTryGet(remainderBatch.fold(input)( Iterator(_) ++ input )) { iter =>
              // make sure we do not use this batch anymore
              remainderBatch = None

              // we now have a reservoir with length k, in which we will replace random elements
              val rand = new RandomUtils(new XORShiftRandom(seed))

              while (iter.hasNext) {
                Resources.autoCloseTryGet(ArrowColumnarBatchRowTransformers.sample(iter.next(), seed)) { sample =>
                  0 until sample.numRows foreach { index =>
                    reservoir.copyAtIndex(sample, rand.generateRandomNumber(end = k-1), index)
                  }
                  inputSize += sample.numRows
                }
              }

              // we have to copy since we want to guarantee to always close reservoir
              (reservoir, inputSize)
            }
          }
        }
      }
    }
  }

}
