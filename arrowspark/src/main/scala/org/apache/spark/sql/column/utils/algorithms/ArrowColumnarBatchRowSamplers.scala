package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.RandomUtils
import org.apache.spark.sql.column.ArrowColumnarBatchRow
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

    try {
      // The first batch should be separate, so we can determine the vector-types
      val first = input.next()
      val iter = Iterator(first) ++ input
      try {
        val array = ArrowColumnarBatchRowConverters.makeFresh(first.copy())
        try {
          val rand = new XORShiftRandom(seed)
          var i = 0
          while (iter.hasNext) {
            val batch = iter.next()
            try {
              batch.columns.foreach( col => assert(col.getValueVector.getValueCount == batch.numRows) )

              0 until batch.numRows foreach { index =>
                // do sample
                if (rand.nextDouble() <= fraction) {
                  array zip batch.columns foreach { case (ours, theirs) => ours.getValueVector.copyFromSafe(index, i, theirs.getValueVector)}
                  i += 1
                }
              }
            } finally {
              batch.close()
            }
          }
          array foreach ( column => column.getValueVector.setValueCount(i) )
          // we copy to ensure we can close the array whenever we need to return early
          new ArrowColumnarBatchRow(array, i).copy()
        } finally {
          array.foreach( _.close() )
        }
      } finally {
        iter.foreach( _.close() )
      }
    } finally {
      input.foreach( _.close() )
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
    try {
      if (k < 1) (Array.empty[ArrowColumnarBatchRow], 0)

      // First, we fill the reservoir with k elements
      var inputSize = 0L
      var nrBatches = 0
      var remainderBatch: Option[ArrowColumnarBatchRow] = None
      val reservoirBuf = new ArrayBuffer[ArrowColumnarBatchRow](k)
      try {
        while (inputSize < k) {
          // ArrowColumnarBatchRow.create consumes the batches
          if (!input.hasNext) return (ArrowColumnarBatchRow.create(reservoirBuf.slice(0, nrBatches).toIterator), inputSize)

          val (batchOne, batchTwo): (ArrowColumnarBatchRow, ArrowColumnarBatchRow) =
            ArrowColumnarBatchRowConverters.split(input.next(), (k-inputSize).toInt)
          // consume them
          remainderBatch = Option(batchTwo) // should not trigger an exception
          reservoirBuf += batchOne // for now, we assume there will be no exception here...
          nrBatches += 1
          inputSize += batchOne.numRows
        }

        // closes reservoirBuf
        val reservoir = ArrowColumnarBatchRow.create(reservoirBuf.toIterator)
        try {
          // add our remainder to the iterator, if there is any
          val iter: Iterator[ArrowColumnarBatchRow] = remainderBatch.fold(input)( Iterator(_) ++ input )
          try {
            // make sure we do not use this batch anymore
            remainderBatch = None

            // we now have a reservoir with length k, in which we will replace random elements
            val rand = new RandomUtils(new XORShiftRandom(seed))

            while (iter.hasNext) {
              val sample = ArrowColumnarBatchRowTransformers.sample(iter.next(), seed)
              try {
                0 until sample.numRows foreach { index =>
                  reservoir.copyAtIndex(sample, rand.generateRandomNumber(end = k-1), index)
                }
                inputSize += sample.numRows
              } finally {
                sample.close()
              }
            }

            // we have to copy since we want to guarantee to always close reservoir
            (reservoir.copy(), inputSize)
          } finally {
            // if we returned earlier than expected, close the batches in the Iterator
            iter.foreach( _.close() )
          }
        } finally {
          reservoir.close()
        }
      } finally {
        remainderBatch.foreach( _.close() )
        reservoirBuf.foreach( _.close() )
      }
    } finally {
      // if we somehow returned earlier then expected, close all other batches as well!
      input.foreach( _.close() )
    }
  }

}
