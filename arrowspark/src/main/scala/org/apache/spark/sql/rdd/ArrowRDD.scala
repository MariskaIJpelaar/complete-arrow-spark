package org.apache.spark.sql.rdd

import org.apache.spark.internal.config.RDD_LIMIT_SCALE_UP_FACTOR
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, createAllocator}
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowEncoders, ArrowColumnarBatchRowUtils}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// Caller should close batches in RDD
trait ArrowRDD extends RDD[ArrowColumnarBatchRow] {
  // Caller should close returned batches
  // Closes the batches in the RDD
  override def collect(): Array[ArrowColumnarBatchRow] = ArrowRDD.collect(this).map(_._2)
  // Caller should close returned batches
  // Closes the batches in the RDD
  override def take(num: Int): Array[ArrowColumnarBatchRow] = ArrowRDD.take(num, this)
  // Caller should close returned batches
  // Closes the batches in the RDD
  override def toLocalIterator: Iterator[ArrowColumnarBatchRow] = ArrowRDD.toLocalIterator(this)
}

object ArrowRDD {
  /** Returns a local iterator for each partition
   * Caller should cose batches in the returned iterator */
  def toLocalIterator(rdd: RDD[ArrowColumnarBatchRow]): Iterator[ArrowColumnarBatchRow] = {
    val childRDD = rdd.mapPartitionsInternal( res => ArrowColumnarBatchRowEncoders.encode(res))
    childRDD.toLocalIterator.flatMap( result =>
      ArrowColumnarBatchRowEncoders.decode(result).asInstanceOf[Iterator[ArrowColumnarBatchRow]]
    )
  }

  /**
   * Collect utility for rdds that contain ArrowColumnarBatchRows. Users can pass optional functions to process data
   * if the rdd has more complex data than only ArrowColumnarBatchRows
   * @param rdd rdd with the batches, which is also closed
   * @param extraEncoder (optional) split item into encoded custom-data and a batch
   * @param extraDecoder (optional) decode an array of bytes to custom-data and a batch to a single instance
   * @param extraTaker (optional) split the item from the iterator into (customData, batch)
   * @return array of custom-data and batches
   *         Caller should close the batches in the array
   */
  def collect[T: ClassTag](rdd: RDD[T],
                           extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) = batch => (Array.emptyByteArray, batch.asInstanceOf[ArrowColumnarBatchRow]),
                           extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (_, batch) => batch,
                           extraTaker: Any => (Any, ArrowColumnarBatchRow) = batch => (None, batch.asInstanceOf[ArrowColumnarBatchRow]))
                          (implicit ct: ClassTag[T]): Array[(Any, ArrowColumnarBatchRow)] = {
    val childRDD = rdd.mapPartitionsInternal { res => ArrowColumnarBatchRowEncoders.encode(res, extraEncoder = extraEncoder)}
    val res = rdd.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
      if (!it.hasNext) Array.emptyByteArray else it.next()
    })
    // FIXME: For now, we assume we do not return too early when building the buf
    val buf = new ArrayBuffer[(Any, ArrowColumnarBatchRow)]
    res.foreach(result => {
      val decoded = ArrowColumnarBatchRowEncoders.decode(result, extraDecoder = extraDecoder)
      buf ++= decoded.map( item => extraTaker(item) )
    })
    buf.toArray
  }

  /** Note: copied and adapted from RDD.scala
   * batches in RDD are consumed (closed)
   * Caller should close returned batches */
  def take(num: Int, rdd: RDD[ArrowColumnarBatchRow]): Array[ArrowColumnarBatchRow] = {
    if (num == 0) new Array[ArrowColumnarBatchRow](0)

    val scaleUpFactor = Math.max(rdd.conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
    // FIXME: For now, we assume we do not return too early
    val buf = new ArrayBuffer[ArrowColumnarBatchRow]
    val totalParts = rdd.partitions.length
    var partsScanned = 0
    val childRDD = rdd.mapPartitionsInternal { res => ArrowColumnarBatchRowEncoders.encode(res, numRows = Option(num)) }

    while (buf.size < num && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1L
      val left = num - buf.size
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        if (buf.isEmpty) {
          numPartsToTry = partsScanned * scaleUpFactor
        } else {
          // As left > 0, numPartsToTry is always >= 1
          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.size).toInt
          numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
        }
      }

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
      val res = childRDD.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
        if (!it.hasNext) Array.emptyByteArray else it.next()
      }, p)

      res.foreach(result => {
        // NOTE: we require the 'take', because we do not want more than num numRows
        val allocator = createAllocator("ArrowRDD::take")
        buf += ArrowColumnarBatchRow.create(allocator,
          ArrowColumnarBatchRowUtils.take(ArrowColumnarBatchRowEncoders.decode(result), numRows = Option(num))._2)
      })

      partsScanned += p.size
    }

    buf.toArray
  }

}
