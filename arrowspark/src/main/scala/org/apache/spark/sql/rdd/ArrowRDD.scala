package org.apache.spark.sql.rdd

import org.apache.spark.internal.config.RDD_LIMIT_SCALE_UP_FACTOR
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.column.ArrowColumnarBatchRow

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

trait ArrowRDD extends RDD[ArrowColumnarBatchRow] {
  override def collect(): Array[ArrowColumnarBatchRow] = ArrowRDD.collect(this).map(_._2)
  override def take(num: Int): Array[ArrowColumnarBatchRow] = ArrowRDD.take(num, this)
  override def toLocalIterator: Iterator[ArrowColumnarBatchRow] = ArrowRDD.toLocalIterator(this)
}

object ArrowRDD {
  /** Returns a local iterator for each partition */
  def toLocalIterator[T](rdd: RDD[T]): Iterator[T] = {
    val childRDD = rdd.mapPartitionsInternal( res => ArrowColumnarBatchRow.encode(res))
    childRDD.toLocalIterator.flatMap( result =>
      ArrowColumnarBatchRow.decode(result).asInstanceOf[Iterator[T]]
    )
  }

  /**
   * Collect utility for rdds that contain ArrowColumnarBatchRows. Users can pass optional functions to process data
   * if the rdd has more complex data than only ArrowColumnarBatchRows
   * @param rdd rdd with the batches
   * @param extraEncoder (optional) split item into encoded custom-data and a batch
   * @param extraDecoder (optional) decode an array of bytes to custom-data and a batch to a single instance
   * @param extraTaker (optional) split the item from the iterator into (customData, batch)
   * @return array of custom-data and batches
   */
  def collect[T: ClassTag](rdd: RDD[T],
                           extraEncoder: Any => (Array[Byte], ArrowColumnarBatchRow) = batch => (Array.emptyByteArray, batch.asInstanceOf[ArrowColumnarBatchRow]),
                           extraDecoder: (Array[Byte], ArrowColumnarBatchRow) => Any = (_, batch) => batch,
                           extraTaker: Any => (Any, ArrowColumnarBatchRow) = batch => (None, batch.asInstanceOf[ArrowColumnarBatchRow]))
                          (implicit ct: ClassTag[T]): Array[(Any, ArrowColumnarBatchRow)] = {
    val childRDD = rdd.mapPartitionsInternal { res => ArrowColumnarBatchRow.encode(res, extraEncoder = extraEncoder)}
    val res = rdd.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
      if (!it.hasNext) Array.emptyByteArray else it.next()
    })
    val buf = new ArrayBuffer[(Any, ArrowColumnarBatchRow)]
    res.foreach(result => {
      val decoded = ArrowColumnarBatchRow.decode(result, extraDecoder = extraDecoder)
      buf ++= decoded.map( item => extraTaker(item) )
    })
    buf.toArray
  }

  /** Note: copied and adapted from RDD.scala */
  def take[T: ClassTag](num: Int, rdd: RDD[T])(implicit ct: ClassTag[T]): Array[T] = {
    if (num == 0) new Array[ArrowColumnarBatchRow](0)

    val scaleUpFactor = Math.max(rdd.conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
    val buf = new ArrayBuffer[ArrowColumnarBatchRow]
    val totalParts = rdd.partitions.length
    var partsScanned = 0
    val childRDD = rdd.mapPartitionsInternal { res => ArrowColumnarBatchRow.encode(res.asInstanceOf[Iterator[ArrowColumnarBatchRow]], numRows = Option(num)) }

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
      val res = rdd.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
        if (!it.hasNext) Array.emptyByteArray else it.next()
      }, p)

      res.foreach(result => {
        // NOTE: we require the 'take', because we do not want more than num numRows
        val cols = ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(result), numRows = Option(num))._2
        buf += ArrowColumnarBatchRow.create(cols)
      })

      partsScanned += p.size
    }

    buf.toArray.asInstanceOf[Array[T]]
  }
}
