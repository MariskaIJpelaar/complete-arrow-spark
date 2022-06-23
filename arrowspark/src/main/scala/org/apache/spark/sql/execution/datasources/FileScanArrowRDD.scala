package org.apache.spark.sql.execution.datasources

import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.rdd.ArrowRDD
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkUpgradeException, TaskContext}

import java.io.{Closeable, FileNotFoundException, IOException}
import scala.reflect.runtime.universe._

/**
 * Files to scan an ArrowRDD from a partitioned file
 * inspiration from: org/apache/spark/sql/execution/datasources/FileScanRDD.scala
 *
 * TODO: should be properly tested, in particular: memory management!
 */

/**
 * An ArrowRDD that scans a list of file partitions
 * @param sparkSession the SparkSession associated with this RDD
 * @param readFunction function to read in a PartitionedArrowFile and convert it to an Iterator of Array[ValueVector]
 * @param filePartitions the partitions to operate on
 */
class FileScanArrowRDD (@transient override protected val sparkSession: SparkSession,
                                     readFunction: PartitionedFile => Iterator[ArrowColumnarBatchRow],
                                     @transient val filePartitions: Seq[FilePartition])
                                     extends ArrowRDD {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

//  override def collect(): Array[ArrowColumnarBatchRow] = {
//    val childRDD = this.mapPartitionsInternal { res => ArrowColumnarBatchRow.encode(res) }
//    val res = sparkSession.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
//      if (!it.hasNext) Array.emptyByteArray else it.next()
//    })
//    val buf = new ArrayBuffer[ArrowColumnarBatchRow]
//    res.foreach(result => {
//      val cols = ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(result))
//      buf += new ArrowColumnarBatchRow(cols, if (cols.length > 0) cols(0).getValueVector.getValueCount else 0)
//    })
//    buf.toArray
//  }
//
//  /** Note: copied and adapted from RDD.scala */
//  override def take(num: Int): Array[ArrowColumnarBatchRow] = {
//    if (num == 0) new Array[ArrowColumnarBatchRow](0)
//
//    val scaleUpFactor = Math.max(conf.get(RDD_LIMIT_SCALE_UP_FACTOR), 2)
//    val buf = new ArrayBuffer[ArrowColumnarBatchRow]
//    val totalParts = this.partitions.length
//    var partsScanned = 0
//    val childRDD = this.mapPartitionsInternal { res => ArrowColumnarBatchRow.encode(res, numRows = Option(num)) }
//
//    while (buf.size < num && partsScanned < totalParts) {
//      // The number of partitions to try in this iteration. It is ok for this number to be
//      // greater than totalParts because we actually cap it at totalParts in runJob.
//      var numPartsToTry = 1L
//      val left = num - buf.size
//      if (partsScanned > 0) {
//        // If we didn't find any rows after the previous iteration, quadruple and retry.
//        // Otherwise, interpolate the number of partitions we need to try, but overestimate
//        // it by 50%. We also cap the estimation in the end.
//        if (buf.isEmpty) {
//          numPartsToTry = partsScanned * scaleUpFactor
//        } else {
//          // As left > 0, numPartsToTry is always >= 1
//          numPartsToTry = Math.ceil(1.5 * left * partsScanned / buf.size).toInt
//          numPartsToTry = Math.min(numPartsToTry, partsScanned * scaleUpFactor)
//        }
//      }
//
//      val p = partsScanned.until(math.min(partsScanned + numPartsToTry, totalParts).toInt)
//      val res = sparkSession.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
//        if (!it.hasNext) Array.emptyByteArray else it.next()
//      }, p)
//
//      res.foreach(result => {
//        val cols = ArrowColumnarBatchRow.take(ArrowColumnarBatchRow.decode(result), numRows = Option(num))
//        buf += new ArrowColumnarBatchRow(cols, if (cols.length > 0) cols(0).getValueVector.getValueCount else 0)
//      })
//
//      partsScanned += p.size
//    }
//
//    buf.toArray
//  }

//  override def take(num: Int): Array[ArrowColumnarBatchRow] = {
//    if (num == 0)
//      new Array[ArrowColumnVector](0)
//
//    val childRDD = this.mapPartitionsInternal { res => ArrowColumnarBatchRow.encode(num, res) }
//
//    val totalParts = this.partitions.length
//    val parts = 0 until totalParts
//
//
//    val res = sparkSession.sparkContext.runJob(childRDD, (it: Iterator[Array[Byte]]) => {
//      if (!it.hasNext) Array.emptyByteArray else it.next()
//    }, parts)
//
//    res.map(result => {
//      val cols = ArrowColumnarBatchRow.take(num, ArrowColumnarBatchRow.decode(result))
//      new ArrowColumnarBatchRow(cols, if (cols.length > 0) cols(0).getValueVector.getValueCount else 0)
//    })
//  }

  override def compute(split: Partition, context: TaskContext): Iterator[ArrowColumnarBatchRow] = {
    val iterator = new Iterator[ArrowColumnarBatchRow] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback =
      SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: Option[PartitionedFile] = None
      private[this] var currentIterator : Option[Iterator[Object]] = None

      private def resetCurrentIterator(): Unit = {
        currentIterator.getOrElse(Nil) match {
          case iter: NextIterator[_] =>
            iter.closeIfNeeded()
          case iter: Closeable =>
            iter.close()
          case _ => // do nothing
        }
        currentIterator = None
      }

      private def readCurrentFile(): Iterator[ArrowColumnarBatchRow] = {
        try {
          readFunction(currentFile.get)
        } catch {
          case e: FileNotFoundException =>
            throw QueryExecutionErrors.readCurrentFileNotFoundError(e)
        }
      }

      private def nextIterator(): Boolean = {
        if (!files.hasNext) {
          currentFile = None
          InputFileBlockHolder.unset()
          return false
        }
        val nextFile = files.next()
        currentFile = Option(nextFile)
        logInfo(s"Reading File $nextFile")
        // Sets InputFileBlockHolder for the file block's information
        InputFileBlockHolder.set(nextFile.filePath, nextFile.start, nextFile.length)

        resetCurrentIterator()
        if (ignoreMissingFiles || ignoreCorruptFiles) {
          currentIterator = Option(new NextIterator[Object] {
            // The readFunction may read some bytes before consuming the iterator, e.g.,
            // vectorized Parquet reader. Here we use a lazily initialized variable to delay the
            // creation of iterator so that we will throw exception in `getNext`.
            lazy private val internalIter: Iterator[ArrowColumnarBatchRow] = readCurrentFile()

            override def getNext(): AnyRef = {
              try {
                if (internalIter.hasNext) {
                  internalIter.next()
                } else {
                  finished = true
                  null
                }
              } catch {
                case e: FileNotFoundException if ignoreMissingFiles =>
                  logWarning(s"Skipped missing file: $currentFile", e)
                  finished = true
                  null
                // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
                case e: FileNotFoundException if !ignoreMissingFiles => throw e
                case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                  logWarning(
                    s"Skipped the rest of the content in the corrupted file: $currentFile", e)
                  finished = true
                  null
              }
            }

            override def close(): Unit = {
              internalIter match {
                case iter: Closeable =>
                  iter.close()
                case _ => // do nothing
              }
            }
          })
        } else {
          currentIterator = Option(readCurrentFile())
        }

        try {
          hasNext
        } catch {
          case e: SchemaColumnConvertNotSupportedException =>
            throw QueryExecutionErrors.unsupportedSchemaColumnConvertError(
              nextFile.filePath, e.getColumn, e.getLogicalType, e.getPhysicalType, e)
          case e: ParquetDecodingException =>
            if (e.getCause.isInstanceOf[SparkUpgradeException]) {
              throw e.getCause
            } else if (e.getMessage.contains("Can not read value at")) {
              // Fixme: for some reason I cannot use cannotReadParquetFilesError(e: Exception)
              val message = "Encounter error while reading parquet files. " +
                "One possible cause: Parquet column cannot be converted in the " +
                "corresponding files. Details: "
              throw new QueryExecutionException(message, e)
            }
            throw e
        }
      }

      override def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
//        files.hasNext
        (currentIterator.isDefined && currentIterator.get.hasNext) || nextIterator()
      }

      override def next(): ArrowColumnarBatchRow = {
        val nextElement = currentIterator.get.next()
        incTaskInputMetricsBytesRead()
        nextElement match {
          case partition: ArrowColumnarBatchRow =>
            inputMetrics.incRecordsRead(partition.numFields)
            partition
        }

//        val array = new ArrayBuffer[ArrowColumnarBatchRow]()
//        nextIterator()
//
//        var numFields = 0
//
//        while (currentIterator.isDefined && currentIterator.get.hasNext) {
//          val nextElement = currentIterator.get.next()
//          incTaskInputMetricsBytesRead()
//          nextElement match {
//            case partition: ArrowColumnarBatchRow =>
//              inputMetrics.incRecordsRead(partition.numFields)
//              array += partition
//              numFields = partition.numFields
//          }
//        }
//
//        val cols = ArrowColumnarBatchRow.take(numFields, array.toIterator)
//        new ArrowColumnarBatchRow(cols, if (cols.length > 0) cols(0).getValueVector.getValueCount else 0)
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
        resetCurrentIterator()
      }
    }

    // Register an on-task-completion callback to close the input stream
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator
  }


  override protected def getPartitions: Array[Partition] = {
    filePartitions.toArray
  }

  override protected def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[FilePartition].preferredLocations()
  }


}
