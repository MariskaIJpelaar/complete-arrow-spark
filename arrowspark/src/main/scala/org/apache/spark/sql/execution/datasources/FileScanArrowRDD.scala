package org.apache.spark.sql.execution.datasources

import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.vectorized.ColumnarBatchRow
import org.apache.spark.util.NextIterator
import org.apache.spark.{Partition, SparkUpgradeException, TaskContext}

import java.io.{Closeable, FileNotFoundException, IOException}
import scala.reflect.ClassTag
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
 * @tparam T the RDD primitive data type (as defined by the Scala Standard)
 */
class FileScanArrowRDD[T: ClassTag] (@transient private val sparkSession: SparkSession,
                                     readFunction: PartitionedFile => Iterator[ColumnarBatchRow],
                                     @transient val filePartitions: Seq[FilePartition])
                                    (implicit tag: TypeTag[T])
                                     extends RDD[T](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val iterator = new Iterator[Object] with AutoCloseable {
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

      private def readCurrentFile(): Iterator[ColumnarBatchRow] = {
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
        currentFile = Some(nextFile)
        logInfo(s"Reading File $nextFile")
        // Sets InputFileBlockHolder for the file block's information
        InputFileBlockHolder.set(nextFile.filePath, nextFile.start, nextFile.length)

        resetCurrentIterator()
        if (ignoreMissingFiles || ignoreCorruptFiles) {
          currentIterator = Some(new NextIterator[Object] {
            // The readFunction may read some bytes before consuming the iterator, e.g.,
            // vectorized Parquet reader. Here we use a lazily initialized variable to delay the
            // creation of iterator so that we will throw exception in `getNext`.
            lazy private val internalIter: Iterator[ColumnarBatchRow] = readCurrentFile()

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
          currentIterator = Some(readCurrentFile())
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
        (currentIterator.isDefined && currentIterator.get.hasNext) || nextIterator()
      }

      override def next(): Object = {
        val nextElement = currentIterator.get.next()
        incTaskInputMetricsBytesRead()
        nextElement match {
          case partition: ColumnarBatchRow => inputMetrics.incRecordsRead(partition.numFields)
        }
        nextElement
      }

      override def close(): Unit = {
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
        resetCurrentIterator()
      }
    }

    // Register an on-task-completion callback to close the input stream
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[T]]
  }


  override protected def getPartitions: Array[Partition] = {
    filePartitions.toArray
  }

  override protected def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[FilePartition].preferredLocations()
  }


}