package org.apache.arrow.util.vector.read

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.NativeMemoryPool
import org.apache.arrow.dataset.scanner.{ScanOptions, ScanTask, Scanner}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.spark.TaskContext
import org.apache.spark.sql.column
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer

/** inspired from: https://arrow.apache.org/cookbook/java/dataset.html#query-parquet-file */
class ArrowParquetReaderIterator(protected val file: PartitionedFile, protected val rootAllocator: RootAllocator) extends Iterator[ArrowColumnarBatchRow] with AutoCloseable {
  if (file.length > column.AllocationManager.perAllocatorSize)
    throw new RuntimeException("[ArrowParquetReaderIterator] Partition is too large")

  private val closeables: ArrayBuffer[AutoCloseable] = ArrayBuffer.empty
  def close(): Unit = {
    closeables.foreach(_.close())
  }
  private def scheduleClose[T <: AutoCloseable](closeables: T*): Unit = {
    closeables.foreach(this.closeables.append(_))
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => {
      closeables.foreach(_.close())
    }))
  }

  val scanner: Scanner = {
    // TODO: tmp:
    println(System.getProperty("java.library.path"))
    Resources.autoCloseTryGet(new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault, FileFormat.PARQUET, file.filePath)) { factory =>
      val dataset = factory.finish()
      // TODO: make configurable?
      val scanner = dataset.newScan(new ScanOptions(Integer.MAX_VALUE))
      scheduleClose(dataset, scanner)
      scanner
    }
  }
  val scanTasks: util.Iterator[_ <: ScanTask] = scanner.scan().iterator()
  val root: VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(scanner.schema(), rootAllocator)
    scheduleClose(root)
    root
  }
  val loader: VectorLoader = new VectorLoader(root)

  var internalIter: Option[ScanTask.BatchIterator] = None

  override def hasNext: Boolean = internalIter.exists( _.hasNext ) || scanTasks.hasNext

  override def next(): ArrowColumnarBatchRow = {
    if (internalIter.isEmpty || internalIter.exists(!_.hasNext)) {
      val iter = scanTasks.next().execute()
      scheduleClose(iter)
      internalIter = Option(iter)
    }

    Resources.autoCloseTryGet(internalIter
      .getOrElse( throw new RuntimeException("[ArrowParquetReaderIterator] Could not set internalIter")).next()) { recordBatch =>
      loader.load(recordBatch)
      /** Transfer ownership to new batch */
      Resources.autoCloseTraversableTryGet(root.getFieldVectors.asScala.toIterator) { data =>
        val allocator = createAllocator(rootAllocator, "ArrowParquetReaderIterator::transfer")
        val transferred = data.map { fieldVector =>
          val tp = fieldVector.getTransferPair(createAllocator(allocator, fieldVector.getName))
          tp.splitAndTransfer(0, fieldVector.getValueCount)
          new ArrowColumnVector(tp.getTo)
        }
        new ArrowColumnarBatchRow(allocator, transferred.toArray, recordBatch.getLength)
      }
    }
  }
}
