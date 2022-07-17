package org.apache

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.dataset.file.{FileFormat, FileSystemDatasetFactory}
import org.apache.arrow.dataset.jni.{NativeDataset, NativeMemoryPool, NativeScanTask, NativeScanner}
import org.apache.arrow.dataset.scanner.{ScanOptions, ScanTask, Scanner}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.spark.TaskContext
import org.apache.spark.sql.column
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

import java.io.Closeable
import java.{io, util}

class ArrowParquetReaderIterator(protected val file: PartitionedFile, protected val rootAllocator: RootAllocator) extends  Iterator[ArrowColumnarBatchRow] {
  if (file.length > column.AllocationManager.perAllocatorSize)
    throw new RuntimeException("[ArrowParquetReaderIterator] Partition is too large")

  private def scheduleClose[T <: io.Closeable](closeables: T*): Unit = {
    Option(TaskContext.get()).getOrElse(throw new RuntimeException("Not in a Spark Context")).addTaskCompletionListener[Unit](_ => {
      closeables.foreach(_.close())
    })
  }

//  /** Wrappers to help closing */
//  private case class CloseAbleDataSet(dataset: NativeDataset) extends Closeable {
//    override def close(): Unit = dataset.close()
//  }
//  private case class CloseAbleScanner(scanner: NativeScanner) extends Closeable {
//    override def close(): Unit = scanner.close()
//  }

  val scanner: Scanner = {
    Resources.autoCloseTryGet(new FileSystemDatasetFactory(rootAllocator, NativeMemoryPool.getDefault, FileFormat.PARQUET, file.filePath)) { factory =>
      val dataset = factory.finish()
      // TODO: make configurable?
      val scanner = dataset.newScan(new ScanOptions(Integer.MAX_VALUE))
      scheduleClose(dataset, scanner)
      scanner
    }
  }
  val scanTasks: util.Iterator[_ <: ScanTask] = scanner.scan().iterator()
  val loader: VectorLoader = {
    val root = VectorSchemaRoot.create(scanner.schema(), rootAllocator)
    scheduleClose(root)
    new VectorLoader(root)
  }

  val internalIter: Option[ScanTask.BatchIterator] = None

  override def hasNext: Boolean = internalIter.exists( _.hasNext ) || scanTasks.hasNext

  override def next(): ArrowColumnarBatchRow = {


    Resources.autoCloseTryGet(scanTasks.next().execute()) { reader =>
      loader.load(reader.next())



    }
  }
}
