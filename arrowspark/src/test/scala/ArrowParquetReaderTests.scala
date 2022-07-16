import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.util.vector.read.ParquetReaderIterator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.column.AllocationManager.newRoot
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.io.Directory

/** These tests can be used to monitor memory usage when reading in parquet files */
class ArrowParquetReaderTests extends AnyFunSuite {
  test("simpleRead") {
    val dir = Directory("data/generated")
    assert(dir.exists)

    Resources.autoCloseTryGet(newRoot()) { root =>
      dir.files foreach { file =>
        val partition = PartitionedFile(InternalRow.empty, file.path, 0, file.length)
        val iter = new ParquetReaderIterator(partition, root)
        iter.map( batch =>
          Resources.autoCloseTryGet(batch) { batch =>
            batch.numRows
          }).sum
      }
    }
    assert(true)
  }

  test("multiRead") {
    val numReads = 30
    val dir = Directory("data/generated")
    assert(dir.exists)
    0 until numReads foreach { _ =>
      Resources.autoCloseTryGet(newRoot()) { root =>
        dir.files foreach { file =>
          val partition = PartitionedFile(InternalRow.empty, file.path, 0, file.length)
          val iter = new ParquetReaderIterator(partition, root)
          iter.map( batch =>
            Resources.autoCloseTryGet(batch) { batch =>
              batch.numRows
            }).sum
        }
      }

      assert(true)
    }
  }
}
