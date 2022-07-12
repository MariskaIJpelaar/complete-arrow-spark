import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column
import org.apache.spark.sql.column.AllocationManager.{createAllocator, newRoot}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowBuilder
import org.apache.spark.sql.vectorized.ArrowColumnarArray
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowColumnarBatchTestUtils

class ArrowColumnarBatchBuilderTests extends AnyFunSuite {
  def testBatches(rootAllocator: RootAllocator, batches: Seq[ArrowColumnarBatchRow]): Unit = {
    if (batches.isEmpty) return

    Resources.autoCloseTraversableTryGet(batches.map(_.copy()).toIterator) { copies =>
      Resources.autoCloseTryGet(new ArrowColumnarBatchRowBuilder(batches.head)) { builder =>
        batches.tail foreach { batch => builder.append(batch) }
        Resources.autoCloseTryGet(builder.build(createAllocator(rootAllocator, "ArrowColumnarBatchBuilderTests::testBatches"))) { answer =>
          var rowIndex = 0
          copies.zipWithIndex.foreach { case (batch, index) =>
            Resources.autoCloseTryGet(batch) { batch =>
              assertResult(batch.numFields, s"-- batch $index")(answer.numFields)
              0 until answer.numFields foreach { colIndex =>
                val answerArray = answer.getArray(colIndex).asInstanceOf[ArrowColumnarArray].getData
                val expectedArray = batch.getArray(colIndex).asInstanceOf[ArrowColumnarArray].getData
                0 until batch.numRows foreach { batchIndex =>
                  assertResult(false, s"-- colIndex: $colIndex, batchIndex: $batchIndex, rowIndex: $rowIndex")(answerArray.isNullAt(rowIndex+batchIndex))
                  val answerInt = answerArray.getInt(rowIndex + batchIndex)
                  val expectedInt = expectedArray.getInt(batchIndex)
                  assertResult(expectedInt, s"-- colIndex: $colIndex, batchIndex: $batchIndex, rowIndex: $rowIndex")(answerInt)
                }
              }
              rowIndex += batch.numRows
            }
          }
        }
      }
    }
  }


  test("ArrowColumnarBatchBuilder empty batch") {
    val empty = ArrowColumnarBatchRow.empty
    val answer = new ArrowColumnarBatchRowBuilder(empty)
      .build(createAllocator(empty.allocator.getRoot, "ArrowColumnarBatchRowBuilderTests::empty"))
    assert(empty.equals(answer))
  }

  test("ArrowColumnarBatchBuilder singleton batch") {
    val root = newRoot()
    val batch = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "singleton"))
    testBatches(root, Seq(batch))
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchBuilder two singleton batches") {
    val root = newRoot()

    val first = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "first"))
    val second = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(32)), createAllocator(root, "second"))
    testBatches(root, Seq(first, second))
    column.AllocationManager.cleanup()
  }
}
