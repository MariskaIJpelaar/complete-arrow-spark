import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column
import org.apache.spark.sql.column.AllocationManager.{createAllocator, newRoot}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowUtils
import org.apache.spark.sql.vectorized.ArrowColumnarArray
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowColumnarBatchTestUtils.batchFromSeqs

class ArrowColumnarBatchUtilsTakeTests extends AnyFunSuite {
  def testBatches(batches: Seq[ArrowColumnarBatchRow]): Unit = {
    Resources.autoCloseTraversableTryGet(batches) { batches =>
      Resources.autoCloseTraversableTryGet(batches.map(_.copy()).toIterator) { copies =>
        Resources.autoCloseArrayTryGet(ArrowColumnarBatchRowUtils.take(batches.toIterator)._2) { answer =>
          var rowIndex = 0
          copies.zipWithIndex.foreach { case (batch, index) =>
            Resources.autoCloseTryGet(batch) { batch =>
              assertResult(batch.numFields, s"batch $index")(answer.length)
              answer.indices foreach { colIndex =>
                val answerArray = answer.apply(colIndex)
                val expectedArray = batch.getArray(colIndex).asInstanceOf[ArrowColumnarArray].getData
                0 until batch.numRows foreach { batchIndex =>
                  val answerInt = answerArray.getInt(rowIndex + batchIndex)
                  val expectedInt = expectedArray.getInt(batchIndex)
                  assertResult(expectedInt, s"colIndex: $colIndex, batchIndex: $batchIndex, rowIndex: $rowIndex")(answerInt)
                }
              }
              rowIndex += batch.numRows
            }
          }
        }
      }
    }
  }

  def testSingleBatch(rootAllocator: RootAllocator, table: Seq[Seq[Int]]): Unit = {
    val batch = batchFromSeqs(table, createAllocator(rootAllocator, "ArrowColumnarBatchUtilsTakeTests::testSingleBatch"))
    testBatches(Seq(batch))
  }

  def testSingleIntVector(rootAllocator: RootAllocator, nums: Seq[Int]): Unit = testSingleBatch(rootAllocator, Seq(nums))

  test("ArrowColumnarBatchRowUtils::take() empty iterator") {
    val answer = ArrowColumnarBatchRowUtils.take(Iterator.empty)
    assertResult(0)(answer._2.length)
  }

  test("ArrowColumnarBatchRowUtils::take() single empty batch") {
    val empty = ArrowColumnarBatchRow.empty
    val answer = ArrowColumnarBatchRowUtils.take(Iterator(empty))
    assertResult(0)(answer._2.length)
  }

  test("ArrowColumnarBatchRowUtils::take() single singleton batch") {
    val root = newRoot()
    testSingleIntVector(root, Seq(42))
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, single column, four rows") {
    val root = newRoot()
    testSingleIntVector(root, Seq(42, 28, 11, 0))
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, single-row, two columns") {
    val root = newRoot()
    testSingleBatch(root, Seq(Seq(42), Seq(24)))
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, two-rows, two columns") {
    val root = newRoot()
    testSingleBatch(root, Seq(Seq(24, 42), Seq(28, 11)))
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchRowUtils::take() two singleton batches") {
    val root = newRoot()
    val firstBatch = batchFromSeqs(Seq(Seq(42)),
      createAllocator(root, "ArrowColumnarBatchUtilsTakeTests::twoSingletons::first"))
    val secondBatch = batchFromSeqs(Seq(Seq(32)),
      createAllocator(root, "ArrowColumnarBatchUtilsTakeTests::twoSingletons::second"))
    testBatches(Seq(firstBatch, secondBatch))
    column.AllocationManager.cleanup()
  }
}
