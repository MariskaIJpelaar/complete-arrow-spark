import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowUtils
import org.apache.spark.sql.vectorized.ArrowColumnarArray
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowColumnarBatchTestUtils.batchFromSeqs

class ArrowColumnarBatchUtilsTakeTests extends AnyFunSuite {
  def testBatches(batches: Seq[ArrowColumnarBatchRow]): Unit = {
    val copies = batches.map( _.copy() ).toIterator
    val answer = ArrowColumnarBatchRowUtils.take(batches.toIterator)._2
    try {
      var rowIndex = 0
      copies.zipWithIndex.foreach { case (batch, index) =>
        try {
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
        } finally {
          batch.close()
        }
      }
    } finally {
      answer.foreach(_.close())
      copies.foreach(_.close())
    }
  }

  def testSingleBatch(table: Seq[Seq[Int]]): Unit = {
    val allocator: BufferAllocator = new RootAllocator(Integer.MAX_VALUE)
    val batch = batchFromSeqs(table, allocator)
    testBatches(Seq(batch))
    allocator.close()
  }

  def testSingleIntVector(nums: Seq[Int]): Unit = testSingleBatch(Seq(nums))

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
    testSingleIntVector(Seq(42))
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, single column, four rows") {
    testSingleIntVector(Seq(42, 28, 11, 0))
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, single-row, two columns") {
    testSingleBatch(Seq(Seq(42), Seq(24)))
  }

  test("ArrowColumnarBatchRowUtils::take() single batch, two-rows, two columns") {
    testSingleBatch(Seq(Seq(24, 42), Seq(28, 11)))
  }

  test("ArrowColumnarBatchRowUtils::take() two singleton batches") {
    val allocator: BufferAllocator = new RootAllocator(Integer.MAX_VALUE)
    val firstBatch = batchFromSeqs(Seq(Seq(42)), allocator.newChildAllocator("first", 0, Integer.MAX_VALUE))
    val secondBatch = batchFromSeqs(Seq(Seq(32)), allocator.newChildAllocator("second", 0, Integer.MAX_VALUE))
    testBatches(Seq(firstBatch, secondBatch))
    allocator.close()
  }
}
