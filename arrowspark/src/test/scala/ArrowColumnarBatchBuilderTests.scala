import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowBuilder
import org.apache.spark.sql.vectorized.ArrowColumnarArray
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowColumnarBatchTestUtils

class ArrowColumnarBatchBuilderTests extends AnyFunSuite {
  def testBatches(batches: Seq[ArrowColumnarBatchRow]): Unit = {
    if (batches.isEmpty) return

    val copies = batches.map(_.copy()).toIterator
    val builder = new ArrowColumnarBatchRowBuilder(batches.head)
    batches.tail foreach { batch => builder.append(batch) }
    val answer = builder.build()
    try {
      var rowIndex = 0
      copies.zipWithIndex.foreach { case (batch, index) =>
        try {
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
        } finally {
          batch.close()
        }
      }
    } finally {
      answer.close()
    }
  }


  test("ArrowColumnarBatchBuilder empty batch") {
    val empty = ArrowColumnarBatchRow.empty
    val answer = new ArrowColumnarBatchRowBuilder(empty).build()
    assert(empty.equals(answer))
  }

  test("ArrowColumnarBatchBuilder singleton batch") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val batch = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)),
      allocator.newChildAllocator("singleton", 0, Integer.MAX_VALUE))
    testBatches(Seq(batch))
    allocator.close()
  }

  test("ArrowColumnarBatchBuilder two singleton batches") {
    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val first = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)),
      allocator.newChildAllocator("first", 0, Integer.MAX_VALUE))
    val second = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(32)),
      allocator.newChildAllocator("second", 0, Integer.MAX_VALUE))

    testBatches(Seq(first, second))
    allocator.close()
  }
}
