import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ValueVector
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
    val root = newRoot()
    val empty = ArrowColumnarBatchRow.empty(root)
    val answer = new ArrowColumnarBatchRowBuilder(empty)
      .build(createAllocator(empty.allocator.getRoot, "ArrowColumnarBatchRowBuilderTests::empty"))
    assert(empty.equals(answer))
    empty.close()
    root.close()
  }

  test("ArrowColumnarBatchBuilder singleton batch") {
    column.AllocationManager.reset()
    val root = newRoot()
    val batch = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "singleton"))
    testBatches(root, Seq(batch))
    root.close()
    column.AllocationManager.cleanup()
  }

  test("ArrowColumnarBatchBuilder two singleton batches") {
    column.AllocationManager.reset()
    val root = newRoot()

    val first = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "first"))
    val second = ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(32)), createAllocator(root, "second"))
    testBatches(root, Seq(first, second))
    column.AllocationManager.cleanup()
  }

  // FIXME: create testBatches for multi-typed vectors
  test("Multi-typed vectors") {
    column.AllocationManager.reset()
    val root = newRoot()

    val numVecs = 2
    val numRows = 2
    val intVec: ValueVector = utils.ArrowVectorUtils.intFromSeq(Seq(32, 42), root, name = "intVec")
    val floatVec: ValueVector = utils.ArrowVectorUtils.floatFromSeq(Seq(1.0, 1.5), root, name = "floatVec")

    Resources.autoCloseTryGet(ArrowColumnarBatchRow.transfer(root, "batch", Array(intVec, floatVec))) { batch =>
      Resources.autoCloseTryGet(batch.copy()) { copy =>
        Resources.autoCloseTryGet(new ArrowColumnarBatchRowBuilder(batch)) { builder =>
          Resources.autoCloseTryGet(builder.build(createAllocator(root, "ArrowColumnarBatchBuilderTests::multiTyped"))) { answer =>
            assertResult(numVecs)(answer.numFields)
            // IntVector
            // FIXME: find better way to create scope
            if (true) {
              val answerArray = answer.getArray(0).asInstanceOf[ArrowColumnarArray].getData
              val expectedArray = copy.getArray(0).asInstanceOf[ArrowColumnarArray].getData
              assertResult(expectedArray.getValueVector.getValueCount)(answerArray.getValueVector.getValueCount)
              assertResult(expectedArray.getValueVector.getBufferSize)(answerArray.getValueVector.getBufferSize)
              0 until numRows foreach { rowIndex =>
                assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowIndex))
                val answerInt = answerArray.getInt(rowIndex)
                val expectedInt = expectedArray.getInt(rowIndex)
                assertResult(expectedInt, s"-- rowIndex: $rowIndex")(answerInt)
              }
            }

            // FloatVector
            val answerArray = answer.getArray(1).asInstanceOf[ArrowColumnarArray].getData
            val expectedArray = copy.getArray(1).asInstanceOf[ArrowColumnarArray].getData
            assertResult(expectedArray.getValueVector.getValueCount)(answerArray.getValueVector.getValueCount)
            assertResult(expectedArray.getValueVector.getBufferSize)(answerArray.getValueVector.getBufferSize)
            0 until numRows foreach { rowIndex =>
              assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowIndex))
              val answerFloat = answerArray.getFloat(rowIndex)
              val expectedFloat = expectedArray.getFloat(rowIndex)
              assertResult(expectedFloat, s"-- rowIndex: $rowIndex")(answerFloat)
            }
          }
        }
      }
    }

    column.AllocationManager.cleanup()
  }

  test("Multi-typed vectors for multiple batches") {
    column.AllocationManager.reset()
    val root = newRoot()

    val numVecs = 2
    val numRows = 2
    val intVec: ValueVector = utils.ArrowVectorUtils.intFromSeq(Seq(32, 42), root, name = "intVec")
    val floatVec: ValueVector = utils.ArrowVectorUtils.floatFromSeq(Seq(1.0, 1.5), root, name = "floatVec")

    Resources.autoCloseTryGet(ArrowColumnarBatchRow.transfer(root, "batch", Array(intVec, floatVec))) { batch =>
      Resources.autoCloseTryGet(batch.copy()) { copy =>
        Resources.autoCloseTryGet(new ArrowColumnarBatchRowBuilder(batch)) { builder =>
          val intVec2: ValueVector = utils.ArrowVectorUtils.intFromSeq(Seq(16, 54), root, name = "intVec")
          val floatVec2: ValueVector = utils.ArrowVectorUtils.floatFromSeq(Seq(0.8, 1.6), root, name = "floatVec")
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.transfer(root, "batch2", Array(intVec2, floatVec2))) { batch2 =>
            Resources.autoCloseTryGet(batch2.copy()) { copy2 =>
              builder.append(batch2)
              Resources.autoCloseTryGet(builder.build(createAllocator(root, "ArrowColumnarBatchBuilderTests::multiTyped"))) { answer =>
                assertResult(numVecs)(answer.numFields)
                // IntVector 1
                if (true) {
                  val answerArray = answer.getArray(0).asInstanceOf[ArrowColumnarArray].getData
                  val expectedArray = copy.getArray(0).asInstanceOf[ArrowColumnarArray].getData
                  0 until numRows foreach { rowIndex =>
                    assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowIndex))
                    val answerInt = answerArray.getInt(rowIndex)
                    val expectedInt = expectedArray.getInt(rowIndex)
                    assertResult(expectedInt, s"-- rowIndex: $rowIndex")(answerInt)
                  }
                }

                // FloatVector 1
                if (true) {
                  val answerArray = answer.getArray(1).asInstanceOf[ArrowColumnarArray].getData
                  val expectedArray = copy.getArray(1).asInstanceOf[ArrowColumnarArray].getData
                  0 until numRows foreach { rowIndex =>
                    assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowIndex))
                    val answerFloat = answerArray.getFloat(rowIndex)
                    val expectedFloat = expectedArray.getFloat(rowIndex)
                    assertResult(expectedFloat, s"-- rowIndex: $rowIndex")(answerFloat)
                  }
                }

                val rowStart = numRows

                // IntVector 2
                if (true) {
                  val answerArray = answer.getArray(0).asInstanceOf[ArrowColumnarArray].getData
                  val expectedArray = copy2.getArray(0).asInstanceOf[ArrowColumnarArray].getData
                  0 until numRows foreach { rowIndex =>
                    assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowStart + rowIndex))
                    val answerInt = answerArray.getInt(rowStart + rowIndex)
                    val expectedInt = expectedArray.getInt(rowIndex)
                    assertResult(expectedInt, s"-- rowIndex: $rowIndex")(answerInt)
                  }
                }

                // FloatVector 2
                if (true) {
                  val answerArray = answer.getArray(1).asInstanceOf[ArrowColumnarArray].getData
                  val expectedArray = copy2.getArray(1).asInstanceOf[ArrowColumnarArray].getData
                  0 until numRows foreach { rowIndex =>
                    assertResult(false, s"-- rowIndex: $rowIndex")(answerArray.isNullAt(rowStart + rowIndex))
                    val answerFloat = answerArray.getFloat(rowStart + rowIndex)
                    val expectedFloat = expectedArray.getFloat(rowIndex)
                    assertResult(expectedFloat, s"-- rowIndex: $rowIndex")(answerFloat)
                  }
                }
              }
            }
          }
        }
      }
    }

    column.AllocationManager.cleanup()
  }
}
