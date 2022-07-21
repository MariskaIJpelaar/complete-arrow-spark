import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, ExtendedIndexSorter}
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowVectorUtils

class ExtendedIndexSorterTests extends AnyFunSuite {
  test("Empty vector") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      Resources.autoCloseTryGet(createAllocator(root, "empty vector")) { emptyAllocator =>
        Resources.autoCloseTryGet(ArrowVectorUtils.intFromSeq(Seq(), emptyAllocator, name = "empty vector")) { intVector =>
          val (indices, borders) =
            ExtendedIndexSorter.sortManyDuplicates(intVector,
              DefaultVectorComparators.createDefaultComparator(intVector), createAllocator(root, "indices"))
          Resources.autoCloseTryGet(indices) { _ =>
            assertResult(0)(indices.getValueCount)
            assert(borders.isEmpty)
          }
        }
      }
    }
  }

  test("Single Item") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      Resources.autoCloseTryGet(createAllocator(root, "singleton")) { emptyAllocator =>
        Resources.autoCloseTryGet(ArrowVectorUtils.intFromSeq(Seq(42), emptyAllocator, name = "singleton vector")) { intVector =>
          val (indices, borders) =
            ExtendedIndexSorter.sortManyDuplicates(intVector,
              DefaultVectorComparators.createDefaultComparator(intVector), createAllocator(root, "indices"))
          Resources.autoCloseTryGet(indices) { _ =>
            ArrowVectorUtils.checkVector(indices, Seq(0), this)
            assertResult(Seq(0))(borders)
          }
        }
      }
    }
  }

  test("All duplicates") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      Resources.autoCloseTryGet(createAllocator(root, "duplicates")) { emptyAllocator =>
        val values = Seq(42, 42, 42, 42, 42)
        Resources.autoCloseTryGet(ArrowVectorUtils.intFromSeq(values, emptyAllocator, name = "duplicates")) { intVector =>
          val (indices, borders) =
            ExtendedIndexSorter.sortManyDuplicates(intVector,
              DefaultVectorComparators.createDefaultComparator(intVector), createAllocator(root, "indices"))
          Resources.autoCloseTryGet(indices) { _ =>
            ArrowVectorUtils.checkVector(indices, values.indices, this)
            assertResult(Seq(values.length-1))(borders)
          }
        }
      }
    }
  }

  test("Mix1") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      Resources.autoCloseTryGet(createAllocator(root, "mix")) { emptyAllocator =>
        val values = Seq(64, 42, 16, 32, 42, 64, 16)
        val answer = Seq(6, 2, 3, 4, 1, 5, 0)
        val answerBorders = Seq(1, 2, 4, 6)
        Resources.autoCloseTryGet(ArrowVectorUtils.intFromSeq(values, emptyAllocator, name = "mix-vector")) { intVector =>
          val (indices, borders) =
            ExtendedIndexSorter.sortManyDuplicates(intVector,
              DefaultVectorComparators.createDefaultComparator(intVector), createAllocator(root, "indices"))
          Resources.autoCloseTryGet(indices) { _ =>
            ArrowVectorUtils.checkVector(indices, answer, this)
            assertResult(answerBorders)(borders)
          }
        }
      }
    }
  }

  test("Mix2") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      Resources.autoCloseTryGet(createAllocator(root, "mix")) { emptyAllocator =>
        val values = Seq(64, 42, 42, 32, 42, 64, 16)
        val answer = Seq(6, 3, 2, 4, 1, 5, 0)
        val answerBorders = Seq(1, 4, 6)
        Resources.autoCloseTryGet(ArrowVectorUtils.intFromSeq(values, emptyAllocator, name = "mix-vector")) { intVector =>
          val (indices, borders) =
            ExtendedIndexSorter.sortManyDuplicates(intVector,
              DefaultVectorComparators.createDefaultComparator(intVector), createAllocator(root, "indices"))
          Resources.autoCloseTryGet(indices) { _ =>
            ArrowVectorUtils.checkVector(indices, answer, this)
            assertResult(answerBorders)(borders)
          }
        }
      }
    }
  }
}
