import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.sort.IndexIntSorters
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowVectorUtils

class IndexIntSortersTests extends AnyFunSuite {
  test("Empty vector") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      val (indices, borders) = IndexIntSorters.sortManyDuplicates(Array.empty, createAllocator(root, "indices"))
      Resources.autoCloseTryGet(indices) { _ =>
        assertResult(0)(indices.getValueCount)
        assert(borders.isEmpty)
      }
    }
  }

  test("Single item") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      val expectedBorder = Map[Int, Range](42 -> (0 to 0))
      val (indices, borders) = IndexIntSorters.sortManyDuplicates(Array(42), createAllocator(root, "indices"))
      Resources.autoCloseTryGet(indices) { _ =>
        ArrowVectorUtils.checkVector(indices, Seq(0), this)
        assertResult(expectedBorder)(borders)
      }
    }
  }

  test("All duplicates") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      val values = Array(42, 42, 42, 42, 42)
      val expectedBorders = Map[Int, Range](42 -> values.indices)
      val (indices, borders) = IndexIntSorters.sortManyDuplicates(values, createAllocator(root, "indices"))
      Resources.autoCloseTryGet(indices) { _ =>
        ArrowVectorUtils.checkVector(indices, values.indices, this)
        assertResult(expectedBorders)(borders)
      }
    }
  }

  test("Mix1") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      val values = Array(64, 42, 16, 32, 42, 64, 16)
      val answer = Seq(6, 2, 3, 4, 1, 5, 0)
      val expectedBorders = Map[Int, Range](
        16 -> (0 until 2), 32 -> (2 until 3), 42 -> (3 until 5),
        64 -> (5 until 7))
      val (indices, borders) = IndexIntSorters.sortManyDuplicates(values, createAllocator(root, "indices"))
      Resources.autoCloseTryGet(indices) { _ =>
        ArrowVectorUtils.checkVector(indices, answer, this)
        assertResult(expectedBorders)(borders)
      }
    }
  }

  test("Mix2") {
    Resources.autoCloseTryGet(new RootAllocator(column.AllocationManager.perAllocatorSize)) { root =>
      val values = Array(64, 42, 42, 32, 42, 64, 16)
      val answer = Seq(6, 3, 2, 4, 1, 5, 0)
      val expectedBorders = Map[Int, Range](
        16 -> (0 until 1), 32 -> (1 until 2), 42 -> (2 until 5),
        64 -> (5 until 7))
      val (indices, borders) = IndexIntSorters.sortManyDuplicates(values, createAllocator(root, "indices"))
      Resources.autoCloseTryGet(indices) { _ =>
        ArrowVectorUtils.checkVector(indices, answer, this)
        assertResult(expectedBorders)(borders)
      }
    }
  }
}
