import nl.liacs.mijpelaar.utils.Resources
import org.apache.spark.sql.column.AllocationManager.{createAllocator, newRoot}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowConverters
import org.scalatest.funsuite.AnyFunSuite
import utils.ArrowColumnarBatchTestUtils

class ArrowColumnarBatchConvertersSplitTests extends AnyFunSuite {

  test("ArrowColumnarBatchRowConvertersSplit empty") {
    Resources.autoCloseTryGet(ArrowColumnarBatchRow.empty()) { empty =>
      val (first, second) = ArrowColumnarBatchRowConverters.split(empty.copy(), 0)
      Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
        assert(first.equals(empty))
        assert(second.equals(empty))
      })
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit singleton to left") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "singleton"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch.copy(), 1)
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
          assert(first.equals(batch))
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.empty()) ( empty => assert(second.equals(empty)) )
        })
      }
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit singleton to right") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "singleton"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch.copy(), 0)
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
          assert(second.equals(batch))
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.empty()) ( empty => assert(first.equals(empty)) )
        })
      }
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit two-ints to left") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42, 32)), createAllocator(root, "two-ints"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch.copy(), 2)
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
          assert(first.equals(batch))
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.empty()) ( empty => assert(second.equals(empty)) )
        })
      }
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit two-ints split") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42, 32)), createAllocator(root, "two-ints"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch, 1)
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
          Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "first"))) ( expected => assert(first.equals(expected)) )
          Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(32)), createAllocator(root, "second"))) ( expected => assert(second.equals(expected)) )
        })
      }
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit two-ints split reversed release") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42, 32)), createAllocator(root, "two-ints"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch, 1)
        Resources.autoCloseTryGet(second) ( second => Resources.autoCloseTryGet(first) { first =>
          Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42)), createAllocator(root, "first"))) ( expected => assert(first.equals(expected)) )
          Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(32)), createAllocator(root, "second"))) ( expected => assert(second.equals(expected)) )
        })
      }
    }
  }

  test("ArrowColumnarBatchRowConvertersSplit two-ints to right") {
    Resources.autoCloseTryGet(newRoot()) { root =>
      Resources.autoCloseTryGet(ArrowColumnarBatchTestUtils.batchFromSeqs(Seq(Seq(42, 32)), createAllocator(root, "two-ints"))) { batch =>
        val (first, second) = ArrowColumnarBatchRowConverters.split(batch.copy(), 0)
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(second) { second =>
          assert(second.equals(batch))
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.empty()) ( empty => assert(first.equals(empty)) )
        })
      }
    }
  }

}
