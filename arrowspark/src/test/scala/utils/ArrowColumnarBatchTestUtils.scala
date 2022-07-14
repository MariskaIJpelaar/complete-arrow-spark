package utils

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

object ArrowColumnarBatchTestUtils {
  def batchFromSeqs(table: Seq[Seq[Int]], allocator: BufferAllocator): ArrowColumnarBatchRow = {
    var maxSize = 0
    val array: Array[ArrowColumnVector] = Array.tabulate(table.length) { index =>
      val nums = table(index)
      maxSize = maxSize.max(nums.size)

      Resources.autoCloseTryGet(utils.ArrowVectorUtils.intFromSeq(nums, allocator.getRoot, name=s"IntVector $index")) { intVec =>
        val tp = intVec.getTransferPair(createAllocator(allocator, s"Sequence $index"))
        tp.splitAndTransfer(0, intVec.getValueCount)
        new ArrowColumnVector(tp.getTo)
      }
    }
    new ArrowColumnarBatchRow(allocator, array, maxSize)
  }
}
