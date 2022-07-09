package utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

object ArrowColumnarBatchTestUtils {
  def batchFromSeqs(table: Seq[Seq[Int]], allocator: BufferAllocator): ArrowColumnarBatchRow = {
    var maxSize = 0
    val array: Array[ArrowColumnVector] = Array.tabulate(table.length) { index =>
      val nums = table(index)
      maxSize = maxSize.max(nums.size)
      new ArrowColumnVector(utils.IntVectorUtils.fromSeq(nums,
        allocator.newChildAllocator(s"Sequence $index", 0, Integer.MAX_VALUE),
        name=s"IntVector $index"))
    }
    new ArrowColumnarBatchRow(array, maxSize)
  }
}
