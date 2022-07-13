package utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{Float4Vector, IntVector}

object ArrowVectorUtils {
  /**
   * Creates an IntVector from a sequence of integers
   * FIXME: we assume creation does not fail :)
   * @param integers sequence of integers to create IntVector from
   * @param allocator BufferAllocator to allocate vector with
   * @param name (optional) a name for the vector
   * @return Created IntVector
   *         Caller is responsible to close the vector
   */
  def intFromSeq(integers: Seq[Int], allocator: BufferAllocator, name: String = "IntVector"): IntVector = {
    val vector = new IntVector(name, allocator)
    vector.allocateNew(integers.size)
    integers.zipWithIndex foreach { case (num, index) =>
      vector.set(index, num)
    }
    vector.setValueCount(integers.size)
    vector
  }

  def floatFromSeq(floats: Seq[Double], allocator: BufferAllocator, name: String = "Float4Vector" ): Float4Vector = {
    val vector = new Float4Vector(name, allocator)
    vector.allocateNew(floats.size)
    floats.zipWithIndex foreach { case (num, index) =>
      vector.set(index, num.toFloat)
    }
    vector.setValueCount(floats.size)
    vector
  }

}
