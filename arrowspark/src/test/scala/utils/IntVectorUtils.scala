package utils

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector

object IntVectorUtils {
  /**
   * Creates an IntVector from a sequence of integers
   * FIXME: we assume creation does not fail :)
   * @param integers sequence of integers to create IntVector from
   * @param allocator BufferAllocator to allocate vector with
   * @param name (optional) a name for the vector
   * @return Created IntVector
   *         Caller is responsible to close the vector
   */
  def fromSeq(integers: Seq[Int], allocator: BufferAllocator, name : String = "IntVector"): IntVector = {
    val vector = new IntVector(name, allocator)
    vector.allocateNew(integers.size)
    integers.zipWithIndex foreach { case (num, index) =>
      vector.set(index, num)
    }
    vector.setValueCount(integers.size)
    vector
  }

}
