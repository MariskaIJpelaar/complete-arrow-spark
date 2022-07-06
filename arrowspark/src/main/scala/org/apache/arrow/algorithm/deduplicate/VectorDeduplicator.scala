package org.apache.arrow.algorithm.deduplicate

import org.apache.arrow.algorithm.sort.VectorValueComparator
import org.apache.arrow.vector.{IntVector, ValueVector}

/** Removes adjacent duplicate values according to a given comparator */
class VectorDeduplicator[V <: ValueVector](private val comparator: VectorValueComparator[V], private val vector: V) extends AutoCloseable{
  private val original = {
    val tp = vector.getTransferPair(vector.getAllocator.newChildAllocator("VectorDeduplicator::original", 0, Integer.MAX_VALUE))
    tp.transfer()
    tp.getTo
  }

  // returns the indices required to create de-duplicated vector from the original
  def uniqueIndices(): IntVector = {
    val indices = new IntVector("indices", original.getAllocator.newChildAllocator("VectorDeduplicator::indices", 0, Integer.MAX_VALUE))

    comparator.attachVector(original.asInstanceOf[V])
    // the first one won't be a duplicate :)
    indices.setSafe(0, 0)
    var previous_unique_index = 0
    var unique_index = 1

    0 until original.getValueCount foreach { index =>
      // found the next unique value!
      if (comparator.compare(previous_unique_index, index) != 0) {
        indices.setSafe(unique_index, index)
        unique_index += 1
        previous_unique_index = index
      }
    }

    indices.setValueCount(unique_index)
    indices
  }

  // returns the original vector with duplicates removed
  def unique(): V = {
    vector.clear()
    vector.allocateNew()

    comparator.attachVector(original.asInstanceOf[V])

    // the first one won't be a duplicate :)
    vector.copyFromSafe(0, 0, original)
    var previous_unique_index = 0
    var unique_index = 1

    0 until original.getValueCount foreach { index =>
      // found the next unique value!
      if (comparator.compare(previous_unique_index, index) != 0) {
        vector.copyFromSafe(index, unique_index, original)
        unique_index += 1
        previous_unique_index = index
      }
    }

    vector.setValueCount(unique_index)
    vector
  }

  override def close(): Unit = original.close()
}
