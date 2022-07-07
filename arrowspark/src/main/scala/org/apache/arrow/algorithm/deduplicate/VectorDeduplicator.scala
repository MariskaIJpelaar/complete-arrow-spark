package org.apache.arrow.algorithm.deduplicate

import org.apache.arrow.algorithm.sort.VectorValueComparator
import org.apache.arrow.vector.{IntVector, ValueVector}

/** Removes adjacent duplicate values according to a given comparator */
object VectorDeduplicator {
  // returns the indices required to create de-duplicated vector from the original
  // does not close the provided vector
  // TODO: Caller is responsible for closing returned IntVector
  def uniqueIndices[V <: ValueVector](comparator: VectorValueComparator[V], vector: V): IntVector = {
    val indices = new IntVector("indices", vector.getAllocator.newChildAllocator("VectorDeduplicator::indices", 0, Integer.MAX_VALUE))

    comparator.attachVector(vector)
    // the first one won't be a duplicate :)
    indices.setSafe(0, 0)
    var previous_unique_index = 0
    var unique_index = 1

    0 until vector.getValueCount foreach { index =>
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
  // closes the passed vector
  // TODO: Caller is responsible for closing returned vector
  def unique[V <: ValueVector](comparator: VectorValueComparator[V], vector: V): V = {
    val original = {
      val tp = vector.getTransferPair(vector.getAllocator
        .newChildAllocator("VectorDeduplicator::unique::transfer", vector.getBufferSize, Integer.MAX_VALUE))
      tp.transfer()
      tp.getTo
    }

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
}