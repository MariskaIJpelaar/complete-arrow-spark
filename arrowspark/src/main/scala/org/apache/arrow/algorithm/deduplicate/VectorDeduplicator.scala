package org.apache.arrow.algorithm.deduplicate

import org.apache.arrow.algorithm.sort.VectorValueComparator
import org.apache.arrow.vector.ValueVector

/** Removes adjacent duplicate values according to a given comparator */
class VectorDeduplicator[V <: ValueVector](private val comparator: VectorValueComparator[V], private val vector: V) {
  private val original = {
    val tp = vector.getTransferPair(vector.getAllocator)
    tp.transfer()
    tp.getTo
  }

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


}
