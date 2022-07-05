package org.apache.arrow.algorithm.search

import org.apache.arrow.algorithm.sort.VectorValueComparator
import org.apache.arrow.vector.ValueVector

class BucketSearcher[V <: ValueVector](
     private val keyVector: V,
     private val bucketVector: V,
     private val comparator: VectorValueComparator[V]) {

  def distribute(): Array[Int] = {
    assert(keyVector.getValueCount > 0)
    assert(bucketVector.getValueCount > 0)

    val indices = new Array[Int](keyVector.getValueCount)
    println(s"BucketSearcher: ${indices.length}")
    println(s"BucketSearcher, total memory (mb): ${Runtime.getRuntime.totalMemory / (1024*1024)}")
    println(s"BucketSearcher, free memory (mb): ${Runtime.getRuntime.freeMemory / (1024*1024)}")
    comparator.attachVectors(keyVector, bucketVector)

    0 until keyVector.getValueCount foreach { i => indices(i) = binary_search(i) }

    indices
  }

  def binary_search(keyIndex: Int): Int = {
    var low = 0
    var high = bucketVector.getValueCount -1

    var comp = 0
    var mid = 0
    while (low <= high) {
      mid = low + (high - low) / 2
      comp = comparator.compare(keyIndex, mid)
      if (comp == 0) return mid // we found an exact match, which is the upperbound of the mid-th partition
      else if (comp < 0) high = mid -1 // we need a lower partition
      else if (comp > 0) low = mid + 1 // we need a higher partition
    }

    if (comp < 0) mid else mid+1
  }

}
