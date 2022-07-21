package org.apache.arrow.algorithm.sort

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector

import scala.collection.mutable


/** Sorters that provide the indices representing the permutation to create a sorted sequence,
 * specialized for integer-arrays
 * The indices are returned as [[IntVector]] */
object IndexIntSorters {
  /**
   * Sorts an array of integers, assuming it contains many duplicates
   * Note: very similar to org.apache.arrow.algorithm.sort.ExtendedIndexSorter
   * @param integers array containing integers to sort.
   * @param allocator [[BufferAllocator]] to create index-vector with
   * @return a Tuple2 of:
   *         * The index-vector as [[IntVector]]
   *         * A mapping of value to the last index containing that value, after sorting
   */
  def sortManyDuplicates(integers: Array[Int], allocator: BufferAllocator): (IntVector, Map[Int, Range]) = {
    Resources.closeOnFailGet(new IntVector("sortManyDuplicates::indices", allocator)) { indices =>
      indices.setInitialCapacity(integers.length)
      indices.allocateNew()
      integers.indices foreach (index => indices.set(index, index))
      indices.setValueCount(integers.length)
      (indices, quickSort(integers, indices))
    }
  }

  private def quickSort(integers: Array[Int], indices: IntVector): Map[Int, Range] = {
    if (integers.isEmpty) return Map.empty

    val partitions = mutable.Map[Int, Range]()
    Resources.autoCloseTryGet(new OffHeapIntStack(indices.getAllocator)) { rangeStack =>
      rangeStack.push(0)
      rangeStack.push(indices.getValueCount - 1)

      while (!rangeStack.isEmpty) {
        val high = rangeStack.pop()
        val low = rangeStack.pop()

        if (low < high) {
          val partition = threeWayPartition(low, high, integers, indices)
          partitions(partition.pivot) = partition.range

          // push the larger part to the stack first,
          // to reduce the required stack size
          if (high - partition.range.last < partition.range.head - low) {
            rangeStack.push(low)
            rangeStack.push(partition.range.start-1)

            rangeStack.push(partition.range.end)
            rangeStack.push(high)
          } else {
            rangeStack.push(partition.range.end)
            rangeStack.push(high)

            rangeStack.push(low)
            rangeStack.push(partition.range.start-1)
          }
        } else if (low == high) {
          partitions(integers(indices.get(low))) = low until high+1
        }
      }
    }

    partitions.toMap
  }

  private[sort] def choosePivot(low: Int, high: Int, integers: Array[Int], indices: IntVector): Int = {
    // we need at least 3 items
    if (high - low + 1 < FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD) return indices.get(low)
    val mid = low + (high - low) / 2
    // find the median by at most 3 comparisons
    var medianIdx = 0
    if (compare(integers, indices.get(low), indices.get(mid)) < 0) {
      if (compare(integers, indices.get(mid), indices.get(high)) < 0) medianIdx = mid
      else if (compare(integers, indices.get(low), indices.get(high)) < 0) medianIdx = high
      else medianIdx = low
    } else if (compare(integers, indices.get(mid), indices.get(high)) > 0) {
      medianIdx = mid
    } else if (compare(integers, indices.get(low), indices.get(high)) < 0) {
      medianIdx = low
    } else {
      medianIdx = high
    }

    indices.get(medianIdx)
  }

  /**
   * Compares two values in an array by computing their difference
   * @return
   *  - 0: if they are equal
   *  - > 0: if integers(index1) > integers(index2)
   *  - < 0: if integers(index1) < integers(index2)
   */
  @inline
  private def compare(integers: Array[Int], index1: Int, index2: Int): Int =
    integers(index1) - integers(index2)

  private case class Partition(pivot: Int, range: Range)

  /** inspired from: https://www.baeldung.com/java-sorting-arrays-with-repeated-entries
   * In particular, we use Dijkstra's Approach, as we assume the user only calls
   * 'sortManyDuplicates', when it knows it has many duplicates */
  private def threeWayPartition(low: Int, high: Int, integers: Array[Int], indices: IntVector): Partition = {
    val pivotIndex = choosePivot(low, high, integers, indices)

    var lt = low // everything left of lt, will always be strictly smaller than pivot
    var current = low // for every x with lt < x <= current ==> x = pivot
    var gt = high // everything right of gt, will always be strictly greater than pivot

    while (current <= gt) {
      val comp = compare(integers, indices.get(current), pivotIndex)
      if (comp < 0) {
        swap(current, lt, indices)
        current += 1
        lt += 1
      } else if (comp > 0) {
        swap(current, gt, indices)
        gt -= 1
      } else {
        current += 1
      }
    }

    // in the end, lt will be the first pivot-value, and gt the last
    Partition(integers(pivotIndex), lt until gt+1)
  }


  @inline
  private def swap(index1: Int, index2: Int, indices: IntVector): Unit = {
    if (index1 == index2)
      return
    val tmp = indices.get(index1)
    indices.set(index1, indices.get(index2))
    indices.set(index2, tmp)
  }
}
