package org.apache.arrow.algorithm.sort

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.IntVector


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
      ???
    }
  }

  private def quickSort(integers: Array[Int], indices: IntVector): Map[Int, Range] = {
    ???
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
  def threeWayPartition(low: Int, high: Int, integers: Array[Int], indices: IntVector): Partition = ???

}
