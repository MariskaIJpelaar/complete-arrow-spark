package org.apache.arrow.algorithm.sort

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{IntVector, ValueVector}

import scala.collection.mutable

object ExtendedIndexSorter {
  /**
   * Sorts a vector through a provided comparator, tailored at many duplicates
   * @param vector [[ValueVector]] containing values to sort. We assume this vector contains many duplicates.
   *               Otherwise, expect a performance issue.
   *               We do not close the vector
   * @param comparator [[VectorValueComparator]] on which we attach the vector, to compare its values
   * @param allocator [[BufferAllocator]] to allocate index-vector with
   * @return A Tuple2 of [[IntVector]] and a sequence of Ints
   *         The vector represents the permutation to create the sorted vector
   *         The sequence represents the borders for the duplicate values,
   *         in particular, these are the indices marking the last value for each duplicate value
   */
  def sortManyDuplicates[V <: ValueVector](vector: V, comparator: VectorValueComparator[V], allocator: BufferAllocator): (IntVector, Seq[Int]) = {
    comparator.attachVector(vector)
    Resources.closeOnFailGet(new IntVector("sortManyDuplicates::indices", allocator)) { indices =>
      indices.setInitialCapacity(vector.getValueCount)
      indices.allocateNew()
      0 until vector.getValueCount foreach (index => indices.set(index, index))
      indices.setValueCount(vector.getValueCount)
      quickSort(indices, comparator)
    }
  }

  private def quickSort[V <: ValueVector](indices: IntVector, comparator: VectorValueComparator[V]): (IntVector, Seq[Int]) = {
    if (indices.getValueCount == 0) return (indices, Seq())

    val borders = mutable.SortedSet[Int](indices.getValueCount-1)
    Resources.autoCloseTryGet(new OffHeapIntStack(indices.getAllocator)) { rangeStack =>
      rangeStack.push(0)
      rangeStack.push(indices.getValueCount - 1)

      while (!rangeStack.isEmpty) {
        val high = rangeStack.pop()
        val low = rangeStack.pop()

        if (low < high) {
          val (lte, gte) = threeWayPartition(low, high, indices, comparator)
          borders += gte

          // push the larger part to the stack first,
          // to reduce the required stack size
          if (high - gte < lte - low) {
            rangeStack.push(low)
            rangeStack.push(lte-1)

            rangeStack.push(gte+1)
            rangeStack.push(high)
          } else {
            rangeStack.push(gte+1)
            rangeStack.push(high)

            rangeStack.push(low)
            rangeStack.push(lte-1)
          }
        }
      }
    }

    (indices, borders.toSeq)
  }

  /** copied directly from IndexSorter.choosePivot,
   * with the main differences that we removed the pivot-swap
   * this saves us 2 swaps in total :) */
  private[sort] def choosePivot[T <: ValueVector](low: Int, high: Int, indices: IntVector, comparator: VectorValueComparator[T]): Int = {
    // we need at least 3 items
    if (high - low + 1 < FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD) return indices.get(low)
    val mid = low + (high - low) / 2
    // find the median by at most 3 comparisons
    var medianIdx = 0
    if (comparator.compare(indices.get(low), indices.get(mid)) < 0) {
      if (comparator.compare(indices.get(mid), indices.get(high)) < 0) medianIdx = mid
      else if (comparator.compare(indices.get(low), indices.get(high)) < 0) medianIdx = high
      else medianIdx = low
    } else if (comparator.compare(indices.get(mid), indices.get(high)) > 0) {
      medianIdx = mid
    } else if (comparator.compare(indices.get(low), indices.get(high)) < 0) {
      medianIdx = low
    } else {
      medianIdx = high
    }

    indices.get(medianIdx)
  }

  /** inspired from: https://www.baeldung.com/java-sorting-arrays-with-repeated-entries
   * In particular, we use Dijkstra's Approach, as we assume the user only calls
   * 'sortManyDuplicates', when it knows it has many duplicates */
  def threeWayPartition[V <: ValueVector](low: Int, high: Int, indices: IntVector, comparator: VectorValueComparator[V]): (Int, Int) = {
    val pivotIndex = choosePivot(low, high, indices, comparator)

    var lt = low // everything left of lt, will always be strictly smaller than pivot
    var current = low // for every x with lt < x <= current ==> x = pivot
    var gt = high // everything right of gt, will always be strictly greater than pivot

    while (current <= gt) {
      val compare = comparator.compare(indices.get(current), pivotIndex)
      if (compare < 0) {
        swap(current, lt, indices)
        current += 1
        lt += 1
      } else if (compare > 0) {
        swap(current, gt, indices)
        gt -= 1
      } else {
        current += 1
      }
    }

    // in the end, lt will be the first pivot-value, and gt the last
    (lt, gt)
  }

  private def swap(index1: Int, index2: Int, indices: IntVector): Unit = {
    if (index1 == index2)
      return
    val tmp = indices.get(index1)
    indices.set(index1, indices.get(index2))
    indices.set(index2, tmp)
  }
}

