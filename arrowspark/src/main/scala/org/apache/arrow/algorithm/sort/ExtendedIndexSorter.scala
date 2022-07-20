package org.apache.arrow.algorithm.sort

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.vector.{IntVector, ValueVector}

import scala.collection.mutable

object ExtendedIndexSorter {
  def sortManyDuplicates[V <: ValueVector](vector: V, comparator: VectorValueComparator[V]): (IntVector, Seq[Int]) = {
    comparator.attachVector(vector)
    Resources.closeOnFailGet(new IntVector("sortManyDuplicates::indices", vector.getAllocator)) { indices =>
      indices.setInitialCapacity(vector.getValueCount)
      indices.allocateNew()
      0 until vector.getValueCount foreach (index => indices.set(index, index))
      indices.setValueCount(vector.getValueCount)
      quickSort(indices, comparator)
    }
  }

  private def quickSort[V <: ValueVector](indices: IntVector, comparator: VectorValueComparator[V]): (IntVector, Seq[Int] )= {
    val borders = mutable.SortedSet[Int]()
    Resources.autoCloseTryGet(new OffHeapIntStack(indices.getAllocator)) { rangeStack =>
      rangeStack.push(0)
      rangeStack.push(indices.getValueCount - 1)

      while (!rangeStack.isEmpty) {
        val high = rangeStack.pop()
        val low = rangeStack.pop()

        if (low < high) {
          val (lt, gt) = threeWayPartition(low, high, indices, comparator)
          borders += lt
          borders += gt

          // push the larger part to the stack first,
          // to reduce the required stack size
          if (high - gt < lt - low) {
            rangeStack.push(low)
            rangeStack.push(lt)

            rangeStack.push(gt)
            rangeStack.push(high)
          } else {
            rangeStack.push(gt)
            rangeStack.push(high)

            rangeStack.push(low)
            rangeStack.push(lt)
          }
        }
      }
    }

    (indices, borders.toSeq)
  }

  /** inspired from: https://www.baeldung.com/java-sorting-arrays-with-repeated-entries
   * In particular, we use Dijkstra's Approach, as we assume the user only calls
   * 'sortManyDuplicates', when it knows it has many duplicates */
  def threeWayPartition[V <: ValueVector](low: Int, high: Int, indices: IntVector, comparator: VectorValueComparator[V]): (Int, Int) = {
    val pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator)

    var lt = low // everything left of lt, will always be strictly smaller than pivot
    var current = low // for every x with lt < x <= current ==> x = pivot
    var gt = high // everything right of gt, will always be strictly greater than pivot

    while (current < gt) {
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

    (lt, gt)
  }

  private def swap(index1: Int, index2: Int, indices: IntVector): Unit = {
    val tmp = indices.get(index1)
    indices.set(index1, indices.get(index2))
    indices.set(index2, tmp)
  }
}

