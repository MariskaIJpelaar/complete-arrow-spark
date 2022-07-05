package org.apache.arrow.algorithm.sort

import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.complex.UnionVector


/** Comparator for a UnionVector to sort according to a given SortOrder */
class SparkUnionComparator(comparators: Array[(String, SparkComparator[ValueVector])]) extends VectorValueComparator[UnionVector] {
  override def compareNotNull(index1: Int, index2: Int): Int = {
    var compare = 0
    comparators.takeWhile { case (_, comparator) =>
      compare = comparator.compareNotNull(index1, index2)
      compare == 0
    }
    compare
  }

  override def compare(index1: Int, index2: Int): Int = {
    var compare = 0
    comparators.takeWhile { case (_, comparator) =>
      compare = comparator.compare(index1, index2)
      compare == 0
    }
    compare
  }

  override def attachVector(vector: UnionVector): Unit = {
    super.attachVector(vector)
    comparators.foreach { case (name, comparator) => comparator.attachVector(vector.getChild(name)) }
  }

  override def attachVectors(vector1: UnionVector, vector2: UnionVector): Unit = {
    super.attachVectors(vector1, vector2)
    comparators.foreach { case (name, comparator) =>
      comparator.attachVectors(vector1.getChild(name), vector2.getChild(name))
    }
  }

  override def createNew(): VectorValueComparator[UnionVector] = new SparkUnionComparator(comparators)
}
