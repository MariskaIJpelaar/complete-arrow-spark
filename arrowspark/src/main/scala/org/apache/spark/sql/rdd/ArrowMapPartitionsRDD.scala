package org.apache.spark.sql.rdd

import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.{ArrowPartition, Partition, Partitioner, TaskContext}

import scala.reflect.ClassTag

/** Note: copied and adapted from org.apache.spark.rdd.MapPartitionsRDD
 * Purpose of this special mapRDD is to have access to the [[org.apache.arrow.memory.RootAllocator]]s of
 * the partitions */
class ArrowMapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, RootAllocator, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false) extends RDD[U](prev) {

  override def compute(split: Partition, context: TaskContext): Iterator[U] = split match {
    case arrowPartition: ArrowPartition =>
      f(context, arrowPartition.index, arrowPartition.allocator, firstParent[T].iterator(split, context))
    case _ => throw new IllegalArgumentException(s"ArrowMapPartitionsRDD only supports ArrowPartitions, not ${split.getClass.getName}")
  }

  /** Note: below is copied directly from MapPartitionsRDD */
  override val partitioner: Option[Partitioner] = if (preservesPartitioning) firstParent[T].partitioner else None
  override def getPartitions: Array[Partition] = firstParent[T].partitions
  override def clearDependencies(): Unit = {
    super.clearDependencies()
    prev = null
  }
  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())
  override protected def getOutputDeterministicLevel: DeterministicLevel.Value = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
