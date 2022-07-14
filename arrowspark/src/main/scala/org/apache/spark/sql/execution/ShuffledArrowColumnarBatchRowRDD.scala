package org.apache.spark.sql.execution

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rdd.ArrowRDD

import scala.language.implicitConversions

private case class ArrowShuffledRowRDDPartition(partition: ShuffledRowRDDPartition) extends Partition with ArrowPartition {
  override def index: Int = partition.index

}

private object ArrowShuffledRowRDDPartition {
  implicit def toShuffledRowRDDPartition(arrowShuffledRowRDDPartition: ArrowShuffledRowRDDPartition): ShuffledRowRDDPartition =
    arrowShuffledRowRDDPartition.partition
}

/** Note: copied functionalities from org.apache.spark.rdd.ShuffledRowRDD
 * Caller is responsible for closing rdd output */
class ShuffledArrowColumnarBatchRowRDD(
      var dependency: ShuffleDependency[Array[Int], InternalRow, InternalRow],
      metrics: Map[String, SQLMetric],
      partitionSpecs: Array[ShufflePartitionSpec]) extends RDD[ArrowColumnarBatchRow](dependency.rdd.context, Nil) with ArrowRDD {

  def this(
            dependency: ShuffleDependency[Array[Int], InternalRow, InternalRow],
            metrics: Map[String, SQLMetric]) = {
    this(dependency, metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }


  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.exists(!_.isInstanceOf[CoalescedPartitionSpec])) {
      None
    } else {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      if (indices.toSet.size != partitionSpecs.length) None else Option(new CoalescedPartitioner(dependency.partitioner, indices))
    }

  override protected def getPartitions: Array[Partition] =
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ArrowShuffledRowRDDPartition(ShuffledRowRDDPartition(i, partitionSpecs(i)))
    }

  override def getPreferredLocations(split: Partition): Seq[String] = split match {
    case arrowShuffledRowRDDPartition: ArrowShuffledRowRDDPartition =>
      val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
      arrowShuffledRowRDDPartition.spec match {
        case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
          // TODO order by partition size.
          startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
            tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
          }

        case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
          tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

        case PartialMapperPartitionSpec(mapIndex, _, _) =>
          tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

        case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, _) =>
          tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
      }
    case _ =>
      throw new IllegalArgumentException("ShuffledArrowColumnarBatchRowRDD::getPreferredLocations only accepts ArrowShuffledRowRDDPartitions")
  }

  // Caller is responsible for closing batches in iterator
  override def compute(split: ArrowPartition, context: TaskContext): Iterator[ArrowColumnarBatchRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val reader = split.asInstanceOf[ArrowShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          mapIndex,
          mapIndex + 1,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          0,
          numReducers,
          context,
          sqlMetricsReporter)
    }

    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2).asInstanceOf[Iterator[ArrowColumnarBatchRow]]
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }

}
