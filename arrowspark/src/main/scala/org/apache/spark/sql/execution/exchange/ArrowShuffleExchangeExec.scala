package org.apache.spark.sql.execution.exchange

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.{ArrowColumnarBatchRowSerializer, SQLExecution, ShufflePartitionSpec, ShuffledRowRDD, SparkPlan}
import org.apache.spark.{MapOutputStatistics, ShuffleDependency}

import scala.concurrent.Future

/** copied and adapted from org.apache.spark.sql.execution.exchange.ShuffleExchangeExec */
case class ArrowShuffleExchangeExec(override val outputPartitioning: Partitioning, child: SparkPlan, shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS) extends ShuffleExchangeLike {
  private lazy val serializer: Serializer = new ArrowColumnarBatchRowSerializer(Option(longMetric("dataSize")))

  private lazy val writeMetrics =
    SQLShuffleWriteMetricsReporter.createShuffleWriteMetrics(sparkContext)
  private[sql] lazy val readMetrics =
    SQLShuffleReadMetricsReporter.createShuffleReadMetrics(sparkContext)
  override lazy val metrics: Map[String, SQLMetric] = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions")
  ) ++ readMetrics ++ writeMetrics

  @transient lazy val inputRDD: RDD[InternalRow] = child.execute()

  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = ShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  override def numMappers: Int = shuffleDependency.rdd.getNumPartitions
  override def numPartitions: Int = shuffleDependency.partitioner.numPartitions

  override protected def mapOutputStatisticsFuture: Future[MapOutputStatistics] = {
    if (inputRDD.getNumPartitions == 0) {
      Future.successful(null)
    } else {
      sparkContext.submitMapStage(shuffleDependency)
    }
  }

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] = {
    new ShuffledRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  private lazy val cachedShuffleRDD: ShuffledRowRDD = new ShuffledRowRDD(shuffleDependency, readMetrics)
  override protected def doExecute(): RDD[InternalRow] = cachedShuffleRDD

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowShuffleExchangeExec = copy(child = newChild)

}
