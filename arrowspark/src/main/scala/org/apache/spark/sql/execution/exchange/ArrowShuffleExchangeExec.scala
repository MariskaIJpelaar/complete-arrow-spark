package org.apache.spark.sql.execution.exchange

import nl.liacs.mijpelaar.utils.Resources
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ArrowShuffleWriteProcessor, ShuffleWriteMetricsReporter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateArrowColumnarBatchRowProjection
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowEncoders, ArrowColumnarBatchRowSerializer}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleReadMetricsReporter, SQLShuffleWriteMetricsReporter}
import org.apache.spark.sql.internal.ArrowConf
import org.apache.spark.util.MutablePair
import org.apache.spark.{ArrowRangePartitioner, MapOutputStatistics, ShuffleDependency, TaskContext}

import scala.concurrent.Future

/** copied and adapted from org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
 * caller should close whatever is gathered from this plan */
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
  lazy val shuffleDependency: ShuffleDependency[Array[Int], InternalRow, InternalRow] = {
    val dep = ArrowShuffleExchangeExec.prepareShuffleDependency(
      inputRDD,
      child.output,
      outputPartitioning,
      serializer,
      writeMetrics,
      ArrowConf.get(sparkContext, ArrowConf.BUCKETSEARCH_PARALLEL))
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

  // Caller should close
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] = {
    new ShuffledArrowColumnarBatchRowRDD(shuffleDependency, readMetrics, partitionSpecs)
  }

  override def runtimeStatistics: Statistics = {
    val dataSize = metrics("dataSize").value
    val rowCount = metrics(SQLShuffleWriteMetricsReporter.SHUFFLE_RECORDS_WRITTEN).value
    Statistics(dataSize, Some(rowCount))
  }

  private lazy val cachedShuffleRDD: ShuffledArrowColumnarBatchRowRDD = new ShuffledArrowColumnarBatchRowRDD(shuffleDependency, readMetrics)
  override protected def doExecute(): RDD[InternalRow] = cachedShuffleRDD.asInstanceOf[RDD[InternalRow]]

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowShuffleExchangeExec = copy(child = newChild)

}

object ArrowShuffleExchangeExec {
  def prepareShuffleDependency(
      rdd: RDD[InternalRow],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      parallel: Boolean) : ShuffleDependency[Array[Int], InternalRow, InternalRow] = {
    assert(newPartitioning.isInstanceOf[RangePartitioning])

    val RangePartitioning(sortingExpressions, numPartitions) = newPartitioning.asInstanceOf[RangePartitioning]
    // Extract only the columns that matter for sorting
    val rddForSampling = rdd.mapPartitionsInternal { iter =>
      val projection = GenerateArrowColumnarBatchRowProjection.create(sortingExpressions.map(_.child), outputAttributes)
      val mutablePair = new MutablePair[Array[Byte], Null]()
      iter.asInstanceOf[Iterator[ArrowColumnarBatchRow]].map( row => {
        try {
          mutablePair.update(ArrowColumnarBatchRowEncoders.encode(Iterator(projection(row))).toArray.apply(0), null)
        } finally {
          row.close()
        }
      })
    }

    val part = new ArrowRangePartitioner(numPartitions, rddForSampling, sortingExpressions, parallel, ascending = true)
    val rddWithPartitionIds = rdd.mapPartitionsWithIndexInternal( (_, iter) => {
      val projection = GenerateArrowColumnarBatchRowProjection.create(sortingExpressions.map(_.child), outputAttributes)
      val getPartitionKey: InternalRow => InternalRow = row => projection(row)
      iter.map {
        case batch: ArrowColumnarBatchRow => Resources.autoCloseTryGet(batch) { batch =>
          Resources.autoCloseTryGet(getPartitionKey(batch.copyFromCaller("ArrowShuffleExchangeExec::getKey")).asInstanceOf[ArrowColumnarBatchRow]) { key =>
            (part.getPartitions(key), batch.copyFromCaller("ArrowShuffleExchangeExec::return"))
          }
        }
      }
    }, isOrderSensitive = false)

    val dependency = new ShuffleDependency[Array[Int], InternalRow, InternalRow](
      rddWithPartitionIds,
      new PartitionIdPassthrough(part.numPartitions),
      serializer,
      shuffleWriterProcessor = ArrowShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics))

    dependency
  }

  /**
   * Create a customized [[ArrowShuffleWriteProcessor]] for SQL which wrap the default metrics reporter
   * with [[SQLShuffleWriteMetricsReporter]] as new reporter for [[ArrowShuffleWriteProcessor]].
   */
  def createShuffleWriteProcessor(metrics: Map[String, SQLMetric]): ArrowShuffleWriteProcessor = {
    new ArrowShuffleWriteProcessor {
      override protected def createMetricsReporter(
                                                    context: TaskContext): ShuffleWriteMetricsReporter = {
        new SQLShuffleWriteMetricsReporter(context.taskMetrics().shuffleWriteMetrics, metrics)
      }
    }
  }
}
