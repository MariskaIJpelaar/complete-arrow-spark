package org.apache.spark.sql.execution.exchange

import org.apache.spark.{MapOutputStatistics, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}

import scala.concurrent.Future

/** copied and adapted from org.apache.spark.sql.execution.exchange.ShuffleExchangeExec */
class ArrowShuffleExchangeExec(override val outputPartitioning: Partitioning, child: SparkPlan, shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS) extends ShuffleExchangeLike {

  private lazy val serializer: Serializer = ???

  @transient
  lazy val shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow] = ???

  override def numMappers: Int = ???

  override def numPartitions: Int = ???

  override protected def mapOutputStatisticsFuture: Future[MapOutputStatistics] = ???

  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] = ???

  override def runtimeStatistics: Statistics = ???

  override protected def doExecute(): RDD[InternalRow] = ???

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = ???

  override def child: SparkPlan = ???

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = ???

  override def shuffleOrigin: ShuffleOrigin = ???
}
