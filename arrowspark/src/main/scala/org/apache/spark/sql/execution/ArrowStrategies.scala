package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.exchange.{ArrowShuffleExchangeExec, ENSURE_REQUIREMENTS}
import org.apache.spark.sql.internal.SQLConf

/** For later extensions */
abstract class ArrowSparkStrategy extends SparkStrategy {}

case class ArrowBasicOperators(spark: SparkSession) extends ArrowSparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!plan.isInstanceOf[Sort])
      return Nil

    val Sort(order, global, child) = plan.asInstanceOf[Sort]
    if (!child.isInstanceOf[LogicalRelation])
      return Nil

    child.asInstanceOf[LogicalRelation].relation match {
      case hadoop: HadoopFsRelation if hadoop.fileFormat.isInstanceOf[ArrowFileFormat] =>
        val distribution = physical.OrderedDistribution(order)
        val numPartitions = distribution.requiredNumPartitions.getOrElse(SQLConf.get.numShufflePartitions)
        val shuffleChild: SparkPlan = ArrowShuffleExchangeExec(distribution.createPartitioning(numPartitions), planLater(child), ENSURE_REQUIREMENTS)
        // TODO: ArrowCollectExec should close ArrowSortExec
        ArrowCollectExec(ArrowSortExec(order, global, shuffleChild)) :: Nil
      case _ => Nil
    }
  }
}
