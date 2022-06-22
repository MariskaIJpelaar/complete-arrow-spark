package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, ReturnAnswer, Sort}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.execution.exchange.{ArrowShuffleExchangeExec, ENSURE_REQUIREMENTS}
import org.apache.spark.sql.internal.SQLConf

///** Note: copied and edited from SparkStrategies::SparkStrategy */
//abstract class SpArrowStrategy extends SparkStrategy {
//  protected def ArrowPlanLater(plan: LogicalPlan with ArrowLimit): SparkPlan with ArrowLimit = ArrowLimitPlanLater(plan)
//}
//
//
///** Note: Mostly copied from SparkStrategies::PlanLater with type-change  */
//case class ArrowLimitPlanLater(plan: LogicalPlan with ArrowLimit) extends LeafExecNode with ArrowLimit {
//  override protected def doExecute(): RDD[InternalRow] = {
//    throw new UnsupportedOperationException()
//  }
//
//  override def output: Seq[Attribute] = plan.output
//
//  override def executeTakeArrow(n: Int): Array[ArrowPartition] = plan.executeTakeArrow(n)
//}


case class ArrowBasicOperators(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!plan.isInstanceOf[Sort])
      return Nil

    val sortPlan = plan.asInstanceOf[Sort]
    val distribution = physical.OrderedDistribution(sortPlan.order)
    val numPartitions = distribution.requiredNumPartitions.getOrElse(SQLConf.get.numShufflePartitions)
    val shuffleChild: SparkPlan = ArrowShuffleExchangeExec(distribution.createPartitioning(numPartitions), planLater(sortPlan.child), ENSURE_REQUIREMENTS)
    SortExec(sortPlan.order, sortPlan.global, shuffleChild) :: Nil
  }

}

// TODO: prob. can be removed
/** Plans special cases of limit operators
 *  Similar to: SpecialLimits in SparkStrategies.scala (org.apache.spark.sql.execution) */
case class SpArrowSpecialLimits(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(rootPlan) => rootPlan match {
      case Limit(IntegerLiteral(limit), child) =>
        ArrowCollectLimitExec(limit, planLater(child)) :: Nil
      case other => planLater(other) :: Nil
    }
    case _ => Nil
  }

}
