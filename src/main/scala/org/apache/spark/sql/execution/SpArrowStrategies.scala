package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Limit, LogicalPlan, ReturnAnswer}

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
