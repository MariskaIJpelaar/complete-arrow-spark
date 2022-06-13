package org.apache.spark.sql.execution

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

case class ColumnarTransitionBlockerStrategy(spark: SparkSession) extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = ColumnarTransitionBlocker(plan.asInstanceOf[SparkPlan]) :: Nil
}
