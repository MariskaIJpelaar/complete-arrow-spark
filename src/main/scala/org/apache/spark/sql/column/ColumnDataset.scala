package org.apache.spark.sql.column

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.column.encoders.ColumnEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

object ColumnDataset {
  def ofColumns(sparkSession: SparkSession, logicalPlan: LogicalPlan): ColumnDataFrame = sparkSession.withActive {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    val temp = ColumnEncoder(qe.analyzed.schema)
    new Dataset[TColumn](qe, temp)
  }
}
