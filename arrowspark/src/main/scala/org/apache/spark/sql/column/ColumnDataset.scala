package org.apache.spark.sql.column

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.column.encoders.ColumnEncoder
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.language.implicitConversions

object ColumnDataset {
  def ofColumns(sparkSession: SparkSession, logicalPlan: LogicalPlan): ColumnDataFrame = sparkSession.withActive {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    val schema = qe.analyzed.schema
//    schema.fields.transform( field => field.copy(dataType = ArrayType.apply(field.dataType)) )
    val temp = ColumnEncoder(schema)
    new Dataset[ColumnBatch](qe, temp)
  }
}