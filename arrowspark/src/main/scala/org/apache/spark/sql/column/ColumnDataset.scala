package org.apache.spark.sql.column

import org.apache.spark.annotation.{DeveloperApi, Unstable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.column.encoders.ColumnEncoder
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Dataset, Encoder, SQLContext, SparkSession}

import scala.language.implicitConversions

object ColumnDataset {
  def ofColumns(sparkSession: SparkSession, logicalPlan: LogicalPlan): ColumnDataset = sparkSession.withActive {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    val schema = qe.analyzed.schema
    schema.fields.transform( field => field.copy(dataType = ArrayType.apply(field.dataType)) )
    val temp = ColumnEncoder(schema)
    new ColumnDataset(qe, temp)
  }

  /** implicitly forwards Dataset functions */
  implicit def toColumnDataFrame(c: ColumnDataset): ColumnDataFrame = c.dataset
}


class ColumnDataset(@DeveloperApi @Unstable @transient val queryExecution: QueryExecution,
                     @DeveloperApi @Unstable @transient val encoder: Encoder[ColumnBatch]) {

  val dataset: ColumnDataFrame = new Dataset[ColumnBatch](queryExecution, encoder)

  def this(sparkSession: SparkSession, logicalPlan: LogicalPlan, encoder: Encoder[ColumnBatch]) = {
    this(sparkSession.sessionState.executePlan(logicalPlan), encoder)
  }

  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan, encoder: Encoder[ColumnBatch]) = {
    this(sqlContext.sparkSession, logicalPlan, encoder)
  }

  def head(n: Int): Array[TColumn] = TColumn.fromBatches(dataset.head(n))
  def head(): TColumn = head(1).head
  def first(): TColumn = head()
}
