package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.vectorized.ColumnarBatchRow

trait ArrowLimit extends SparkPlan {
  def executeTakeArrow(n: Int): Array[ColumnarBatchRow]
}

// TODO: prob. can be removed
case class ArrowCollectLimitExec(limit: Int, child: SparkPlan) extends LimitExec {
  private val exec = CollectLimitExec(limit, child)

  override def executeCollect(): Array[InternalRow] = child.asInstanceOf[ArrowLimit].executeTakeArrow(limit).asInstanceOf[Array[InternalRow]]
  def executeArrowCollect(): Array[ColumnarBatchRow] = child.asInstanceOf[ArrowLimit].executeTakeArrow(limit)

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  /** Note: copied from limit.scala:CollectLimitExec */
  override protected def doExecute(): RDD[InternalRow] = exec.execute()
  override def output: Seq[Attribute] = exec.output
}
