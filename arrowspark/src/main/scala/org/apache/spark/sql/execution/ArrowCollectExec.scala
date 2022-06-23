package org.apache.spark.sql.execution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.rdd.ArrowRDD

case class ArrowCollectExec(child: SparkPlan) extends UnaryExecNode {
  override protected def withNewChildInternal(newChild: SparkPlan): ArrowCollectExec = copy(child = newChild)

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def output: Seq[Attribute] = child.output

  override def executeCollect(): Array[InternalRow] = {
    val rdd = execute()
    rdd match {
      case arrowRDD: ArrowRDD => arrowRDD.collect().asInstanceOf[Array[InternalRow]]
      case _ => ArrowRDD.collect(rdd).asInstanceOf[Array[InternalRow]]
    }
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    val rdd = execute()
    rdd match {
      case arrowRDD: ArrowRDD => arrowRDD.take(n).asInstanceOf[Array[InternalRow]]
      case _ => ArrowRDD.take(n, rdd).asInstanceOf[Array[InternalRow]]
    }
  }
}
