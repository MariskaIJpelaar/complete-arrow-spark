package org.apache.spark.sql.execution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.rdd.ArrowRDD

case class ArrowCollectExec(child: SparkPlan) extends UnaryExecNode {
  override protected def withNewChildInternal(newChild: SparkPlan): ArrowCollectExec = copy(child = newChild)

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def output: Seq[Attribute] = child.output

  override def executeToIterator(): Iterator[InternalRow] = {
    val rdd = execute()
    // TODO: Close batches in ArrowRDD
    if (rdd.isInstanceOf[ArrowRDD]) rdd.toLocalIterator
    // TODO: Close toLocalIterator
    else ArrowRDD.toLocalIterator(rdd.asInstanceOf[RDD[ArrowColumnarBatchRow]])
  }

  override def executeCollect(): Array[InternalRow] = {
    val rdd = execute()
    // TODO: Close batches in ArrowRDD
    if (rdd.isInstanceOf[ArrowRDD]) rdd.collect()
    // TODO: Close collect
    else ArrowRDD.collect(rdd).map(_._2)
  }

  override def executeTake(n: Int): Array[InternalRow] = {
    val rdd = execute()
    /** Note: no, we cannot replace this by pattern matching */
    // TODO: Close batches in ArrowRDD + take
    if (rdd.isInstanceOf[ArrowRDD]) return rdd.asInstanceOf[ArrowRDD].take(n).asInstanceOf[Array[InternalRow]]
    // TODO: Close take
    ArrowRDD.take(n, rdd.asInstanceOf[RDD[ArrowColumnarBatchRow]]).asInstanceOf[Array[InternalRow]]
  }
}
