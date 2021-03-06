package org.apache.spark.sql.execution
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.rdd.ArrowRDD

// Caller should close whatever is collected from plan
case class ArrowCollectExec(child: SparkPlan) extends UnaryExecNode {
  override protected def withNewChildInternal(newChild: SparkPlan): ArrowCollectExec = copy(child = newChild)

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def output: Seq[Attribute] = child.output

  /** Caller should close batches in local iterator */
  override def executeToIterator(): Iterator[InternalRow] = {
    val rdd = execute()
    if (rdd.isInstanceOf[ArrowRDD]) rdd.toLocalIterator
    else ArrowRDD.toLocalIterator(rdd.asInstanceOf[RDD[ArrowColumnarBatchRow]])
  }

  /** Caller should close batches in Array, and the RootAllocator used to allocate them */
  override def executeCollect(): Array[InternalRow] = {
    val rdd = execute()
    if (rdd.isInstanceOf[ArrowRDD]) rdd.collect() else ArrowRDD.collect(rdd).map(_._2)
  }

  /** Caller should close batches in Array */
  override def executeTake(n: Int): Array[InternalRow] = {
    val rdd = execute()
    /** Note: no, we cannot replace this by pattern matching */
    if (rdd.isInstanceOf[ArrowRDD]) return rdd.asInstanceOf[ArrowRDD].take(n).asInstanceOf[Array[InternalRow]]
    ArrowRDD.take(n, rdd.asInstanceOf[RDD[ArrowColumnarBatchRow]]).asInstanceOf[Array[InternalRow]]
  }
}
