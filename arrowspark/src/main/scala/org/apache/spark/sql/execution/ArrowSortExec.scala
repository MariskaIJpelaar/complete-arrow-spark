package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow

/** copied and adapted from org.apache.spark.sql.execution.SortExec */
case class ArrowSortExec(sortOrder: Seq[SortOrder], global: Boolean, child: SparkPlan)
  extends UnaryExecNode with BlockingOperatorWithCodegen {
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = ???

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      var batch = ArrowColumnarBatchRow.take(iter.asInstanceOf[Iterator[ArrowColumnarBatchRow]])
      /** Note: we apply the reversed SortOrder-sequence, as this assures we only have to consider one column at a time
       * (if we assume the used sorting algorithm does not do too much work) */
      // TODO: for reversed SortOrders, sort
      sortOrder.reverseIterator foreach { order =>
      }

      ???
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowSortExec = copy(child = newChild)
}
