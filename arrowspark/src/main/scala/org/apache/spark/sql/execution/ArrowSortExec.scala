package org.apache.spark.sql.execution

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** copied and adapted from org.apache.spark.sql.execution.SortExec */
case class ArrowSortExec(sortOrder: Seq[SortOrder], global: Boolean, child: SparkPlan)
  extends UnaryExecNode with BlockingOperatorWithCodegen {
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()

  private def attributeReferenceToCol(ref: AttributeReference, relation: LogicalRelation): Int = {
    relation.output.indexWhere(relRef => relRef.name.equals(ref.name))
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // initialize class member variables
    val thisPlan = ctx.addReferenceObj("plan", this)
    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val sortedIterator = ctx.addMutableState("scala.collection.Iterator<ArrowColumnarBatchRow>", "sortedIter",
      forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
         | private void $addToSorter() throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
      """.stripMargin.trim)

    val outputBatch = ctx.freshName("outputBatch")
    val sortTime = metricTerm(ctx, "sortTime")


    val code =
      s"""
       | if ($needToSort) {
       |  $addToSorterFuncName();
       |  $needToSort = false;
       | }
       |
       |
       |
       |""".stripMargin
    code
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    s"""
       |${row.code}
       |${row.value}
     """.stripMargin
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val columns = ArrowColumnarBatchRow.take(iter.asInstanceOf[Iterator[ArrowColumnarBatchRow]])
      var batch = new ArrowColumnarBatchRow(columns, if (columns.length > 0) columns(0).getValueVector.getValueCount else 0)
      /** Note: we apply the reversed SortOrder-sequence, as this assures we only have to consider one column at a time
       * (if we assume the used sorting algorithm does not do too much work) */
      sortOrder.reverseIterator foreach { order =>
        val col = attributeReferenceToCol(order.child.asInstanceOf[AttributeReference], child.asInstanceOf[LogicalRelation])
        batch = ArrowColumnarBatchRow.sort(batch, col, order)
      }

      Iterator(batch)
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowSortExec = copy(child = newChild)
}
