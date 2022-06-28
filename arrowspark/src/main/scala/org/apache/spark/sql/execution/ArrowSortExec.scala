package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.util
import scala.collection.mutable.ArrayBuffer

/** copied and adapted from org.apache.spark.sql.execution.SortExec */
case class ArrowSortExec(sortOrder: Seq[SortOrder], global: Boolean, child: SparkPlan)
  extends UnaryExecNode with BlockingOperatorWithCodegen {
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()


  /** Needs to be called from codegen */
  def attributeReferenceToCol(order: SortOrder): Int = {
    child.output.indexWhere(relRef => relRef.name.equals(order.child.asInstanceOf[AttributeReference].name))
  }

  private val partitions: scala.collection.mutable.Buffer[ArrowColumnarBatchRow] = new ArrayBuffer[ArrowColumnarBatchRow]()
  private var partitionsIdx: Int = _
  private var thisPartitions: String = _
  private var sortedIdx: Int = _
  private var sortedBatch: ArrowColumnarBatchRow = new ArrowColumnarBatchRow(Array.empty[ArrowColumnVector], 0)
  private var thisSorted: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // initialize class member variables
    val thisPlan = ctx.addReferenceObj("plan", this)
    val thisChild = ctx.addReferenceObj("child", child)
//    val metrics = ctx.addMutableState(classOf[TaskMetrics].getName, "metrics",
//      v => s"$v = org.apache.spark.TaskContext.get().taskMetrics();", forceInline = true)
    val orders = ctx.addReferenceObj("sortOrder", sortOrder)
    partitionsIdx = ctx.references.length
    thisPartitions = ctx.addReferenceObj("partitions", partitions)
    sortedIdx = ctx.references.length
    thisSorted = ctx.addReferenceObj("sortedBatch", sortedBatch)

//    partitions = ctx.addMutableState("scala.collection.mutable.Buffer<ArrowColumnarBatchRow>", "partitions",
//      forceInline = true)

    val columns = ctx.addMutableState("org.apache.spark.sql.vectorized.ArrowColumnVector[]", "columns",
      forceInline = true)
    val batch = ctx.addMutableState(classOf[ArrowColumnarBatchRow].getName, "batch", forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
         | private void $addToSorter() throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
      """.stripMargin.trim)

//    val outputBatch = ctx.freshName("outputBatch")
//    val sortTime = metricTerm(ctx, "sortTime")

    val numRows = ctx.freshName("numRows")
    val reversedOrders = ctx.freshName("reversedOrders")
    val order = ctx.freshName("order")
    val col = ctx.freshName("col")

    val noneType = "scala.Option.apply(null)"
    val staticBatch = classOf[ArrowColumnarBatchRow].getName + "$.MODULE$"
    val bytesArray = ctx.freshName("bytesArray")
    val batchArray = ctx.freshName("batchArray")
    val staticIterator = classOf[Iterator[ArrowColumnarBatchRow]].getName + "$.MODULE$"

    val code =
      s"""
       | if ($needToSort) {
       |  $addToSorterFuncName();
       |
       |  $columns = $staticBatch.take($thisPartitions.toIterator(), $noneType, $noneType);
       |  int $numRows = ($columns.length > 0) ? $columns[0].getValueVector().getValueCount() : 0;
       |  $batch = new ${classOf[ArrowColumnarBatchRow].getName}($columns, $numRows);
       |
       |  if (((${classOf[Seq[SortOrder]].getName})$orders).length() == 1) {
       |    ${classOf[SortOrder].getName} $order = (${classOf[SortOrder].getName})(((${classOf[Seq[SortOrder]].getName})$orders).head());
       |    int $col = $thisPlan.attributeReferenceToCol($order);
       |    $batch = $staticBatch.sort($batch, $col, $order);
       |  } else {
       |    $batch = $staticBatch.multiColumnSort($batch, $orders);
       |  }
       |  references[$sortedIdx] = $batch;
       |
       |  $needToSort = false;
       |  ${consume(ctx, null, thisSorted)}
       | }
       |
       |
       |""".stripMargin
    code
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val temp = ctx.freshName("temp")

    // TODO: remove debug prints
    val code = s"""
       |System.out.println("aaaaa");
       |${row.code}
       |${classOf[util.List[ArrowColumnarBatchRow]].getName} $temp = scala.collection.JavaConverters.bufferAsJavaList($thisPartitions);
       |$temp.add((${classOf[ArrowColumnarBatchRow].getName})${row.value});
       |System.out.println(((${classOf[ArrowColumnarBatchRow].getName})${row.value}).length());
       |references[$partitionsIdx] = (${classOf[ArrayBuffer[ArrowColumnarBatchRow]].getName})scala.collection.JavaConverters.asScalaBuffer($temp);
     """.stripMargin
    code
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      val columns = ArrowColumnarBatchRow.take(iter.asInstanceOf[Iterator[ArrowColumnarBatchRow]])._2
      var batch = new ArrowColumnarBatchRow(columns, if (columns.length > 0) columns(0).getValueVector.getValueCount else 0)

      if (sortOrder.length == 1) {
        val col = attributeReferenceToCol(sortOrder.head)
        batch = ArrowColumnarBatchRow.sort(batch, col, sortOrder.head)
      } else {
        batch = ArrowColumnarBatchRow.multiColumnSort(batch, sortOrder)
      }

      Iterator(batch)
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowSortExec = copy(child = newChild)
}
