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
  private val sortedBatch: ArrowColumnarBatchRow = new ArrowColumnarBatchRow(Array.empty[ArrowColumnVector], 0)
  private var thisSorted: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // initialize class member variables
    val thisPlan = ctx.addReferenceObj("plan", this)
    val orders = ctx.addReferenceObj("sortOrder", sortOrder)
    partitionsIdx = ctx.references.length
    thisPartitions = ctx.addReferenceObj("partitions", partitions)
    sortedIdx = ctx.references.length
    thisSorted = ctx.addReferenceObj("sortedBatch", sortedBatch)

    val batch = ctx.addMutableState(classOf[ArrowColumnarBatchRow].getName, "batch", forceInline = true)

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
         | private void $addToSorter() throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
      """.stripMargin.trim)

    val order = ctx.freshName("order")
    val col = ctx.freshName("col")

    val staticBatch = classOf[ArrowColumnarBatchRow].getName + "$.MODULE$"

    val code =
      s"""
       | if ($needToSort) {
       |  $addToSorterFuncName();
       |
       |  $batch = $staticBatch.create($thisPartitions.toIterator());
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

    val code = s"""
       |${row.code}
       |${classOf[util.List[ArrowColumnarBatchRow]].getName} $temp = scala.collection.JavaConverters.bufferAsJavaList($thisPartitions);
       |$temp.add((${classOf[ArrowColumnarBatchRow].getName})${row.value});
       |references[$partitionsIdx] = (${classOf[ArrayBuffer[ArrowColumnarBatchRow]].getName})scala.collection.JavaConverters.asScalaBuffer($temp);
     """.stripMargin
    code
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { iter =>
      var batch = ArrowColumnarBatchRow.create(iter.asInstanceOf[Iterator[ArrowColumnarBatchRow]])
      val new_batch: ArrowColumnarBatchRow = {
        if (sortOrder.length == 1) {
          val col = attributeReferenceToCol(sortOrder.head)
          ArrowColumnarBatchRow.sort(batch, col, sortOrder.head)
        } else {
          ArrowColumnarBatchRow.multiColumnSort(batch, sortOrder)
        }
      }
      batch.close()


      Iterator(new_batch)
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): ArrowSortExec = copy(child = newChild)
}
