package org.apache.spark.sql.execution

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.memory.RootAllocator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder}
import org.apache.spark.sql.column
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowBuilder
import org.apache.spark.sql.column.utils.algorithms.ArrowColumnarBatchRowSorters
import org.apache.spark.sql.rdd.ArrowRDD

/** copied and adapted from org.apache.spark.sql.execution.SortExec
 * Caller is responsible for closing returned batches from this plan */
case class ArrowSortExec(sortOrder: Seq[SortOrder], global: Boolean, child: SparkPlan)
  extends UnaryExecNode with BlockingOperatorWithCodegen {
  override def inputRDDs(): Seq[RDD[InternalRow]] = child.asInstanceOf[CodegenSupport].inputRDDs()


  /** Needs to be called from codegen */
  def attributeReferenceToCol(order: SortOrder): Int = {
    child.output.indexWhere(relRef => relRef.name.equals(order.child.asInstanceOf[AttributeReference].name))
  }

  // FIXME: For now, we assume that we won't return too early, and that partitions will be consumed by create
  private val partitions: Option[ArrowColumnarBatchRowBuilder] = Option.empty
  private var partitionsIdx: Int = _
  private var thisPartitions: String = _
  // FIXME: For now, we assume that we won't return too early, and that sortedBatch will be consumed by caller
//  private var sortedIdx: Int = _
//  private val sortedBatch: ArrowColumnarBatchRow = ArrowColumnarBatchRow.empty()
//  private var thisSorted: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val needToSort =
      ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "needToSort", v => s"$v = true;")

    // initialize class member variables
    val thisPlan = ctx.addReferenceObj("plan", this)
    val orders = ctx.addReferenceObj("sortOrder", sortOrder)
    partitionsIdx = ctx.references.length
    thisPartitions = ctx.addReferenceObj("partitions", partitions)
//    sortedIdx = ctx.references.length
//    thisSorted = ctx.addReferenceObj("sortedBatch", sortedBatch)

    val batch = ctx.addMutableState(classOf[ArrowColumnarBatchRow].getName, "batch", forceInline = true)
    val newBatch = ctx.freshName("newBatch")

    val addToSorter = ctx.freshName("addToSorter")
    val addToSorterFuncName = ctx.addNewFunction(addToSorter,
      s"""
         | private void $addToSorter() throws java.io.IOException {
         |   ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
         | }
      """.stripMargin.trim)

    val order = ctx.freshName("order")
    val col = ctx.freshName("col")
    val root = ctx.freshName("root")
    val builder = ctx.freshName("builder")

    val staticBatch = classOf[ArrowColumnarBatchRow].getName + "$.MODULE$"
    val staticSorter = ArrowColumnarBatchRowSorters.getClass.getName + ".MODULE$"
    val builderClass = classOf[ArrowColumnarBatchRowBuilder].getName
    val allocatorClass = classOf[RootAllocator].getName
    val staticManager = column.AllocationManager.getClass.getName + ".MODULE$"

    val code =
      s"""
       | if ($needToSort) {
       |  $addToSorterFuncName();
       |
       |  $builderClass $builder = (($builderClass)((scala.Option<$builderClass>)references[$partitionsIdx]).get());
       |  $allocatorClass $root = $builder.getRootAllocator();
       |  $batch = $builder.build($staticManager.createAllocator($root, "CompiledSort"));
       |  $builder.close();
       |  ${classOf[ArrowColumnarBatchRow].getName} $newBatch = null;
       |
       |  if (((${classOf[Seq[SortOrder]].getName})$orders).length() == 1) {
       |    ${classOf[SortOrder].getName} $order = (${classOf[SortOrder].getName})(((${classOf[Seq[SortOrder]].getName})$orders).head());
       |    int $col = $thisPlan.attributeReferenceToCol($order);
       |    $newBatch = $staticSorter.sort($batch, $col, $order);
       |  } else {
       |    $newBatch = $staticSorter.multiColumnSort($batch, $orders);
       |  }
       |
       |  $needToSort = false;
       |  ${consume(ctx, null, newBatch)}
       | }
       |
       |
       |""".stripMargin
    code
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val temp = ctx.freshName("temp")
    val builderClass = classOf[ArrowColumnarBatchRowBuilder].getName
    val batchClass = classOf[ArrowColumnarBatchRow].getName
    val builder = ctx.freshName("builder")

    val code = s"""
       |${row.code}
       |
       |scala.Option<$builderClass> $builder = ((scala.Option<$builderClass>)references[$partitionsIdx]);
       |if ($builder.isDefined()) {
       |  (($builderClass)($builder.get())).append(($batchClass)${row.value});
       |} else {
       |  references[$partitionsIdx] = scala.Option.apply(new $builderClass(($batchClass)${row.value}, scala.Option.empty(), scala.Option.empty()));
       |}
     """.stripMargin
    code
  }

  // caller is responsible for cleaning ArrowColumnarBatchRows
  override protected def doExecute(): RDD[InternalRow] = {
    ArrowRDD.mapPartitionsInternal(child.execute(), {
      (root: RootAllocator, item: Iterator[InternalRow]) => item match {
        case batchIter: Iterator[ArrowColumnarBatchRow] =>
          Resources.autoCloseTryGet(ArrowColumnarBatchRow.create(root, batchIter)) { batch =>
            val newBatch: ArrowColumnarBatchRow = {
              if (sortOrder.length == 1) {
                val col = attributeReferenceToCol(sortOrder.head)
                ArrowColumnarBatchRowSorters.sort(batch, col, sortOrder.head)
              } else {
                ArrowColumnarBatchRowSorters.multiColumnSort(batch, sortOrder)
              }
            }
            Iterator(newBatch)
          }
      }
    }).asInstanceOf[RDD[InternalRow]]
  }

  override def output: Seq[Attribute] = child.output

  // Caller should close
  override protected def withNewChildInternal(newChild: SparkPlan): ArrowSortExec = copy(child = newChild)
}
