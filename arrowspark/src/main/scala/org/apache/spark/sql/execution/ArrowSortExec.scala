package org.apache.spark.sql.execution

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.sort.PublicOffHeapIntStack
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.column
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowTransformers}
import org.apache.spark.sql.column.utils.algorithms.ArrowColumnarBatchRowSorters
import org.apache.spark.sql.rdd.ArrowRDD
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.vectorized.ArrowColumnarArray

import java.util.concurrent.atomic.AtomicLong

object ArrowSortExec {
  var totalTime: AtomicLong = new AtomicLong(0)
  def incrementTotalTime(inc: Long): Unit = totalTime.addAndGet(inc)
}

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
    val indices = ctx.freshName("indices")
    val index = ctx.freshName("index")

    val staticBatch = classOf[ArrowColumnarBatchRow].getName + "$.MODULE$"
    val staticSorter = ArrowColumnarBatchRowSorters.getClass.getName + ".MODULE$"
    val builderClass = classOf[ArrowColumnarBatchRowBuilder].getName
    val allocatorClass = classOf[RootAllocator].getName
    val staticManager = column.AllocationManager.getClass.getName + ".MODULE$"
    val indicesClass = classOf[IntVector].getName
    val staticTransformers = ArrowColumnarBatchRowTransformers.getClass.getName + ".MODULE$"
    val staticSortExec = ArrowSortExec.getClass.getName + ".MODULE$"

    // TODO: perhaps do https://en.wikipedia.org/wiki/Timsort ?
    // https://www.geeksforgeeks.org/timsort/
    // spark uses it as well, source: https://databricks.com/blog/2014/10/10/spark-petabyte-sort.html
    val quickSort = getQuickSortFunc(ctx)
    val insertionSort = getInsertionSortFunc(ctx)
    val timSort = getTimSort(ctx)

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
       |  long t1 = System.nanoTime();
       |  $indicesClass $indices = new $indicesClass("indices", $staticManager.createAllocator($root, "IndicesAllocator"));
       |  try {
       |    $indices.setInitialCapacity($batch.length());
       |    $indices.allocateNew();
       |    for (int $index = 0; $index < $batch.length(); ++$index)
       |      $indices.set($index, $index);
       |    $indices.setValueCount($batch.length());
       |    $timSort($batch, $indices);
       |
       |    $newBatch = $staticTransformers.applyIndices($batch, $indices);
       |
       |  } finally {
       |    try {
       |      $indices.close();
       |      long t2 = System.nanoTime();
       |      ${staticSortExec}.incrementTotalTime(t2-t1);
       |    } catch (Exception e) {
       |      e.printStackTrace();
       |    }
       |  }
       |
       |
       |  $needToSort = false;
       |  ${consume(ctx, null, newBatch)}
       | }
       |
       |
       |""".stripMargin
    code
  }

  /** Old:
  long t3 = System.nanoTime();
  if (((${classOf[Seq[SortOrder]].getName})$orders).length() == 1) {
    ${classOf[SortOrder].getName} $order = (${classOf[SortOrder].getName})(((${classOf[Seq[SortOrder]].getName})$orders).head());
    int $col = $thisPlan.attributeReferenceToCol($order);
    $newBatch = $staticSorter.sort($batch, $col, $order);
  } else {
    $newBatch = $staticSorter.multiColumnSort($batch, $orders);
  }
  long t4 = System.nanoTime();
  double time2 = (t4 - t3) / 1e9d;
  System.out.println(String.format("ArrowSortExec::arrowSort: %04.3f", time2));
   * */


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

  var insertionSortFunc: Option[String] = None
  private def getInsertionSortFunc(ctx: CodegenContext): String = {
    insertionSortFunc.getOrElse {
      val insertionSort = ctx.freshName("insertionSort")
      val func = ctx.addNewFunction(insertionSort, produceInsertionSort(ctx, insertionSort))
      insertionSortFunc = Option(func)
      func
    }
  }

  /** from: org.apache.arrow.algorithm.sort.InsertionSorter */
  private def produceInsertionSort(ctx: CodegenContext, funcName: String): String = {
    // parameters
    val indices = ctx.freshName("indices")
    val batch = ctx.freshName("batch")
    val head = ctx.freshName("head")
    val last = ctx.freshName("last")

    // types
    val indicesClass = classOf[IntVector].getName
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    // local variables
    val index = ctx.freshName("index")
    val key = ctx.freshName("key")
    val index2 = ctx.freshName("index")

    // function names
    val compare = getCompareFunc(ctx)

    val code =
      s"""
         | private void $funcName($batchClass $batch, $indicesClass $indices, int $head, int $last) {
         |  for (int $index = $head+1; $index <= $last; ++$index) {
         |    int $key = $indices.get($index);
         |    int $index2 = $index - 1;
         |    while ($index2 >= $head && $compare($batch, $indices.get($index2), $key) > 0) {
         |      $indices.set($index2 + 1, $indices.get($index2));
         |      $index2 = $index2 -1;
         |    }
         |    $indices.set($index2 + 1, $key);
         |  }
         | }
         |""".stripMargin
    code
  }

  var mergeFunc: Option[String] = None
  private def getMergeFunc(ctx: CodegenContext): String = {
    mergeFunc.getOrElse {
      val merge = ctx.freshName("merge")
      val func = ctx.addNewFunction(merge, produceMerge(ctx, merge))
      mergeFunc = Option(func)
      func
    }
  }

  private def produceMerge(ctx: CodegenContext, funcName: String): String = {
    // parameters
    val indices = ctx.freshName("indices")
    val batch = ctx.freshName("batch")
    val head = ctx.freshName("head")
    val mid = ctx.freshName("mid")
    val last = ctx.freshName("last")

    // types
    val indicesClass = classOf[IntVector].getName
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    // local variables
    val leftLen = ctx.freshName("leftLen")
    val rightLen = ctx.freshName("rightLen")
    val leftIndices = ctx.freshName("leftIndices")
    val rightIndices = ctx.freshName("rightIndices")
    val leftIndex = ctx.freshName("leftIndex")
    val rightIndex = ctx.freshName("rightIndex")
    val index = ctx.freshName("index")
    val i = ctx.freshName("i")

    // function names
    val compare = getCompareFunc(ctx)

    val code =
      s"""
         | private void $funcName($batchClass $batch, $indicesClass $indices, int $head, int $mid, int $last) {
         |  int $leftLen = $mid - $head + 1;
         |  int $rightLen = $last - $mid;
         |
         |  $indicesClass $leftIndices = new $indicesClass("leftIndices", $indices.getAllocator());
         |  $indicesClass $rightIndices = new $indicesClass("rightIndices", $indices.getAllocator());
         |  try {
         |    $leftIndices.setInitialCapacity($leftLen);
         |    $leftIndices.allocateNew();
         |    $rightIndices.setInitialCapacity($rightLen);
         |    $rightIndices.allocateNew();
         |    for (int $i = 0; $i < $leftLen; ++$i)
         |      $leftIndices.set($i, $indices.get($head + $i));
         |    for (int $i = 0; $i < $rightLen; ++$i)
         |      $rightIndices.set($i, $indices.get($mid + 1 + $i));
         |    $leftIndices.setValueCount($leftLen);
         |    $rightIndices.setValueCount($rightLen);
         |
         |    int $leftIndex = 0;
         |    int $rightIndex = 0;
         |    int $index = $head;
         |
         |    while ($leftIndex < $leftLen && $rightIndex < $rightLen) {
         |      if ($compare($batch, $leftIndices.get($leftIndex), $rightIndices.get($rightIndex)) <= 0) {
         |        $indices.set($index, $leftIndices.get($leftIndex));
         |        ++$leftIndex;
         |      } else {
         |        $indices.set($index, $rightIndices.get($rightIndex));
         |        ++$rightIndex;
         |      }
         |      ++$index;
         |    }
         |
         |    while ($leftIndex < $leftLen) {
         |      $indices.set($index, $leftIndices.get($leftIndex));
         |      ++$leftIndex;
         |      ++$index;
         |    }
         |
         |    while ($rightIndex < $rightLen) {
         |      $indices.set($index, $rightIndices.get($rightIndex));
         |      ++$rightIndex;
         |      ++$index;
         |    }
         |  } finally {
         |    try {
         |      $leftIndices.close();
         |      $rightIndices.close();
         |    } catch (Exception e) {
         |      e.printStackTrace();
         |    }
         |  }
         | }
         |""".stripMargin
    code
  }

  var timSortFunc: Option[String] = None
  private def getTimSort(ctx: CodegenContext): String = {
    timSortFunc.getOrElse {
      val timSort = ctx.freshName("timSort")
      val func = ctx.addNewFunction(timSort, produceTimSort(ctx, timSort))
      timSortFunc = Option(func)
      func
    }
  }

  private def produceTimSort(ctx: CodegenContext, funcName: String): String = {
    // parameters
    val indices = ctx.freshName("indices")
    val batch = ctx.freshName("batch")

    // types
    val indicesClass = classOf[IntVector].getName
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    // local variables
    val i = ctx.freshName("i")
    val size = ctx.freshName("size")
    val head = ctx.freshName("head")
    val mid = ctx.freshName("mid")
    val last = ctx.freshName("last")

    // 'constants'
    val RUN = 32

    // Function names
    val insertionSort = getInsertionSortFunc(ctx)
    val merge = getMergeFunc(ctx)

    val code =
      s"""
         | private void $funcName($batchClass $batch, $indicesClass $indices) {
         |    for (int $i = 0; $i < $batch.length(); $i += $RUN) {
         |      $insertionSort($batch, $indices, $i, Math.min($i+$RUN-1, $batch.length()-1));
         |    }
         |
         |    for (int $size = $RUN; $size < $batch.length(); $size = 2*$size) {
         |      for (int $head = 0; $head < $batch.length(); $head += 2*$size) {
         |        int $mid = $head + $size - 1;
         |        int $last = Math.min($head + 2*$size - 1, $batch.length() - 1);
         |
         |        if ($mid < $last)
         |          $merge($batch, $indices, $head, $mid, $last);
         |      }
         |    }
         | }
         |""".stripMargin
    code
  }



  var quickSortFunc: Option[String] = None
  private def getQuickSortFunc(ctx: CodegenContext): String = {
    quickSortFunc.getOrElse {
      val quickSort = ctx.freshName("quickSort")
      val func = ctx.addNewFunction(quickSort, produceQuickSort(ctx, quickSort))
      quickSortFunc = Option(func)
      func
    }
  }


  /** modified from: org.apache.arrow.algorithm.sort.IndexSorter */
  private def produceQuickSort(ctx: CodegenContext, funcName: String): String = {
    // local variables
    val rangeStack = ctx.freshName("rangeStack")
    val high = ctx.freshName("high")
    val low = ctx.freshName("low")
    val mid = ctx.freshName("mid")

    // parameters
    val indices = ctx.freshName("indices")
    val batch = ctx.freshName("batch")

    // types
    val indicesClass = classOf[IntVector].getName
    val batchClass = classOf[ArrowColumnarBatchRow].getName
    // Arrow does not give us access :(
    val stackClass = classOf[PublicOffHeapIntStack].getName

    // function names
    val partition = getPartitionFunc(ctx)

    val code =
      s"""
         | private void $funcName($batchClass $batch, $indicesClass $indices) {
         |    $stackClass $rangeStack = new $stackClass($indices.getAllocator());
         |    try {
         |      $rangeStack.push(0);
         |      $rangeStack.push($indices.getValueCount() - 1);
         |
         |      while (!$rangeStack.isEmpty()) {
         |        int $high = $rangeStack.pop();
         |        int $low = $rangeStack.pop();
         |
         |        if ($low < $high) {
         |          int $mid = $partition($batch, $indices, $low, $high);
         |
         |          if ($high - $mid < $mid - $low) {
         |            $rangeStack.push($low);
         |            $rangeStack.push($mid -1);
         |
         |            $rangeStack.push($mid +1);
         |            $rangeStack.push($high);
         |          } else {
         |            $rangeStack.push($mid +1);
         |            $rangeStack.push($high);
         |
         |            $rangeStack.push($low);
         |            $rangeStack.push($mid -1);
         |          }
         |        }
         |      }
         |    } finally {
         |      try {
         |        $rangeStack.close();
         |      } catch(Exception e) {
         |        e.printStackTrace();
         |      }
         |
         |    }
         | }
         |""".stripMargin

    code
  }

  var partitionFunc: Option[String] = None
  private def getPartitionFunc(ctx: CodegenContext): String = {
    partitionFunc.getOrElse {
      val partition = ctx.freshName("partition")
      val func = ctx.addNewFunction(partition, producePartition(ctx, partition))
      partitionFunc = Option(func)
      func
    }
  }

  private def producePartition(ctx: CodegenContext, funcName: String): String = {
    // parameters
    val low = ctx.freshName("low")
    val high = ctx.freshName("high")
    val indices = ctx.freshName("indices")
    val indicesClass = classOf[IntVector].getName
    val batch = ctx.freshName("batch")
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    // function names
    val compare = getCompareFunc(ctx)
    val choosePivot = getChoosePivotFunc(ctx)

    // local variables
    val pivotIndex = ctx.freshName("pivotIndex")

    val code =
      s"""
         | private int $funcName($batchClass $batch, $indicesClass $indices, int $low, int $high) {
         |    int $pivotIndex = $choosePivot($batch, $indices, $low, $high);
         |
         |    while ($low < $high) {
         |      while ($low < $high && $compare($batch, $indices.get($high), $pivotIndex) >= 0) {
         |        $high -= 1;
         |      }
         |      $indices.set($low, $indices.get($high));
         |
         |      while ($low < $high && $compare($batch, $indices.get($low), $pivotIndex) <= 0) {
         |        $low += 1;
         |      }
         |      $indices.set($high, $indices.get($low));
         |    }
         |
         |    $indices.set($low, $pivotIndex);
         |    return $low;
         | }
         |""".stripMargin
    code
  }

  var choosePivotFunc: Option[String] = None
  private def getChoosePivotFunc(ctx: CodegenContext): String = {
    choosePivotFunc.getOrElse {
      val choosePivot = ctx.freshName("choosePivot")
      val func = ctx.addNewFunction(choosePivot, produceChoosePivot(ctx, choosePivot))
      choosePivotFunc = Option(func)
      func
    }
  }

  private def produceChoosePivot(ctx: CodegenContext, funcName: String): String = {
    // local vars
    val low = ctx.freshName("low")
    val high = ctx.freshName("high")
    val mid = ctx.freshName("mid")
    val medianIdx = ctx.freshName("medianIdx")
    val tmp = ctx.freshName("tmp")
    val midHigh = ctx.freshName("midHigh")
    val lowHigh = ctx.freshName("lowHigh")

    // 'constant'
    val stopChoosingPivotThreshold = 3;

    // parameters
    val indices = ctx.freshName("indices")
    val indicesClass = classOf[IntVector].getName
    val batch = ctx.freshName("batch")
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    // function names
    val compare = getCompareFunc(ctx)

    val code =
      s"""
         | private int $funcName($batchClass $batch, $indicesClass $indices, int $low, int $high) {
         |  if ($high - $low + 1 < ${stopChoosingPivotThreshold}) {
         |    return $indices.get($low);
         |  }
         |
         |  int $mid = $low + ($high - $low) / 2;
         |
         |  int $midHigh = $compare($batch, $indices.get($mid), $indices.get($high));
         |  int $lowHigh = $compare($batch, $indices.get($low), $indices.get($high));
         |
         |  int $medianIdx;
         |  if ($compare($batch, $indices.get($low), $indices.get($mid)) < 0) {
         |    if ($midHigh < 0) {
         |      $medianIdx = $mid;
         |    } else {
         |      $medianIdx = ($lowHigh < 0) ? $high : $low;
         |    }
         |  } else {
         |    if ($midHigh > 0) {
         |      $medianIdx = $mid;
         |    } else {
         |      $medianIdx = ($lowHigh < 0) ? $low : $high;
         |    }
         |  }
         |
         |  if ($medianIdx == $low) return $indices.get($low);
         |  int $tmp = $indices.get($medianIdx);
         |  $indices.set($medianIdx, $indices.get($low));
         |  $indices.set($low, $tmp);
         |  return $tmp;
         | }
         |""".stripMargin
    code
  }

  var compareFunc: Option[String] = None
  private def getCompareFunc(ctx: CodegenContext): String = {
    compareFunc.getOrElse {
      val compare = ctx.freshName("compare")
      val func = ctx.addNewFunction(compare, produceCompare(ctx, compare))
      compareFunc = Option(func)
      func
    }
  }

  private def produceCompare(ctx: CodegenContext, funcName: String): String = {
    val index1 = ctx.freshName("index")
    val index2 = ctx.freshName("index")

    val batch = ctx.freshName("batch")
    val batchClass = classOf[ArrowColumnarBatchRow].getName

    val comparators = sortOrder.map { order =>
      val col = attributeReferenceToCol(order)
      val arrowColumnarArray = ctx.freshName("arrowColumnarArray")
      val arrowColumnarArrayType = classOf[ArrowColumnarArray].getName
      var compString = s"$arrowColumnarArrayType $arrowColumnarArray = ($arrowColumnarArrayType)($batch.getArray($col));\n"

      if (order.nullable) {
        val isNull1 = ctx.freshName("isNull")
        val isNull2 = ctx.freshName("isNull")
        compString +=
          s"""
             | boolean $isNull1 = $arrowColumnarArray.isNullAt($index1);
             | boolean $isNull2 = $arrowColumnarArray.isNullAt($index2);
             | if ($isNull1 && $isNull2) return 0;
             | else if ($isNull1) return ${if (order.nullOrdering == NullsFirst) "-1" else "1"};
             | else if ($isNull2) return ${if (order.nullOrdering == NullsFirst) "1" else "-1"};
             |""".stripMargin
      }

      val dt = schema.fields(col).dataType.asInstanceOf[ArrayType].elementType
      val comp = ctx.freshName("comp")
      val compare: String = dt match {
        case _: IntegerType => s"int $comp = $arrowColumnarArray.getInt($index1) - $arrowColumnarArray.getInt($index2);"
        case other => throw new RuntimeException(s"[ArrowSortExec] ${other.typeName} is not supported (yet)")
      }

      compString += compare
      val returnValue = if (order.isAscending) s"$comp" else s"-$comp"
      compString +=
        s"""
           | if ($comp != 0)
           |  return $returnValue;
           |""".stripMargin

      compString
    }

    val code =
      s"""
         | private int $funcName($batchClass $batch, int $index1, int $index2) {
         |    ${comparators.mkString("\n")}
         |    return 0;
         | }
         |""".stripMargin
    code
  }


}
