package org.apache.spark.sql.catalyst.expressions
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowTransformers
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray}

/** A single reference to encapsulate multiple BoundReferences for a ArrowColumnarBatch */
case class ArrowBoundAttribute(expressions: Seq[Expression]) extends LeafExpression {
  override def toString: String = expressions.map( expression => expression.toString() )
    .mkString("ArrowBoundAttribute: {", ",", "}")

  override def nullable: Boolean = false

  /** Note: consumes input
   * Caller should close returned batch */
  override def eval(input: InternalRow): ArrowColumnarBatchRow = input match {
    case batch: ArrowColumnarBatchRow =>
      try {
        expressions match {
          case references: Seq[BoundReference] =>
            ArrowColumnarBatchRowTransformers.projection(batch, references.map(_.ordinal))
          case other =>
            val columns = Array.tabulate[ArrowColumnVector](other.length) { i =>
              other(i).eval(batch).asInstanceOf[ArrowColumnarArray].getData
            }
            if (columns.isEmpty) return ArrowColumnarBatchRow.empty
            val root = columns(0).getValueVector.getAllocator.getRoot
            ArrowColumnarBatchRow.create(createAllocator(root, "ArrowBoundAttribute::eval"), columns)
        }
      } finally {
        batch.close()
      }
    case _ => throw new RuntimeException("[ArrowBoundAttribute::eval] only ArrowColumnarBatches are supported")
  }

  /** Caller is responsible for closing ev.value */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    expressions match {
      case references: Seq[BoundReference] =>
        assert(ctx.INPUT_ROW != null)
        val projections = ctx.addReferenceObj(objName = "projections", obj = references.map(_.ordinal))
        val batchClass = classOf[ArrowColumnarBatchRow].getName
        val transformerClass = ArrowColumnarBatchRowTransformers.getClass.getName + ".MODULE$"
        val code = code"""
                         |  $batchClass ${ev.value} = $transformerClass.projection(($batchClass)${ctx.INPUT_ROW}, $projections);
                         |""".stripMargin
        ev.copy(code = code)
      case _ =>
        val array = ctx.freshName("array")
        val parents = ctx.freshName("parents")
        val columnType = classOf[ArrowColumnarArray].getName
        val bufferType = classOf[BufferAllocator].getName
        val vectorType = classOf[ArrowColumnVector].getName

        val exprEvals = ctx.generateExpressions(expressions, doSubexpressionElimination = SQLConf.get.subexpressionEliminationEnabled)
        val codes = exprEvals.zipWithIndex map { case(eval, index) =>
          val column = ctx.freshName("column")
          code"""
                | ${eval.code}
                | $columnType $column = (($columnType)(($columnType)(${eval.value})).copy());
                | $array[$index] = (($vectorType)$column.getData());
                | $parents[$index] = (($bufferType)$column.getParentAllocator());
                |""".stripMargin
        }

        val batchType = classOf[ArrowColumnarBatchRow].getName
        val staticBatch = ArrowColumnarBatchRow.getClass.getName + ".MODULE$"
        val numRows = s"$array[0].getValueVector().getValueCount()"
        val parent = s"$parents[0]"
        val ret = s"($array.length > 0) ? new $batchType($parent, $array, $numRows) : $staticBatch.empty()"
        val code = code"""
                         | $vectorType[] $array = new $vectorType[${codes.length}];
                         | $bufferType[] $parents = new $bufferType[${codes.length}];
                         | ${codes.map(_.code).mkString("\n")}
                         | $batchType ${ev.value} = $ret;
                         | boolean ${ev.isNull} = ($array.length == 0);
                         |""".stripMargin
        ev.copy(code = code)
    }
  }

  override def dataType: DataType = StructType( expressions.map(  expression => StructField(expression.prettyName, expression.dataType)))
}
