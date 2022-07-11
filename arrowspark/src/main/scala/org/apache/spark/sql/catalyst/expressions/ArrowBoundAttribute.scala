package org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, createAllocator}
import org.apache.spark.sql.column.utils.ArrowColumnarBatchRowTransformers
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray}

/** A single reference to encapsulate multiple BoundReferences for a ArrowColumnarBatch */
case class ArrowBoundAttribute(expressions: Seq[Expression]) extends LeafExpression {
  override def toString: String = expressions.map( expression => expression.toString() )
    .mkString("ArrowBoundAttribute: {", ",", "}")

  override def nullable: Boolean = false

  /** Wrapper for pattern matching */
  private case class BoundReferenceSeq(value: Seq[BoundReference])

  /** Note: consumes input
   * Caller should close returned batch */
  override def eval(input: InternalRow): ArrowColumnarBatchRow = input match {
    case batch: ArrowColumnarBatchRow =>
      try {
        expressions match {
          case references: BoundReferenceSeq =>
            ArrowColumnarBatchRowTransformers.projection(batch, references.value.map(_.ordinal))
          case other =>
            val columns = Array.tabulate[ArrowColumnVector](other.length) { i =>
              other(i).eval(batch).asInstanceOf[ArrowColumnarArray].getData
            }
            ArrowColumnarBatchRow.create(createAllocator("ArrowBoundAttribute::eval"), columns)
        }
      } finally {
        batch.close()
      }
    case _ => throw new RuntimeException("[ArrowBoundAttribute::eval] only ArrowColumnarBatches are supported")
  }

  /** Note: closes input
   * Caller is responsible for closing ev.value */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    expressions match {
      case references: BoundReferenceSeq =>
        assert(ctx.INPUT_ROW != null)
        val projections = ctx.addReferenceObj(objName = "projections", obj = references.value.map(_.ordinal))
        val batchClass = classOf[ArrowColumnarBatchRow].getName
        val transformerClass = ArrowColumnarBatchRowTransformers.getClass.getName + "$.MODULE$"
        val code = code"""
                         | try {
                         |  $batchClass ${ev.value} = $transformerClass.projection(($batchClass)${ctx.INPUT_ROW}, $projections);
                         | } finally {
                         |  (($batchClass)${ctx.INPUT_ROW}).close();
                         | }
                         |""".stripMargin
        ev.copy(code = code)
      case _ =>
        val array = ctx.freshName("array")
        val columnType = classOf[ArrowColumnarArray].getName
        val vectorType = classOf[ArrowColumnVector].getName

        val exprEvals = ctx.generateExpressions(expressions, doSubexpressionElimination = SQLConf.get.subexpressionEliminationEnabled)
        val codes = exprEvals.zipWithIndex map { case(eval, index) =>
          code"""
                | ${eval.code}
                | $array[$index] = (($vectorType)(($columnType)(($columnType)(${eval.value})).copy()).getData());
                |""".stripMargin
        }

        val batchType = classOf[ArrowColumnarBatchRow].getName
        val numRows = s"($array.length > 0) ? $array[0].getValueVector().getValueCount() : 0"
        val code = code"""
                         | $vectorType[] $array = new $vectorType[${codes.length}];
                         | ${codes.map(_.code).mkString("\n")}
                         | $batchType ${ev.value} = new $batchType($array, $numRows);
                         |""".stripMargin
        ev.copy(code = code)
    }
  }

  override def dataType: DataType = StructType( expressions.map(  expression => StructField(expression.prettyName, expression.dataType)))
}
