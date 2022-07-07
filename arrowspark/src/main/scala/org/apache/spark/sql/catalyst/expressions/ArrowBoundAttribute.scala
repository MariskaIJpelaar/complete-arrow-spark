package org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowColumnarArray}

/** A single reference to encapsulate multiple BoundReferences for a ArrowColumnarBatch */
case class ArrowBoundAttribute(expressions: Seq[Expression]) extends LeafExpression {
  override def toString: String = expressions.map( expression => expression.toString() )
    .mkString("ArrowBoundAttribute: {", ",", "}")

  override def nullable: Boolean = false

  /** Note: closes input
   * TODO: Caller should close returned batch */
  override def eval(input: InternalRow): ArrowColumnarBatchRow = input match {
    case batch: ArrowColumnarBatchRow =>
      try {
        expressions match {
          case references: Seq[BoundReference] => batch.projection(references.map(_.ordinal))
          case _ =>
            val columns = Array.tabulate[ArrowColumnVector](expressions.length) { i =>
              expressions(i).eval(input).asInstanceOf[ArrowColumnarArray].getData
            }
            ArrowColumnarBatchRow.create(columns)
        }
      } finally {
        batch.close()
      }
    case _ => throw new RuntimeException("[ArrowBoundAttribute::eval] only ArrowColumnarBatches are supported")
  }

  /** Note: closes input
   * TODO: Caller is responsible for closing ev.value */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    expressions match {
      case references: Seq[BoundReference] =>
        assert(ctx.INPUT_ROW != null)
        val projections = ctx.addReferenceObj(objName = "projections", obj = references.map(_.ordinal))
        val code = code"""
                         | ${classOf[ArrowColumnarBatchRow].getName} ${ev.value} = ((${classOf[ArrowColumnarBatchRow].getName}) ${ctx.INPUT_ROW}).projection($projections);
                         | ((${classOf[ArrowColumnarBatchRow].getName}) ${ctx.INPUT_ROW}).close();
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
                | $array[$index] = ($vectorType)((($columnType)(${eval.value}));
                |""".stripMargin
        }

        val arrayType = classOf[Array[ArrowColumnarArray]].getName
        val batchType = classOf[Array[ArrowColumnarBatchRow]].getName
        val numRows = s"($array.length > 0) ? $array[0].getValueVector().getValueCount() : 0;"
        val code = code"""
                         | $arrayType $array = new $arrayType(${codes.length})
                         | ${codes.map(_.code).mkString("\n")}
                         | ${ev.value} = new $batchType($array, $numRows)
                         |""".stripMargin
        ev.copy(code = code)
    }
  }

  override def dataType: DataType = StructType( expressions.map(  expression => StructField(expression.prettyName, expression.dataType)))
}
