package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Projection, UnsafeRow}
import org.apache.spark.sql.types.{DataType, DayTimeIntervalType, Decimal, DecimalType, UserDefinedType, YearMonthIntervalType}

import scala.annotation.tailrec

object GenerateArrowColumnarBatchRowProjection extends CodeGenerator[Seq[Expression], Projection] {

  case class Schema(dataType: DataType, nullable: Boolean)

  @tailrec
  private def isFixedLength(dt: DataType): Boolean = dt match {
    case _: UserDefinedType[_] => isFixedLength(dt.asInstanceOf[UserDefinedType[_]].sqlType)
    case _: DecimalType => dt.asInstanceOf[DecimalType].precision <= Decimal.MAX_LONG_DIGITS
    case _ => dt.isInstanceOf[DayTimeIntervalType] || dt.isInstanceOf[YearMonthIntervalType] || UnsafeRow.mutableFieldTypes.contains(dt)
  }

  def createCode(ctx: CodegenContext, expressions: Seq[Expression]): ExprCode = {
    val exprEvals = ctx.generateExpressions(expressions, doSubexpressionElimination = false)
    val exprSchemas = expressions.map( e => Schema(e.dataType, e.nullable))
    val numVarLenFields = exprSchemas.count { case Schema(dt, _) => isFixedLength(dt) }
    // TODO: implement
  }

  override protected def create(in: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val eval = createCode(ctx, in)

    val specificClass = "SpecificArrowColumnarBatchRowProjection"

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificArrowColumnarBatchRowProjection(references);
         |}
         |
         |class $specificClass  extends ${classOf[Projection].getName} {
         |  private Object[] references;
         |  ${ctx.declareMutableStates()}
         |
         |  public $specificClass(Object[] references) {
         |    this.references = references;
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  public void initialize(int partitionIndex) {
         |    ${ctx.initPartition()}
         |  }
         |
         |  // Scala.Function1 needs this
         |  public java.lang.Object apply(java.lang.Object row) {
         |    return apply((ArrowColumnarBatchRow) row);
         |  }
         |
         |  public ArrowColumnarBatchRow apply(ArrowColumnarBatchRow ${ctx.INPUT_ROW}) {
         |    ${eval.code}
         |    return ${eval.value};
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
         |""".stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${in.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[Projection]
  }

  override protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  override protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)
}
