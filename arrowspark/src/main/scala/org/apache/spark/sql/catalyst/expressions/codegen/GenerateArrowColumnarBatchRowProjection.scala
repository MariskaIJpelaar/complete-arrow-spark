package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.{ArrowBoundAttribute, Attribute, Expression, Projection}
import org.apache.spark.sql.column.ArrowColumnarBatchRow

/** Note: closes input
 * Caller should close generated projection */
object GenerateArrowColumnarBatchRowProjection extends CodeGenerator[Seq[Expression], Projection] {

  override protected def create(in: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val eval = ArrowBoundAttribute(in).genCode(ctx)

    val specificClass = "SpecificArrowColumnarBatchRowProjection"
    val batchClass = classOf[ArrowColumnarBatchRow].getName

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
         |    return apply(($batchClass) row);
         |  }
         |
         |  public $batchClass apply($batchClass ${ctx.INPUT_ROW}) {
         |    try {
         |     ${eval.code}
         |     return ($batchClass)${eval.value};
         |    } finally {
         |      (($batchClass) ${ctx.INPUT_ROW}).close();
         |    }
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

  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    create(bindReferences(exprs, inputSchema))
  }
}
