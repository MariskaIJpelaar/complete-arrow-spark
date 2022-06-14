package org.apache.spark.sql.column.expressions.objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.column.expressions.{GenericColumn, GenericColumnBatch}
import org.apache.spark.sql.column.{ColumnBatch, TColumn}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, ObjectType}

/** Constructs a new external column, using the result of evaluating the specified
 * expressions as content */
case class CreateExternalColumnBatch(children: Seq[Expression]) extends Expression with NonSQLExpression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val values = children.map(_.eval(input)).toArray.asInstanceOf[Array[TColumn]]
    new GenericColumnBatch(values)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val columnBatchClass = classOf[GenericColumnBatch].getName
    val columnClass = classOf[TColumn].getName
    val genericColumnClass = classOf[GenericColumn].getName
    val values = ctx.freshName("values")

//    new GenericColumn[T](values.toArray)
    val childrenCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      val ct: Class[_] =  eval.value.javaType
      s"""
         |${eval.code}
         |if (${eval.isNull}) {
         |  $values[$i] = null;
         |} else {
         |  $values[$i] = new $genericColumnClass(${eval.value});
         |}
       """.stripMargin
    }

    val childrenCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenCodes,
      funcName = "createExternalColumnBatch",
      extraArguments = "Object[]" -> values :: Nil)

    val code =
      code"""
            |$columnClass[] $values = new $columnClass[${children.size}];
            |$childrenCode
            |final ${classOf[ColumnBatch].getName} ${ev.value} = new $columnBatchClass($values);
       """.stripMargin

    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def dataType: DataType = ObjectType(classOf[TColumn])

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CreateExternalColumnBatch = copy(children = newChildren)
}


case class GetExternalColumnBatch(child: Expression) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val inputColumn = child.eval(input).asInstanceOf[ColumnBatch]
    if (inputColumn == null)
      throw QueryExecutionErrors.inputExternalRowCannotBeNullError() // well, pretend it is about Columns, I guess
    inputColumn
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val column = child.genCode(ctx)
    val code = code"""
      ${column.code}

      if (${column.isNull}) {
        throw QueryExecutionErrors.inputExternalRowCannotBeNullError();
      }

      final Object ${ev.value} = ${column.value}
     """
    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def dataType: DataType = ObjectType(TColumn.getClass)

  override protected def withNewChildInternal(newChild: Expression): GetExternalColumnBatch = copy(child = newChild)
}