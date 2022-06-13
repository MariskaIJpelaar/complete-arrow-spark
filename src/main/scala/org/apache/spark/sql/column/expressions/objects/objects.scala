package org.apache.spark.sql.column.expressions.objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.column.TColumn
import org.apache.spark.sql.column.expressions.GenericColumn
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, ObjectType}

import scala.reflect.ClassTag

/** Constructs a new external column, using the result of evaluating the specified
 * expressions as content */
case class CreateExternalColumn[T](children: Seq[Expression])(implicit ct: ClassTag[T]) extends Expression with NonSQLExpression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val values = children.map(_.eval(input)).toArray.asInstanceOf[Array[T]]
    new GenericColumn[T](values)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val columnClass = classOf[GenericColumn[T]].getName
    val values = ctx.freshName("values")

    val childrenCodes = children.zipWithIndex.map { case (e, i) =>
      val eval = e.genCode(ctx)
      s"""
         |${eval.code}
         |if (${eval.isNull}) {
         |  $values[$i] = null;
         |} else {
         |  $values[$i] = ${eval.value};
         |}
       """.stripMargin
    }

    val childrenCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = childrenCodes,
      funcName = "createExternalColumn",
      extraArguments = "Object[]" -> values :: Nil)

    val code =
      code"""
            |Object[] $values = new Object[${children.size}];
            |$childrenCode
            |final ${classOf[TColumn].getName} ${ev.value} = new $columnClass($values);
       """.stripMargin

    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def dataType: DataType = ObjectType(classOf[TColumn])

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CreateExternalColumn[T] = copy(children = newChildren)
}


case class GetExternalColumn[T](child: Expression)(implicit ct: ClassTag[T]) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val inputColumn = child.eval(input).asInstanceOf[TColumn]
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

  override def dataType: DataType = ObjectType(ct.getClass)

  override protected def withNewChildInternal(newChild: Expression): GetExternalColumn[T] = copy(child = newChild)
}