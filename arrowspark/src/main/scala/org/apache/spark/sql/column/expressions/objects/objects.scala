package org.apache.spark.sql.column.expressions.objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.column.expressions.{GenericColumn, GenericColumnBatch}
import org.apache.spark.sql.column.{ColumnBatch, TColumn}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, ObjectType}

case class CreateExternalColumn(child: Expression) extends UnaryExpression with NonSQLExpression {
  // FIXME: better
  override def toString(): String = "CreateExternalColumn"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val values = children.map(_.eval(input)).toArray
    new GenericColumn(values)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val columnClass = classOf[TColumn].getName
    val genericColumnClass = classOf[GenericColumn].getName
    val values = ctx.freshName("values")

    val eval = child.genCode(ctx)
    val childCode =
      s"""
         |${eval.code}
       """.stripMargin

    val childrenCode = ctx.splitExpressionsWithCurrentInputs(
      expressions = childCode :: Nil,
      funcName = "createExternalColumn",
      extraArguments = "Object[]" -> values :: Nil)

    val tmp = ctx.freshName("tmp")

    val code =
      code"""
            |$childrenCode
            |$genericColumnClass $tmp;
            |if (${eval.isNull})
            |   $tmp = new $genericColumnClass();
            |else
            |   $tmp = new $genericColumnClass(${eval.value}, true);
            |final $columnClass ${ev.value} = $tmp;
       """.stripMargin

    ev.copy(code = code, isNull = FalseLiteral)
  }

  override def dataType: DataType = ObjectType(classOf[TColumn])

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

/** Constructs a new external column, using the result of evaluating the specified
 * expressions as content */
case class CreateExternalColumnBatch(children: Seq[Expression]) extends Expression with NonSQLExpression {
  // FIXME: better
  override def toString(): String = "CreateExternalColumnBatch"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val values = children.map(_.eval(input)).toArray.asInstanceOf[Array[TColumn]]
    new GenericColumnBatch(values)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val columnBatchClass = classOf[GenericColumnBatch].getName
    val columnClass = classOf[TColumn].getName
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

  override def dataType: DataType = ObjectType(classOf[ColumnBatch])

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): CreateExternalColumnBatch = copy(children = newChildren)
}

case class GetExternalColumn(index: Int, child: Expression, dataType: DataType) extends UnaryExpression with NonSQLExpression {
  override def nullable: Boolean = false

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)

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

  override def dataType: DataType = ObjectType(classOf[ColumnBatch])

  override protected def withNewChildInternal(newChild: Expression): GetExternalColumnBatch = copy(child = newChild)
}