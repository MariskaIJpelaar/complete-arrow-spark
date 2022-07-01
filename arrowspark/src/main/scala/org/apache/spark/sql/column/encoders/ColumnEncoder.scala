package org.apache.spark.sql.column.encoders

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CreateArray, Expression}
import org.apache.spark.sql.column.expressions.objects.{CreateExternalColumn, CreateExternalColumnBatch, GetExternalColumn, GetExternalColumnBatch}
import org.apache.spark.sql.column.{ColumnBatch, TColumn}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import scala.annotation.tailrec
import scala.reflect.ClassTag

object ColumnEncoder {
  def apply(schema: StructType): ExpressionEncoder[ColumnBatch] = {
    val cls = classOf[ColumnBatch]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val serializer = serializerFor(schema, inputObject)
    val deserializer = deserializerFor(schema)
    new ExpressionEncoder[ColumnBatch](serializer, deserializer, ClassTag(cls))
  }

  private def serializerFor(schema: StructType, inputObject: Expression): Expression = {
    val batch = GetExternalColumnBatch(inputObject)
    val fields = schema.fields.zipWithIndex.map { case (field, index) =>
      GetExternalColumn(index, batch, field.dataType)
    }
    CreateArray(fields)
  }

  /** Note below methods are copied from RowEncoder */
  private def deserializerFor(schema: StructType): Expression = {
    val arr = schema.zipWithIndex.map { case (field, i) =>
        CreateExternalColumn(BoundReference(i, field.dataType, nullable = true))
    }
    CreateExternalColumnBatch(arr)
  }

  @tailrec
  def externalDataTypeFor(dt: DataType): DataType = dt match {
    case _ if ScalaReflection.isNativeType(dt) => dt
    case TimestampType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        ObjectType(classOf[java.time.Instant])
      } else {
        ObjectType(classOf[java.sql.Timestamp])
      }
    // SPARK-36227: Remove TimestampNTZ type support in Spark 3.2 with minimal code changes.
    case TimestampNTZType if Utils.isTesting =>
      ObjectType(classOf[java.time.LocalDateTime])
    case DateType =>
      if (SQLConf.get.datetimeJava8ApiEnabled) {
        ObjectType(classOf[java.time.LocalDate])
      } else {
        ObjectType(classOf[java.sql.Date])
      }
    case _: DayTimeIntervalType => ObjectType(classOf[java.time.Duration])
    case _: YearMonthIntervalType => ObjectType(classOf[java.time.Period])
    case _: DecimalType => ObjectType(classOf[java.math.BigDecimal])
    case StringType => ObjectType(classOf[java.lang.String])
    case _: ArrayType => ObjectType(classOf[scala.collection.Seq[_]])
    case _: MapType => ObjectType(classOf[scala.collection.Map[_, _]])
    case _: StructType => ObjectType(classOf[TColumn])
    case p: PythonUserDefinedType => externalDataTypeFor(p.sqlType)
    case udt: UserDefinedType[_] => ObjectType(udt.userClass)
  }
}
