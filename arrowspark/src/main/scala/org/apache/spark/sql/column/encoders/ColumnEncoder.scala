package org.apache.spark.sql.column.encoders

import org.apache.spark.sql.catalyst.DeserializerBuildHelper._
import org.apache.spark.sql.catalyst.SerializerBuildHelper._
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CheckOverflow, CreateArray, CreateNamedStruct, Expression, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.catalyst.{ScalaReflection, WalkedTypePath}
import org.apache.spark.sql.column.encoders.ColumnEncoder.serializerFor
import org.apache.spark.sql.column.{ColumnBatch, TColumn}
import org.apache.spark.sql.column.expressions.objects.{CreateExternalColumn, CreateExternalColumnBatch, GetExternalColumn, GetExternalColumnBatch}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import scala.annotation.tailrec
import scala.collection.Map
import scala.reflect.ClassTag

object ColumnEncoder {
  def apply(schema: StructType): ExpressionEncoder[ColumnBatch] = {
    val cls = classOf[ColumnBatch]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
//    val serializer = serializerFor(inputObject, schema, lenient = false)
    val serializer = serializerFor(inputObject)
    val arr_schema = new StructType( schema.fields.map { field =>
      field.copy(dataType = ArrayType.apply(field.dataType))
    } )
    val deserializer = deserializerFor(arr_schema)
    new ExpressionEncoder[ColumnBatch](serializer, deserializer, ClassTag(cls))
  }

  private def serializerFor(inputObject: Expression): Expression = {
    // TODO: probably wrong, but does not seem to be used atm
    val batch = GetExternalColumnBatch(inputObject)
    GetExternalColumn(batch)

//      val fields = schema.zipWithIndex.flatMap { case (field, index) =>
//        val input = BoundReference(index, field.dataType, nullable = true)
//        val column = GetExternalColumn(input)
//        val expression = ValidateExternalType(column, field.dataType, lenient = false)
//        val fieldValue = serializerFor(expression, field.dataType, lenient = false)
//        val convertedField = if (field.nullable) {
//          If(
//            Invoke(input, "isNullAt", BooleanType, Literal(index) :: Nil),
//            Literal.create(null, fieldValue.dataType),
//            fieldValue
//          )
//        } else {
//          fieldValue
//        }
//        Literal(field.name) :: convertedField :: Nil
//      }
//
//      GetExternalColumnBatch(CreateArray(fields))
  }

  private def serializerFor(inputObject: Expression, inputType: DataType, lenient: Boolean): Expression = inputType match {
    case dt if ScalaReflection.isNativeType(dt) => inputObject

    case p: PythonUserDefinedType => serializerFor(inputObject, p.sqlType, lenient)

    case udt: UserDefinedType[_] =>
      val annotation = udt.userClass.getAnnotation(classOf[SQLUserDefinedType])
      val udtClass: Class[_] = if (annotation != null) {
        annotation.udt()
      } else {
        UDTRegistration.getUDTFor(udt.userClass.getName).getOrElse {
          throw QueryExecutionErrors.userDefinedTypeNotAnnotatedAndRegisteredError(udt)
        }
      }
      val obj = NewInstance(
        udtClass,
        Nil,
        dataType = ObjectType(udtClass), propagateNull = false)
      Invoke(obj, "serialize", udt, inputObject :: Nil, returnNullable = false)

    case TimestampType =>
      if (lenient) {
        createSerializerForAnyTimestamp(inputObject)
      } else if (SQLConf.get.datetimeJava8ApiEnabled) {
        createSerializerForJavaInstant(inputObject)
      } else {
        createSerializerForSqlTimestamp(inputObject)
      }

    // SPARK-38813: Remove TimestampNTZ type support in Spark 3.3 with minimal code changes.
    case TimestampNTZType if Utils.isTesting => createSerializerForLocalDateTime(inputObject)

    case DateType =>
      if (lenient) {
        createSerializerForAnyDate(inputObject)
      } else if (SQLConf.get.datetimeJava8ApiEnabled) {
        createSerializerForJavaLocalDate(inputObject)
      } else {
        createSerializerForSqlDate(inputObject)
      }

    case _: DayTimeIntervalType => createSerializerForJavaDuration(inputObject)

    case _: YearMonthIntervalType => createSerializerForJavaPeriod(inputObject)

    case d: DecimalType =>
      CheckOverflow(StaticInvoke(
        Decimal.getClass,
        d,
        "fromDecimal",
        inputObject :: Nil,
        returnNullable = false), d, !SQLConf.get.ansiEnabled)

    case StringType => createSerializerForString(inputObject)

    case t @ ArrayType(et, containsNull) =>
      et match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
          StaticInvoke(
            classOf[ArrayData],
            t,
            "toArrayData",
            inputObject :: Nil,
            returnNullable = false)

        case _ =>
          createSerializerForMapObjects(
            inputObject,
            ObjectType(classOf[Object]),
            element => {
              val value = serializerFor(ValidateExternalType(element, et, lenient), et, lenient)
              expressionWithNullSafety(value, containsNull, WalkedTypePath())
            })
      }

    case t @ MapType(kt, vt, valueNullable) =>
      val keys =
        Invoke(
          Invoke(inputObject, "keysIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedKeys = serializerFor(keys, ArrayType(kt, containsNull = false), lenient)

      val values =
        Invoke(
          Invoke(inputObject, "valuesIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedValues = serializerFor(values, ArrayType(vt, valueNullable), lenient)

      val nonNullOutput = NewInstance(
        classOf[ArrayBasedMapData],
        convertedKeys :: convertedValues :: Nil,
        dataType = t,
        propagateNull = false)

      if (inputObject.nullable) {
        expressionForNullableExpr(inputObject, nonNullOutput)
      } else {
        nonNullOutput
      }

    case StructType(fields) =>
      val nonNullOutput = CreateNamedStruct(fields.zipWithIndex.flatMap { case (field, index) =>
        val fieldValue = serializerFor(
          ValidateExternalType(
            GetExternalColumnBatch(inputObject),
//            GetExternalRowField(inputObject, index, field.name),
            field.dataType,
            lenient),
          field.dataType,
          lenient)
        val convertedField = if (field.nullable) {
          If(
            Invoke(inputObject, "isNullAt", BooleanType, Literal(index) :: Nil),
            // Because we strip UDTs, `field.dataType` can be different from `fieldValue.dataType`.
            // We should use `fieldValue.dataType` here.
            Literal.create(null, fieldValue.dataType),
            fieldValue
          )
        } else {
          fieldValue
        }
        Literal(field.name) :: convertedField :: Nil
      })

      if (inputObject.nullable) {
        expressionForNullableExpr(inputObject, nonNullOutput)
      } else {
        nonNullOutput
      }
  }

  /** Note below methods are copied from RowEncoder */
  private def deserializerFor(schema: StructType): Expression = {
//    GetColumnByOrdinal(0, serializer.dataType)
//    val arr = schema.zipWithIndex.map { case (_, i) =>
//      deserializerFor(GetStructField(input, i))
//    }
    val arr = schema.zipWithIndex.map { case (_, i) =>
//      GetColumnByOrdinal(i, ObjectType(classOf[TColumn]))
        CreateExternalColumn(GetColumnByOrdinal(i, ObjectType(classOf[TColumn])) :: Nil)
//      deserializerFor(GetStructField(schema, i))
    }
    CreateExternalColumnBatch(arr)
  }

//  private def deserializerFor(input: Expression): Expression = {
//    deserializerFor(input, input.dataType)
//  }
//
//  @tailrec
//  private def deserializerFor(input: Expression, dataType: DataType): Expression = dataType match {
//    case dt if ScalaReflection.isNativeType(dt) => input
//
//    case p: PythonUserDefinedType => deserializerFor(input, p.sqlType)
//
//    case udt: UserDefinedType[_] =>
//      val annotation = udt.userClass.getAnnotation(classOf[SQLUserDefinedType])
//      val udtClass: Class[_] = if (annotation != null) {
//        annotation.udt()
//      } else {
//        UDTRegistration.getUDTFor(udt.userClass.getName).getOrElse {
//          throw QueryExecutionErrors.userDefinedTypeNotAnnotatedAndRegisteredError(udt)
//        }
//      }
//      val obj = NewInstance(
//        udtClass,
//        Nil,
//        dataType = ObjectType(udtClass))
//      Invoke(obj, "deserialize", ObjectType(udt.userClass), input :: Nil)
//
//    case TimestampType =>
//      if (SQLConf.get.datetimeJava8ApiEnabled) {
//        createDeserializerForInstant(input)
//      } else {
//        createDeserializerForSqlTimestamp(input)
//      }
//
//    // SPARK-38813: Remove TimestampNTZ type support in Spark 3.3 with minimal code changes.
//    case TimestampNTZType if Utils.isTesting =>
//      createDeserializerForLocalDateTime(input)
//
//    case DateType =>
//      if (SQLConf.get.datetimeJava8ApiEnabled) {
//        createDeserializerForLocalDate(input)
//      } else {
//        createDeserializerForSqlDate(input)
//      }
//
//    case _: DayTimeIntervalType => createDeserializerForDuration(input)
//
//    case _: YearMonthIntervalType => createDeserializerForPeriod(input)
//
//    case _: DecimalType => createDeserializerForJavaBigDecimal(input, returnNullable = false)
//
//    case StringType => createDeserializerForString(input, returnNullable = false)
//
//    case ArrayType(et, _) =>
//      val arrayData =
//        Invoke(
//          MapObjects(deserializerFor, input, et),
//          "array",
//          ObjectType(classOf[Array[_]]), returnNullable = false)
//      // TODO should use `scala.collection.immutable.ArrayDeq.unsafeMake` method to create
//      //  `immutable.Seq` in Scala 2.13 when Scala version compatibility is no longer required.
//      StaticInvoke(
//        scala.collection.mutable.WrappedArray.getClass,
//        ObjectType(classOf[scala.collection.Seq[_]]),
//        "make",
//        arrayData :: Nil,
//        returnNullable = false)
//
//    case MapType(kt, vt, valueNullable) =>
//      val keyArrayType = ArrayType(kt, containsNull = false)
//      val keyData = deserializerFor(Invoke(input, "keyArray", keyArrayType))
//
//      val valueArrayType = ArrayType(vt, valueNullable)
//      val valueData = deserializerFor(Invoke(input, "valueArray", valueArrayType))
//
//      StaticInvoke(
//        ArrayBasedMapData.getClass,
//        ObjectType(classOf[Map[_, _]]),
//        "toScalaMap",
//        keyData :: valueData :: Nil,
//        returnNullable = false)
//
//    case StructType(fields) =>
//      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
//        If(
//          Invoke(input, "isNullAt", BooleanType, Literal(i) :: Nil),
//          Literal.create(null, externalDataTypeFor(f.dataType)),
//          deserializerFor(GetStructField(input, i)))
//      }
//      If(IsNull(input),
//        Literal.create(null, externalDataTypeFor(input.dataType)),
//        CreateExternalColumn(convertedFields))
//  }

  private def expressionForNullableExpr(
   expr: Expression,
   newExprWhenNotNull: Expression): Expression = {
    If(IsNull(expr), Literal.create(null, newExprWhenNotNull.dataType), newExprWhenNotNull)
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

///**
// * Function that serializes an object of type `T` to an [[InternalRow]]. This class is not
// * thread-safe. Note that multiple calls to `apply(..)` return the same actual [[InternalRow]]
// * object.  Thus, the caller should copy the result before making another call if required.
// */
//class Serializer[T: ClassTag](private val expressions: Seq[Expression])
//  extends (T => InternalRow) with Serializable {
//  @transient
//  private[this] var inputCol: GenericColumn[T] = _
//
//  @transient
//  private[this] var extractProjection: UnsafeProjection = _
//
//  override def apply(t: T): InternalRow = try {
//    if (extractProjection == null) {
//      inputCol = new GenericColumn[T](1)
//      extractProjection = GenerateUnsafeProjection.generate(expressions)
//    }
//    inputCol.set(0, t)
//    extractProjection(inputCol.asInstanceOf[InternalRow])
//  } catch {
//    case e: Exception =>
//      throw QueryExecutionErrors.expressionEncodingError(e, expressions)
//  }
//}
//
///**
// * Function that deserializes an [[InternalRow]] into an object of type `T`. This class is not
// * thread-safe.
// */
//class Deserializer[T](private val expressions: Seq[Expression])
//  extends (InternalRow => T) with Serializable {
//  @transient
//  private[this] var constructProjection: Projection = _
//
//  override def apply(row: InternalRow): T = try {
//    if (constructProjection == null) {
//      constructProjection = SafeProjection.create(expressions)
//    }
//    constructProjection(row).asInstanceOf[TColumn].get(0).asInstanceOf[T]
//  } catch {
//    case e: Exception =>
//      throw QueryExecutionErrors.expressionDecodingError(e, expressions)
//  }
//}
//
//class ColumnEncoder[T: ClassTag](expressionEncoder: ExpressionEncoder[T]) extends Encoder[T] {
//  override def schema: StructType = expressionEncoder.schema
//  override def clsTag: ClassTag[T] = expressionEncoder.clsTag
//  def assertUnresolved(): Unit = expressionEncoder.assertUnresolved()
//  def encoder: ExpressionEncoder[T] = expressionEncoder
//
//  /** Functions to get private members of ExpressionEncoder */
//  private def getPrivate[U](name: String): U = {
//    val field = classOf[ExpressionEncoder[U]].getDeclaredField(name)
//    field.setAccessible(true)
//    field.get(expressionEncoder).asInstanceOf[U]
//  }
//
//  protected lazy val optimizedDeserializer: Seq[Expression] = getPrivate("optimizedDeserializer")
//  protected lazy val optimizedSerializer: Seq[Expression] = getPrivate("optimizedSerializer")
//
//  /**
//   * Create a serializer that can convert an object of type `T` to a Spark SQL TColumn.
//   *
//   * Note that the returned [[Serializer]] is not thread safe. Multiple calls to
//   * `serializer.apply(..)` are allowed to return the same actual [[InternalRow]] object.  Thus,
//   *  the caller should copy the result before making another call if required.
//   */
//  def createSerializer(): Serializer[T] = new Serializer[T](optimizedSerializer)
//
//  /**
//   * Create a deserializer that can convert a Spark SQL TColumn into an object of type `T`.
//   *
//   * Note that you must `resolveAndBind` an encoder to a specific schema before you can create a
//   * deserializer.
//   */
//  def createDeserializer(): Deserializer[T] = new Deserializer[T](optimizedDeserializer)
//}
