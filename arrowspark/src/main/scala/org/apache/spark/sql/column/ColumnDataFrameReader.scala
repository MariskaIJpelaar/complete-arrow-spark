package org.apache.spark.sql.column

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.column.execution.datasources.v2.ColumnDataSourceV2Utils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import java.util
import java.util.Locale

/** Used to extend DataFrameReader with load functionalities to create
 * Dataset[TColumn] */
class ColumnDataFrameReader(sparkSession: SparkSession) extends DataFrameReader(sparkSession) {
 /** Loads input in as ColumnDataFrame, for data sources that don't require a path */
 def loadDF(): ColumnDataFrame = loadDF(Seq.empty: _*)

 /** Loads input in as a ColumnDataFrame, for data sources that require a path */
 def loadDF(path: String): ColumnDataFrame = {
  if (sparkSession.sessionState.conf.legacyPathOptionBehavior) {
   option("path", path).loadDF(Seq.empty: _*)
  } else {
   loadDF(Seq(path): _*)
  }
 }
 /** Loads input in as a DataFrame, for data sources with multiple paths
  * Only works if the source is an HadoopFsRelationProvider */
 def loadDF(paths: String*): ColumnDataFrame = {
  if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER)
   throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("read")

  val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
  if (!legacyPathOptionBehavior &&
    (extraOptions.contains("path") || extraOptions.contains("paths")) && paths.nonEmpty)
   throw QueryCompilationErrors.pathOptionNotSetCorrectlyWhenReadingError()

  DataSource.lookupDataSourceV2(source, sparkSession.sessionState.conf).flatMap { provider =>
   ColumnDataSourceV2Utils.loadV2Source(sparkSession, provider, userSpecifiedSchema, extraOptions, source, paths: _*)
  }.getOrElse(loadV1Source(paths: _*))
 }

 /** Mostly copied from DataFrameReader */
 protected def loadV1Source(paths: String*): ColumnDataFrame = {
  val legacyPathOptionBehavior = sparkSession.sessionState.conf.legacyPathOptionBehavior
  val (finalPaths, finalOptions) = if (!legacyPathOptionBehavior && paths.length == 1) {
   (Nil, extraOptions + ("path" -> paths.head))
  } else {
   (paths, extraOptions)
  }

  ColumnDataset.ofColumns(sparkSession, LogicalRelation(DataSource.apply(
   sparkSession,
   paths = finalPaths,
   userSpecifiedSchema = userSpecifiedSchema,
   className = source,
   options = finalOptions.originalMap).resolveRelation()))
 }

 /** Functions to keep _this_ an ColumnDataFrameReader */
 override def format(source: String): ColumnDataFrameReader = super.format(source).asInstanceOf[ColumnDataFrameReader]
 override def schema(schema: StructType): ColumnDataFrameReader = super.schema(schema).asInstanceOf[ColumnDataFrameReader]
 override def schema(schemaString: String): ColumnDataFrameReader = super.schema(schemaString).asInstanceOf[ColumnDataFrameReader]
 override def option(key: String, value: String): ColumnDataFrameReader = super.option(key, value).asInstanceOf[ColumnDataFrameReader]
 override def option(key: String, value: Boolean): ColumnDataFrameReader = super.option(key, value).asInstanceOf[ColumnDataFrameReader]
 override def option(key: String, value: Long): ColumnDataFrameReader = super.option(key, value).asInstanceOf[ColumnDataFrameReader]
 override def option(key: String, value: Double): ColumnDataFrameReader = super.option(key, value).asInstanceOf[ColumnDataFrameReader]
 override def options(options: collection.Map[String, String]): ColumnDataFrameReader = super.options(options).asInstanceOf[ColumnDataFrameReader]
 override def options(options: util.Map[String, String]): ColumnDataFrameReader = super.options(options).asInstanceOf[ColumnDataFrameReader]


 /** Functions to get the private members of DataFrameReader */
 private def getPrivate[T](name: String): T = {
  val field = classOf[DataFrameReader].getDeclaredField(name)
  field.setAccessible(true)
  field.get(this).asInstanceOf[T]
 }
 protected def source: String = getPrivate("source")
 protected def userSpecifiedSchema: Option[StructType] = getPrivate("userSpecifiedSchema")
 protected def extraOptions : CaseInsensitiveMap[String] = getPrivate("extraOptions")
}
