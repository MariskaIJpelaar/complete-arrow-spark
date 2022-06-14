package org.apache.spark.sql.column.execution.datasources.v2

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TimeTravelSpec
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.column.ColumnDataset
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsCatalogOptions, SupportsRead, TableProvider}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2Utils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** Note: mostly copied from org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils */
private[sql] object ColumnDataSourceV2Utils extends Logging {
  def loadV2Source(
                    sparkSession: SparkSession,
                    provider: TableProvider,
                    userSpecifiedSchema: Option[StructType],
                    extraOptions: CaseInsensitiveMap[String],
                    source: String,
                    paths: String*): Option[ColumnDataset] = {
    val catalogManager = sparkSession.sessionState.catalogManager
    val conf = sparkSession.sessionState.conf
    val sessionOptions = DataSourceV2Utils.extractSessionConfigs(provider, conf)

    val optionsWithPath = getOptionsWithPaths(extraOptions, paths: _*)

    val finalOptions = sessionOptions.filterKeys(!optionsWithPath.contains(_)) ++
      optionsWithPath.originalMap
    val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
    val (table, catalog, ident) = provider match {
      case _: SupportsCatalogOptions if userSpecifiedSchema.nonEmpty =>
        throw new IllegalArgumentException(
          s"$source does not support user specified schema. Please don't specify the schema.")
      case hasCatalog: SupportsCatalogOptions =>
        val ident = hasCatalog.extractIdentifier(dsOptions)
        val catalog = CatalogV2Util.getTableProviderCatalog(
          hasCatalog,
          catalogManager,
          dsOptions)

        val version = hasCatalog.extractTimeTravelVersion(dsOptions)
        val timestamp = hasCatalog.extractTimeTravelTimestamp(dsOptions)

        val timeTravelVersion = if (version.isPresent) Some(version.get) else None
        val timeTravelTimestamp = if (timestamp.isPresent) Some(Literal(timestamp.get)) else None
        val timeTravel = TimeTravelSpec.create(timeTravelTimestamp, timeTravelVersion, conf)
        (CatalogV2Util.loadTable(catalog, ident, timeTravel).get, Some(catalog), Some(ident))
      case _ =>
        // TODO: Non-catalog paths for DSV2 are currently not well defined.
        val tbl = DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema)
        (tbl, None, None)
    }
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    table match {
      case _: SupportsRead if table.supports(BATCH_READ) =>
        Option(ColumnDataset.ofColumns(
          sparkSession,
          DataSourceV2Relation.create(table, catalog, ident, dsOptions)))
      case _ => None
    }
  }

  private def getOptionsWithPaths(
       extraOptions: CaseInsensitiveMap[String],
       paths: String*): CaseInsensitiveMap[String] = {
    if (paths.isEmpty) {
      extraOptions
    } else if (paths.length == 1) {
      extraOptions + ("path" -> paths.head)
    } else {
      val objectMapper = new ObjectMapper()
      extraOptions + ("paths" -> objectMapper.writeValueAsString(paths.toArray))
    }
  }

}
