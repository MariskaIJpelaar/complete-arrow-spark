package org.apache.spark.sql.internal

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime

object ArrowConf {
  private val latestVersion = "0.1.0"

  def get[T](sparkSession: SparkSession, entry: ConfigEntry[T]): T = get(sparkSession.sparkContext, entry)
  def get[T](sparkContext: SparkContext, entry: ConfigEntry[T]): T = sparkContext.getConf.get(entry)

  val NATIVE_SCANNER_BATCHSIZE: ConfigEntry[Long] = SQLConf.buildConf("spark.arrow.native.reader.batchsize")
    .doc("The batch size to read in through arrow's NativeScanner")
    .version(latestVersion)
    .longConf
    .createWithDefault(Integer.MAX_VALUE)

  val ARROWRDD_REPORT_DIRECTORY: ConfigEntry[String] = SQLConf.buildConf("spark.arrow.rdd.report.directory")
    .doc("The directory to write reports in")
    .version(latestVersion)
    .stringConf
    .createWithDefault(s"/tmp/sparkarrow/${LocalDateTime.now()}")

  val TIMSORT_RUN: ConfigEntry[Int] = SQLConf.buildConf("spark.arrow.sort.timsort.run")
    .doc("RUN variable to use within TimSort")
    .version(latestVersion)
    .intConf
    .createWithDefault(32)

  abstract sealed class DistributorAlgorithm(function: String)
  case object ByBatches extends DistributorAlgorithm("byBatches")
  case object ByBuilders extends DistributorAlgorithm("byBuilders")
  case object BySorting extends DistributorAlgorithm("bySorting")
  def fromFunction(function: String): Option[DistributorAlgorithm] = function match {
    case "byBatches" => Some(ByBatches)
    case "byBuilders" => Some(ByBuilders)
    case "bySorting" => Some(BySorting)
    case _ => None
  }
  private val distributorAlgorithms: Array[String] = Array("byBatches", "byBuilders", "bySorting")
  private val distributorAlgorithmsString: String = distributorAlgorithms.map(alg => s"'$alg''").mkString(", ")
  val DISTRIBUTOR_ALGORITHM: ConfigEntry[String] = SQLConf.buildConf("spark.arrow.distributor.algorithm")
    .doc(s"The algorithm to use for distribution, choices: $distributorAlgorithmsString")
    .version(latestVersion)
    .stringConf
    .checkValue( distributorAlgorithms.contains(_), errorMsg = s"Value not one of $distributorAlgorithmsString" )
    .createWithDefault(distributorAlgorithms(2))
}
