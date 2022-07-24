package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDateTime

object ArrowConf {
  private val latestVersion = "0.1.0"

  /** Generic 'get' functions */
  def get[T](sparkSession: SparkSession, entry: ConfigEntry[T]): T = get(sparkSession.sparkContext, entry)
  def get[T](sparkContext: SparkContext, entry: ConfigEntry[T]): T = sparkContext.getConf.get(entry)

  /** Simple Configurations */
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

  /** Distributor-Algorithms */
  abstract sealed class DistributorAlgorithm(function: String)
  object DistributorAlgorithm {
    case object ByBatches extends DistributorAlgorithm("byBatches")
    case object ByBuilders extends DistributorAlgorithm("byBuilders")
    case object BySorting extends DistributorAlgorithm("bySorting")

    def fromFunction(function: String): Option[DistributorAlgorithm] = function match {
      case "byBatches" => Some(ByBatches)
      case "byBuilders" => Some(ByBuilders)
      case "bySorting" => Some(BySorting)
      case _ => None
    }
  }
  private val distributorAlgorithms: Array[String] = Array("byBatches", "byBuilders", "bySorting")
  private val distributorAlgorithmsString: String = distributorAlgorithms.map(alg => s"'$alg''").mkString(", ")
  val DISTRIBUTOR_ALGORITHM: ConfigEntry[String] = SQLConf.buildConf("spark.arrow.distributor.algorithm")
    .doc(s"The algorithm to use for distribution, choices: $distributorAlgorithmsString")
    .version(latestVersion)
    .stringConf
    .checkValue( distributorAlgorithms.contains(_), errorMsg = s"Value not one of $distributorAlgorithmsString" )
    .createWithDefault(distributorAlgorithms(2))
  def getDistributorAlgorithm(sparkConf: SparkConf): Option[DistributorAlgorithm] = DistributorAlgorithm.fromFunction(sparkConf.get(DISTRIBUTOR_ALGORITHM))

  /** Sorting-Algorithms */
  abstract sealed class SortingAlgorithm(function: String)
  object SortingAlgorithm {
    case object GenericQuicksort extends SortingAlgorithm("genericQuicksort")
    case object CompiledQuicksort extends SortingAlgorithm("compiledQuicksort")
    case object CompiledInsertionsort extends SortingAlgorithm("compiledInsertionsort")
    case object CompiledTomsort extends SortingAlgorithm("compiledTomsort")

    def fromFunction(function: String): Option[SortingAlgorithm] = function match {
      case "genericQuicksort" => Some(GenericQuicksort)
      case "compiledQuicksort" => Some(CompiledQuicksort)
      case "compiledInsertionsort" => Some(CompiledInsertionsort)
      case "compiledTomsort" => Some(CompiledTomsort)
      case _ => None
    }
  }
  private val sortingAlgorithms: Array[String] = Array("genericQuicksort", "compiledQuicksort", "compiledInsertionsort", "compiledTomsort")
  private val sortingAlgorithmString: String = sortingAlgorithms.map(alg => s"'$alg''").mkString(", ")
  val SORTING_ALGORITHM: ConfigEntry[String] = SQLConf.buildConf("spark.arrow.sorting.algorithm")
    .doc(s"The algorithm to use for sorting, choices: $sortingAlgorithmString")
    .version(latestVersion)
    .stringConf
    .checkValue(sortingAlgorithms.contains(_), errorMsg = s"Value not one of $sortingAlgorithmString")
    .createWithDefault(sortingAlgorithms(3))

  def getSortingAlgorithm(sparkContext: SparkContext): Option[SortingAlgorithm] = getSortingAlgorithm(sparkContext.getConf)
  def getSortingAlgorithm(sparkConf: SparkConf): Option[SortingAlgorithm] = SortingAlgorithm.fromFunction(sparkConf.get(SORTING_ALGORITHM))

}
