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
}
