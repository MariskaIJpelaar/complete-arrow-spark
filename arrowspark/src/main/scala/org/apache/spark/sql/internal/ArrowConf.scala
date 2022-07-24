package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession

object ArrowConf {
  def get[T](sparkSession: SparkSession, entry: ConfigEntry[T]): T = sparkSession.sparkContext.getConf.get(entry)
  val NATIVE_SCANNER_BATCHSIZE: ConfigEntry[Long] = SQLConf.buildConf("spark.arrow.native.reader.batchsize")
    .doc("The batch size to read in through arrow's NativeScanner")
    .version("0.1.0")
    .longConf
    .createWithDefault(Integer.MAX_VALUE)

}
