package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigEntry

object ArrowSQLConf {
  def get[T](entry: ConfigEntry[T]): T = SQLConf.get.getConf(entry)

  val NATIVE_SCANNER_BATCHSIZE: ConfigEntry[Long] = SQLConf.buildConf("nl.liacs.mijpelaar.native.batchsize")
    .doc("The batch size to read in through arrow's NativeScanner")
    .version("0.1.0")
    .longConf
    .createWithDefault(Integer.MAX_VALUE)

}
