package nl.liacs.mijpelaar.utils

import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowEncoders, ArrowColumnarBatchRowSerializerInstance, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}
import org.apache.spark.sql.column.utils.algorithms.{ArrowColumnarBatchRowDeduplicators, ArrowColumnarBatchRowDistributors, ArrowColumnarBatchRowSamplers, ArrowColumnarBatchRowSorters}
import org.apache.spark.sql.execution.ArrowSortExec

object Reporter {
  def report(id: String = ""): Unit = {
    val report =
      s"""
         | builder:           ${"%04.3f".format(ArrowColumnarBatchRowBuilder.totalTime / 1e9d)}
         | unique:            ${"%04.3f".format(ArrowColumnarBatchRowDeduplicators.totalTime / 1e9d)}
         | bucketDistributor: ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeBucketDistributor / 1e9d)}
         | distributeBySort:  ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeDistributeBySort / 1e9d)}
         | distribute:        ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeDistribute / 1e9d)}
         | sample:            ${"%04.3f".format(ArrowColumnarBatchRowSamplers.totalTimeSample / 1e9d)}
         | sampleAndCount:    ${"%04.3f".format(ArrowColumnarBatchRowSamplers.totalTimeSampleAndCount / 1e9d)}
         | multiColumnSort:   ${"%04.3f".format(ArrowColumnarBatchRowSorters.totalTimeMultiColumnSort / 1e9d)}
         | sort:              ${"%04.3f".format(ArrowColumnarBatchRowSorters.totalTimeSort / 1e9d)}
         | toRecordBatch:     ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToRecordBatch / 1e9d)}
         | toRoot:            ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToRoot / 1e9d)}
         | split:             ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToSplit / 1e9d)}
         | splitColumns:      ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToSplitColumns / 1e9d)}
         | toUnionVector:     ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToUnionVector / 1e9d)}
         | makeFresh:         ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToMakeFresh / 1e9d)}
         | encode:            ${"%04.3f".format(ArrowColumnarBatchRowEncoders.totalTimeEncode / 1e9d)}
         | decode:            ${"%04.3f".format(ArrowColumnarBatchRowEncoders.totalTimeDecode / 1e9d)}
         | serialize:         ${"%04.3f".format(ArrowColumnarBatchRowSerializerInstance.totalTimeSerialize / 1e9d)}
         | deserialize:       ${"%04.3f".format(ArrowColumnarBatchRowSerializerInstance.totalTimeDeserialize / 1e9d)}
         | projection:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeProjection / 1e9d)}
         | take-batch:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeTake / 1e9d)}
         | sample-batch:      ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeSample / 1e9d)}
         | appendColumns:     ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeAppendColumns / 1e9d)}
         | getColumns:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeGetColumns / 1e9d)}
         | applyIndices:      ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeApplyIndices / 1e9d)}
         | getComparator:     ${"%04.3f".format(ArrowColumnarBatchRowUtils.totalTimeGetComparator / 1e9d)}
         | take:              ${"%04.3f".format(ArrowColumnarBatchRowUtils.totalTimeTake / 1e9d)}
         | copy:              ${"%04.3f".format(ArrowColumnarBatchRow.totalTimeCopy / 1e9d)}
         | transfer:          ${"%04.3f".format(ArrowColumnarBatchRow.totalTransferTime / 1e9d)}
         | sort exec:         ${"%04.3f".format(ArrowSortExec.totalTime / 1e9d)}
         |""".stripMargin

    println(s"REPORT $id: $report")
  }

}
