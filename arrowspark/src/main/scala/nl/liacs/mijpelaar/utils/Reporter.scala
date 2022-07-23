package nl.liacs.mijpelaar.utils

import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowEncoders, ArrowColumnarBatchRowSerializerInstance, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}
import org.apache.spark.sql.column.utils.algorithms.{ArrowColumnarBatchRowDeduplicators, ArrowColumnarBatchRowDistributors, ArrowColumnarBatchRowSamplers, ArrowColumnarBatchRowSorters}
import org.apache.spark.sql.execution.ArrowSortExec

object Reporter {

  private def report(name: String, value: Long): String = {
    val converted = value / 1e9d
    if (converted < 0.01) ""
    else "%s %04.3f".format(name, converted)
  }

  def report(id: String = ""): Unit = {
    val report =
      s"""
         | builder:           ${"%04.3f".format(ArrowColumnarBatchRowBuilder.totalTime.get / 1e9d)}
         | unique:            ${"%04.3f".format(ArrowColumnarBatchRowDeduplicators.totalTime.get / 1e9d)}
         | bucketDistributor: ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeBucketDistributor.get / 1e9d)}
         | distributeBySort:  ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeDistributeBySort.get / 1e9d)}
         | distribute:        ${"%04.3f".format(ArrowColumnarBatchRowDistributors.totalTimeDistribute.get / 1e9d)}
         | sample:            ${"%04.3f".format(ArrowColumnarBatchRowSamplers.totalTimeSample.get / 1e9d)}
         | sampleAndCount:    ${"%04.3f".format(ArrowColumnarBatchRowSamplers.totalTimeSampleAndCount.get / 1e9d)}
         | multiColumnSort:   ${"%04.3f".format(ArrowColumnarBatchRowSorters.totalTimeMultiColumnSort.get / 1e9d)}
         | sort:              ${"%04.3f".format(ArrowColumnarBatchRowSorters.totalTimeSort.get / 1e9d)}
         | toRecordBatch:     ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToRecordBatch.get / 1e9d)}
         | toRoot:            ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToRoot.get / 1e9d)}
         | split:             ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToSplit.get / 1e9d)}
         | splitColumns:      ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToSplitColumns.get / 1e9d)}
         | toUnionVector:     ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToUnionVector.get / 1e9d)}
         | makeFresh:         ${"%04.3f".format(ArrowColumnarBatchRowConverters.totalTimeToMakeFresh.get / 1e9d)}
         | encode:            ${"%04.3f".format(ArrowColumnarBatchRowEncoders.totalTimeEncode.get / 1e9d)}
         | decode:            ${"%04.3f".format(ArrowColumnarBatchRowEncoders.totalTimeDecode.get / 1e9d)}
         | serialize:         ${"%04.3f".format(ArrowColumnarBatchRowSerializerInstance.totalTimeSerialize.get / 1e9d)}
         | deserialize:       ${"%04.3f".format(ArrowColumnarBatchRowSerializerInstance.totalTimeDeserialize.get / 1e9d)}
         | projection:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeProjection.get / 1e9d)}
         | take-batch:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeTake.get / 1e9d)}
         | sample-batch:      ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeSample.get / 1e9d)}
         | appendColumns:     ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeAppendColumns.get / 1e9d)}
         | getColumns:        ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeGetColumns.get / 1e9d)}
         | applyIndices:      ${"%04.3f".format(ArrowColumnarBatchRowTransformers.totalTimeApplyIndices.get / 1e9d)}
         | getComparator:     ${"%04.3f".format(ArrowColumnarBatchRowUtils.totalTimeGetComparator.get / 1e9d)}
         | take:              ${"%04.3f".format(ArrowColumnarBatchRowUtils.totalTimeTake.get / 1e9d)}
         | copy:              ${"%04.3f".format(ArrowColumnarBatchRow.totalTimeCopy.get / 1e9d)}
         | transfer:          ${"%04.3f".format(ArrowColumnarBatchRow.totalTransferTime.get / 1e9d)}
         | sort exec:         ${"%04.3f".format(ArrowSortExec.totalTime.get / 1e9d)}
         |""".stripMargin

    println(s"REPORT $id: $report")
  }

}
