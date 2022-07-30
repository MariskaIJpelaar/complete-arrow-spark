package nl.liacs.mijpelaar.utils

import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils._
import org.apache.spark.sql.column.utils.algorithms.{ArrowColumnarBatchRowDeduplicators, ArrowColumnarBatchRowDistributors, ArrowColumnarBatchRowSamplers, ArrowColumnarBatchRowSorters}
import org.apache.spark.sql.execution.ArrowSortExec

import java.io.FileWriter
import java.nio.file.{Files, Paths}
import scala.reflect.io.Directory

object Reporter {
  private def reportSingle(name: String, value: Long): String = {
    val converted = value / 1e9d
    if (converted < 0.01) ""
    else "%s %04.3f\n".format(name, converted)
  }

  def report(directory: Directory, executorId: String = "", stageId: String = "", partitionId: String = ""): Unit = {
    var report = ""
    report += reportSingle("builder", ArrowColumnarBatchRowBuilder.totalTime.get)
    report += reportSingle("unique", ArrowColumnarBatchRowDeduplicators.totalTime.get)
    report += reportSingle("bucketDistributor", ArrowColumnarBatchRowDistributors.totalTimeBucketDistributor.get)
    report += reportSingle("distributeBySort", ArrowColumnarBatchRowDistributors.totalTimeDistributeBySort.get)
    report += reportSingle("distributeByBuilder", ArrowColumnarBatchRowDistributors.totalTimeDistributeByBuilder.get)
    report += reportSingle("distributeByBatches", ArrowColumnarBatchRowDistributors.totalTimeDistributeByBatches.get)
    report += reportSingle("sample", ArrowColumnarBatchRowSamplers.totalTimeSample.get)
    report += reportSingle("sampleAndCount", ArrowColumnarBatchRowSamplers.totalTimeSampleAndCount.get)
    report += reportSingle("multiColumnSort", ArrowColumnarBatchRowSorters.totalTimeMultiColumnSort.get)
    report += reportSingle("sort", ArrowColumnarBatchRowSorters.totalTimeSort.get)
    report += reportSingle("toRecordBatch", ArrowColumnarBatchRowConverters.totalTimeToRecordBatch.get)
    report += reportSingle("toRoot", ArrowColumnarBatchRowConverters.totalTimeToRoot.get)
    report += reportSingle("split", ArrowColumnarBatchRowConverters.totalTimeToSplit.get)
    report += reportSingle("splitColumns", ArrowColumnarBatchRowConverters.totalTimeToSplitColumns.get)
    report += reportSingle("toUnionVector", ArrowColumnarBatchRowConverters.totalTimeToUnionVector.get)
    report += reportSingle("makeFresh", ArrowColumnarBatchRowConverters.totalTimeToMakeFresh.get)
    report += reportSingle("encode", ArrowColumnarBatchRowEncoders.totalTimeEncode.get)
    report += reportSingle("decode", ArrowColumnarBatchRowEncoders.totalTimeDecode.get)
    report += reportSingle("serialize", ArrowColumnarBatchRowSerializerInstance.totalTimeSerialize.get)
    report += reportSingle("deserialize", ArrowColumnarBatchRowSerializerInstance.totalTimeDeserialize.get)
    report += reportSingle("projection", ArrowColumnarBatchRowTransformers.totalTimeProjection.get)
    report += reportSingle("take-batch", ArrowColumnarBatchRowTransformers.totalTimeTake.get)
    report += reportSingle("sample-batch", ArrowColumnarBatchRowTransformers.totalTimeSample.get)
    report += reportSingle("appendColumns", ArrowColumnarBatchRowTransformers.totalTimeAppendColumns.get)
    report += reportSingle("getColumns", ArrowColumnarBatchRowTransformers.totalTimeGetColumns.get)
    report += reportSingle("applyIndices", ArrowColumnarBatchRowTransformers.totalTimeApplyIndices.get)
    report += reportSingle("getComparator", ArrowColumnarBatchRowUtils.totalTimeGetComparator.get)
    report += reportSingle("take", ArrowColumnarBatchRowUtils.totalTimeTake.get)
    report += reportSingle("copy", ArrowColumnarBatchRow.totalTimeCopy.get)
    report += reportSingle("transfer", ArrowColumnarBatchRow.totalTransferTime.get)
    report += reportSingle("sort-exec", ArrowSortExec.totalTime.get)

    val path = directory
      .resolve(executorId.filterNot(_.isWhitespace))
      .resolve(stageId.filterNot(_.isWhitespace))
      .resolve(s"${partitionId.filterNot(_.isWhitespace)}.log")

    Files.createDirectories(Paths.get(path.parent.path))
    path.createFile(failIfExists = false)

    Resources.autoCloseTryGet(new FileWriter(path.path)) { writer =>
      writer.write(s"# Executor ID: $executorId, Stage ID: $stageId, Partition ID: $partitionId\n")
      writer.write(report)
    }
  }

}
