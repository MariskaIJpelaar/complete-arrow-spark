package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.{ArrowPartition, Partition}

import scala.language.implicitConversions

/** Note: copied and adapted from org.apache.spark.sql.execution.datasources.FilePartition */
case class ArrowFilePartition(filePartition: FilePartition) extends Partition with ArrowPartition {
  override def index: Int = filePartition.index
}

object ArrowFilePartition {
  implicit def arrowToFilePartition(arrowFilePartition: ArrowFilePartition): FilePartition =
    arrowFilePartition.filePartition

  def getFilePartitions(sparkSession: SparkSession,
                         partitionedFiles: Seq[PartitionedFile],
                         maxSplitBytes: Long): Seq[ArrowFilePartition] =
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
      .map(filePartition => new ArrowFilePartition(filePartition))
}
