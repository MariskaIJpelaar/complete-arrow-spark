package utils

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.util.vector.read.ParquetReaderIterator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.ArrowFileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetUtils}
import org.apache.spark.sql.execution.datasources.{OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.StructType

/** SimpleArrowFileFormat that does not support filters or options
 * Note: some functions have been copied from:
 * https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow-spark/blob/master/arrow-spark-connector/src/main/scala/org/apache/spark/sql/execution/datasources/arrow/ArrowFileFormat.scala */
class SimpleArrowFileFormat extends ArrowFileFormat with DataSourceRegister with Serializable with Logging {
  private lazy val rootAllocator = new RootAllocator(Integer.MAX_VALUE)

  /** Checks whether we can split the file: copied from arrow-spark::ArrowFileFormat */
  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = false

  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] =
    super.inferSchema(ParquetUtils.inferSchema(sparkSession, options, files))


  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory =
    (new ParquetFileFormat).prepareWrite(sparkSession, job, options, dataSchema)

  override def shortName(): String = "simple-sparrow"
  override def toString: String = "Simple-SpArrow-Format"

  /** Returns a function that can be used to read a single file in as an Iterator of Array[ValueVector] */
  override def buildArrowReaderWithPartitionValues(sparkSession: SparkSession, dataSchema: StructType, partitionSchema: StructType, requiredSchema: StructType, filters: Seq[Filter], options: Map[String, String], hadoopConf: Configuration): PartitionedFile => Iterator[ArrowColumnarBatchRow] = {
    (file: PartitionedFile) => { new ParquetReaderIterator(file, rootAllocator)}
  }
}
