package nl.liacs.mijpelaar.evaluation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.column._
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.io.FileWriter
import java.nio.file.Paths
import scala.reflect.io.Directory

object EvaluationSuite {
  val isSortedBatch: (ArrowColumnarBatchRow, Range) => Boolean = (answer: ArrowColumnarBatchRow, colNrs: Range) => {
    val columns: Array[ArrowColumnVector] = ArrowColumnarBatchRow.take(Iterator(answer))._2
    try {
      var result = true
      if (columns.length <= 0) result = false
      else if (answer.numRows == 1) result = true
      else if (columns.exists( col => col.getValueVector.getValueCount != answer.numRows )) result = false
      else {
        1 until answer.numRows.toInt foreach { rowIndex =>
          colNrs.takeWhile { colIndex =>
            val numOne = columns(colIndex).getInt(rowIndex - 1)
            val numTwo = columns(colIndex).getInt(rowIndex)
            if (numOne == numTwo) {
              true // continue to check the next column
            } else if (numOne < numTwo) {
              false // stop looking, it's alright
            } else {
              // stop looking, it's bad
              result = false
              false
            }
          }
        }
      }
      result
    } finally {
      columns.foreach( vector => vector.close() )
    }
  }


  val isSorted: (ColumnBatch, Range) => Boolean = (answer: ColumnBatch, colNrs: Range) => {
    val cols = TColumn.fromBatches(Array(answer))
    if (cols.length <= 0) {
      false
    } else if (cols.exists( _.length != cols.head.length)) {
      // every column same no. elements?
      false
    } else if (cols.head.length == 1) {
      // one row is always sorted
      true
    } else {
      var result = true
      1 until cols.head.length foreach { rowIndex =>
        colNrs.takeWhile { colIndex =>
          val firstVar = cols(colIndex).get(rowIndex-1)
          val secondVar = cols(colIndex).get(rowIndex)
          if (firstVar.isEmpty || secondVar.isEmpty) {
            false
          } else {
            (firstVar.get, secondVar.get) match {
              case (numOne: Int, numTwo: Int) =>
                if (numOne == numTwo) {
                  true // continue to check the next column
                } else if (numOne < numTwo) {
                  false // stop looking, it's alright
                }  else {
                  // stop looking, it's bad
                  result = false
                  false
                }
              case _ =>
                // wrong type, stop looking
                result = false
                false
            }
          }
        }
      }
      result
    }
  }

  /** Sort on the first two columns (which we assume are integers) of a parquet file */
  def sort(spark: SparkSession, fw: FileWriter, file: String): Unit = {
    val df = spark.read.parquet(file)
    val cols = df.columns
    assert(cols.length > 0)
    val sorted_df = if (cols.length == 1) df.sort(cols(0)) else df.sort(cols(0), cols(1))
    val vanilla_start = System.nanoTime()
    sorted_df.queryExecution.executedPlan.execute().count()
    val vanilla_stop = System.nanoTime()
    fw.write("Vanilla compute: %04.3f\n".format((vanilla_stop-vanilla_start)/1e9d))
    fw.flush()

    val cdf: ColumnDataFrame =
      new ColumnDataFrameReader(spark).format("org.apache.spark.sql.execution.SimpleParquetArrowFileFormat")
        .loadDF(file)
    val cCols = cdf.columns
    assert(cCols.length > 0)
    val sorted_cdf = if (cCols.length == 1) cdf.sort(cCols(0)) else cdf.sort(cCols(0), cCols(1))
    val cas_start = System.nanoTime()
    sorted_cdf.queryExecution.executedPlan.execute().count()
    val cas_stop = System.nanoTime()
    fw.write("CAS compute: %04.3f\n".format((cas_stop-cas_start)/1e9d))
    fw.flush()
  }

  /** Sort on the first two columns (which we assume are integers) of a directory of parquet files */
  def sort(spark: SparkSession, fw: FileWriter, dir: Directory): Unit = {
    val tableName = "vanilla"
    spark.read.format("parquet").option("mergeSchema", "true").option("dbtable", tableName)
      .load(Paths.get(dir.toString()).resolve("*").toString)
      .createOrReplaceTempView(tableName)
    val df = spark.table(tableName)
    val cols = df.columns
    assert(cols.length > 0)
    val sorted_df = if (cols.length == 1) df.sort(cols(0)) else df.sort(cols(0), cols(1))
    val vanilla_start = System.nanoTime()
    sorted_df.queryExecution.executedPlan.execute().count()
    val vanilla_stop = System.nanoTime()
    fw.write("Vanilla compute: %04.3f\n".format((vanilla_stop-vanilla_start)/1e9d))
    fw.flush()

    val cdf: ColumnDataFrame =
      new ColumnDataFrameReader(spark).format("org.apache.spark.sql.execution.datasources.SimpleParquetArrowFileFormat")
        .loadDF(dir.path)
    val cCols = cdf.columns
    assert(cCols.length > 0)
    val sorted_cdf = if (cCols.length == 1) cdf.sort(cCols(0)) else cdf.sort(cCols(0), cCols(1))
    val cas_start = System.nanoTime()
    val rdd = sorted_cdf.queryExecution.executedPlan.execute()
    rdd.count()
    val cas_stop = System.nanoTime()
    rdd.foreach( batch => batch.asInstanceOf[ArrowColumnarBatchRow].close())
    fw.write("CAS compute: %04.3f\n".format((cas_stop-cas_start)/1e9d))
    fw.flush()
  }


}
