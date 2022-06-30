package nl.liacs.mijpelaar.evaluation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.column.{ColumnBatch, ColumnDataFrame, ColumnDataFrameReader, TColumn}

import java.io.FileWriter
import java.nio.file.Paths
import scala.reflect.io.Directory

object EvaluationSuite {
  def checkFirst(answer: ColumnBatch, colNrs: Range): Boolean = {
    val cols = TColumn.fromBatches(Array(answer))

    if (cols.length <= 0) return false
    if (cols.exists( _.length != cols.head.length)) return false // every column same no. elements?
    if (cols.head.length == 1) return true // one row is always sorted

    var result = true
    1 until cols.head.length foreach { rowIndex =>
      colNrs.takeWhile { colIndex =>
        val firstVar = cols(colIndex).get(rowIndex-1)
        val secondVar = cols(colIndex).get(rowIndex)
        if (firstVar.isEmpty || secondVar.isEmpty) return false

        (firstVar.get, secondVar.get) match {
          case (numOne: Int, numTwo: Int) =>
            if (numOne == numTwo) return true  // continue to check the next column
            if (numOne < numTwo) return false  // stop looking, it's alright
            // stop looking, it's bad
            result = false
            return false
          case _ =>
            // wrong type, stop looking
            result = false
            return false
        }
      }
    }

    result
  }

  /** Sort on the first two columns (which we assume are integers) of a parquet file */
  def sort(spark: SparkSession, fw: FileWriter, file: String): Unit = {
    val df = spark.read.parquet(file)
    val cols = df.columns
    assert(cols.length > 0)
    val sorted_df = if (cols.length == 1) df.sort(cols(0)) else df.sort(cols(0), cols(1))
    val vanilla_start = System.nanoTime()
    val first = sorted_df.first()
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
    val cFirst = sorted_cdf.first()
    val cas_stop = System.nanoTime()
    fw.write("CAS compute: %04.3f\n".format((cas_stop-cas_start)/1e9d))
    fw.flush()

    if (!first.equals(cFirst.getRow(0)))
      println("ERROR: first row does not match spark-result")
    if (!checkFirst(cFirst, 0 until 2))
      println("ERROR: first was not sorted")
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
    val first = sorted_df.first()
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
    val cFirst = sorted_cdf.first()
    val cas_stop = System.nanoTime()
    fw.write("CAS compute: %04.3f\n".format((cas_stop-cas_start)/1e9d))
    fw.flush()

    if (!first.equals(cFirst.getRow(0)))
      println("ERROR: first row does not match spark-result")
    if (!checkFirst(cFirst, 0 until 2))
      println("ERROR: first was not sorted")
  }


}
