package nl.liacs.mijpelaar.benchmarks

import org.apache.spark.sql.column.ColumnBatch
import org.apache.spark.sql.{Dataset, SparkSession}
import org.openjdk.jmh.annotations.{Level, TearDown}


// TODO: https://github.com/g1thubhub/jmh-spark/blob/master/benchmarks/src/main/scala/spark_benchmarks/Bench_APIs2.scala
class CDSBenchmark(val spark: SparkSession, val cds: Dataset[ColumnBatch]) {


  @TearDown(Level.Invocation)
  def teardown(): Unit = {
    cds.unpersist(true)
  }


}
