package nl.liacs.mijpelaar

import nl.liacs.mijpelaar.evaluation.EvaluationSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.ArrowConf
import org.apache.spark.sql.util.ArrowSparkExtensionWrapper
import picocli.CommandLine

import java.io.{File, FileWriter}
import java.nio.file.{Path, Paths}
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.Callable
import scala.reflect.io.Directory
import scala.sys.exit

object Main {
  def main(args: Array[String]): Unit = {
    new CommandLine(new Main()).execute(args:_*)
  }
}

class Main extends Callable[Unit] {
  @picocli.CommandLine.Option(names = Array("-d", "--data-dir"))
  private var data_dir: String = ""
  @picocli.CommandLine.Option(names = Array("-f", "--data-file"))
  private var data_file: String = ""
  @picocli.CommandLine.Option(names = Array("-l", "--local"))
  private var local: Boolean = false
  @picocli.CommandLine.Option(names = Array("--spark-local-dir"))
  private var sparkLocalDir: String = "/tmp/"
  @picocli.CommandLine.Option(names = Array("-c", "--cache", "--cache-warmer"))
  private var cache_warmer: Int = 5
  @picocli.CommandLine.Option(names = Array("-r", "--runs", "--nr-runs"))
  private var nr_runs: Int = 30
  @picocli.CommandLine.Option(names = Array("--log-dir"))
  private var log_dir: Path = Paths.get("", "output")
  @picocli.CommandLine.Option(names = Array("--log-file"))
  private var log_file: String = "exp" + ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".log"
  @picocli.CommandLine.Option(names = Array("--append"))
  private var append: Boolean = false
  @picocli.CommandLine.Option(names = Array("--batch-size"))
  private var batch_size: String = ""
  @picocli.CommandLine.Option(names = Array("--distribution-algorithm"))
  private var distribute_algorithm: String = ""
  @picocli.CommandLine.Option(names = Array("--sorting-algorithm"))
  private var sorting_algorithm: String = ""
  @picocli.CommandLine.Option(names = Array("--parquet-reader"))
  private var parquet_reader: String = ""
  @picocli.CommandLine.Option(names = Array("--bucketseach-parallel"))
  private var bucketsearch_parallel: Boolean = false
  @picocli.CommandLine.Option(names = Array("--timsort-runs"))
  private var timsort_runs: Int = -1
  @picocli.CommandLine.Option(names = Array("--report-directory"))
  private var report_directory: String = ""
  @picocli.CommandLine.Option(names = Array("--only-vanilla"))
  private var only_vanilla: Boolean = false
  @picocli.CommandLine.Option(names = Array("--only-cas"))
  private var only_cas: Boolean = false


  private val num_part: Int = 10

  override def call(): Unit = {
    // User input checks
    if (data_dir == "" && data_file == "") {
      println("[ERROR] please provide a directory or file")
      exit(1)
    }
    if (data_dir != "" && data_file != "") {
      println("[ERROR] please provide either a directory or file")
      exit(1)
    }
    if (data_file != "" && !scala.reflect.io.File(data_file).exists) {
      println(s"[ERROR] $data_file does not exist")
      exit(1)
    }
    if (data_dir != "" && !Directory(data_dir).exists) {
      println(s"[ERROR] $data_dir does not exist")
      exit(1)
    }

    try {
      val start: Long = System.nanoTime()
      val builder = SparkSession.builder().appName("Evaluator")
//        .config("spark.memory.offHeap.enabled", "true")
//        .config("spark.memory.offHeap.size", "20g")
//        .config("spark.local.dir", sparkLocalDir)
//        .config("spark.eventLog.enabled", "true")
        .withExtensions(ArrowSparkExtensionWrapper.injectAll)
      if (local)
        builder.master("local[4]")
      if (batch_size != "")
        builder.config(ArrowConf.NATIVE_SCANNER_BATCHSIZE.key, batch_size)
      if (report_directory != "")
        builder.config(ArrowConf.ARROWRDD_REPORT_DIRECTORY.key, report_directory)
      if (distribute_algorithm != "")
        builder.config(ArrowConf.DISTRIBUTOR_ALGORITHM.key, distribute_algorithm)
      if (sorting_algorithm != "")
        builder.config(ArrowConf.SORTING_ALGORITHM.key, sorting_algorithm)
      if (parquet_reader != "")
        builder.config(ArrowConf.PARQUET_READER.key, parquet_reader)
      builder.config(ArrowConf.BUCKETSEARCH_PARALLEL.key, bucketsearch_parallel)
      if (timsort_runs != -1)
        builder.config(ArrowConf.TIMSORT_RUN.key, timsort_runs)
      val spark = builder.getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      /**
       * Warm up cache with a simple (vanilla) program
       */
      0 until cache_warmer foreach { _ =>
        val temp_view = "temp"
        spark.sparkContext.parallelize(Range(0, 100 * 1000, 1), num_part).sum()
        if (data_dir != "")
          spark.read.format("parquet").option("mergeSchema", "true").option("dbtable", temp_view)
            .load(Paths.get(data_dir).resolve("*").toString)
            .createOrReplaceTempView(temp_view)
        else if (data_file != "")
          spark.read.parquet(data_file).createOrReplaceTempView(temp_view)
      }

      /**
       * Setup Log file
       */
      new File(log_dir.toAbsolutePath.toString).mkdir() // create directory if it does not exist yet
      val write_file = log_dir.resolve(log_file)
      val fw = new FileWriter(write_file.toFile, append)
      fw.write(s"# Experiment repeated $nr_runs times, with running times in seconds\n")
      if (data_file != "")
        fw.write(s"# File used: $data_file\n")
      else if (data_dir != "")
        fw.write(s"# Directory used: $data_dir\n")
      fw.flush()

      /**
       * Run the actual experiments
       */
      0 until nr_runs foreach { _ =>
        if (data_dir != "")
          EvaluationSuite.sort(spark, fw, Directory(data_dir), onlyCas = only_cas, onlyVanilla = only_vanilla)
        else if (data_file != "")
          EvaluationSuite.sort(spark, fw, data_file)
      }

      fw.close()
      println(s"Experiment took %04.3f seconds".format((System.nanoTime()-start)/1e9d))
    } catch {
      case e: Throwable => e.printStackTrace(); exit(1)
    }
  }
}
