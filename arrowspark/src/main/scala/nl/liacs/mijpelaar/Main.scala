package nl.liacs.mijpelaar

import nl.liacs.mijpelaar.evaluation.EvaluationSuite
import org.apache.spark.sql.SparkSession
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
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "20g")
//        .config("spark.local.dir", sparkLocalDir)
//        .config("spark.eventLog.enabled", "true")
        .withExtensions(ArrowSparkExtensionWrapper.injectAll)
      if (local)
        builder.master("local[4]")
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
      // Files.write(write_file, "".getBytes(StandardCharsets.UTF_8)) // clear file
      val fw = new FileWriter(write_file.toFile) 
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
          EvaluationSuite.sort(spark, fw, Directory(data_dir))
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
