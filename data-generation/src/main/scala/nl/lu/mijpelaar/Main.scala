package nl.lu.mijpelaar

import nl.lu.mijpelaar.Main.toRow
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import picocli.CommandLine

import java.io.File
import java.util.concurrent.Callable
import scala.reflect.io.Directory
import scala.sys.exit

object Main {
  def main(args: Array[String]): Unit = {
    new CommandLine(new Main()).execute(args: _*)
  }

  private val seed = 19971128
  private val rnd = new scala.util.Random(seed)
  private val generateRandomNumber = (start: Int, end: Int) => start + rnd.nextInt( (end-start) + 1)
  val toRow: Int => Row = (index: Int) => Row(generateRandomNumber(0, 9), generateRandomNumber(0, Integer.MAX_VALUE-1))
}

class Main extends Callable[Unit] {
  @picocli.CommandLine.Option(names = Array("-a", "--amount"))
  private var amount: Int = 100
  @picocli.CommandLine.Option(names = Array("-l", "--local"))
  private var local: Boolean = false
  @picocli.CommandLine.Option(names = Array("-p", "--path"))
  private var path: String = "data/generated/generated"
  @picocli.CommandLine.Option(names = Array("--spark-local-dir"))
  private var sparkLocalDir: String = "/tmp/"

  override def call(): Unit = {
    try {
      val dir = new File(path)
      if (dir.exists()) {
        println(s"[ERROR] Directory $path already exists")
        exit(1)
      }

      val sparkBuilder = SparkSession.builder.appName("generator")
        .config("spark.local.dir", sparkLocalDir)
      if (local)
        sparkBuilder.config("spark.master", "local[*]")
      val spark = sparkBuilder.getOrCreate()


      /**
       * 2022-05-03:
       * according to: https://spark.apache.org/docs/2.1.0/programming-guide.html#parallelized-collections
       * "One important parameter for parallel collections is the number of partitions
       * to cut the dataset into. Spark will run one task for each partition of the
       * cluster. Typically you want 2-4 partitions for each CPU in your cluster.
       * Normally, Spark tries to set the number of partitions automatically based on
       * your cluster. However, you can also set it manually by passing it as a second
       * parameter to parallelize (e.g. sc.parallelize(data, 10))."
       *
       * Thus, we keep the second argument as default as we trust Spark :)
       */
      val start: Long = System.nanoTime()
      val intRDD = spark.sparkContext.parallelize(Range(0, amount, 1)).map(toRow)
      val schema = new StructType()
        .add(StructField("numA", IntegerType, nullable = false))
        .add(StructField("numB", IntegerType, nullable = false))
      spark.createDataFrame(intRDD, schema).write.parquet(path)
      println(s"Generating and writing took %04.3f seconds".format((System.nanoTime()-start)/1e9d))


      /**
       * We have no influence over the parquet-filename, so we rename it ourselves...
       */
      if (dir.exists && dir.isDirectory) {
        val files = dir.listFiles.filter(_.isFile).filter(file => FilenameUtils.getExtension(file.getName) == "parquet").toList
        files.zipWithIndex.foreach { case (file, idx) =>
          file.renameTo(new File(path + s"_$idx.parquet"))
        }
        new Directory(dir).deleteRecursively()
      } else {
        println("[ERROR] Something went wrong with generating the file(s)")
        exit(1)
      }
    } catch {
      case e: Throwable => e.printStackTrace(); exit(1)
    }
  }
}