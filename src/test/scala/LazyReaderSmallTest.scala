import org.apache.arrow.vector.{IntVector, ValueVector}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.column.{ColumnDataFrame, ColumnDataFrameReader, TColumn}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.SpArrowExtensionWrapper
import org.scalatest.funsuite.AnyFunSuite
import utils.{MultiIterator, ParquetWriter}

import java.io.File
import java.util
import java.util.{Collections, Comparator}
import scala.language.postfixOps
import scala.reflect.io.Directory

class LazyReaderSmallTest extends AnyFunSuite {
  private val size = 100 * 1000 // 100k
  private val num_files = 10
  private val directory_name = "data/numbers"
  private val schema = SchemaBuilder.builder("simple_double_column")
    .record("record").fields().requiredInt("numA").requiredInt("numB").endRecord()

  private val rnd = scala.util.Random
  def generateRandomNumber(start: Int = 0, end: Int = Integer.MAX_VALUE-1) : Int = {
    start + rnd.nextInt( (end-start) + 1)
  }

  private val num_cols = 2
  case class Row(var colA: Int, var colB: Int)
  private val table : util.List[Row] = new util.ArrayList[Row]()

  def generateParquets(): Unit = {
    val directory = new Directory(new File(directory_name))
    assert(!directory.exists || directory.deleteRecursively())

    val records: util.List[GenericData.Record] = new util.ArrayList[GenericData.Record]()
    0 until size foreach { _ =>
      val numA: Int = generateRandomNumber()
      val numB: Int = generateRandomNumber()
      table.add(Row(numA, numB))
      records.add(new GenericRecordBuilder(schema).set("numA", numA).set("numB", numB).build())
    }

    val recordsSize = size / num_files
    val writeables: util.List[ParquetWriter.Writable] = new util.ArrayList[ParquetWriter.Writable]()
    0 until num_files foreach { i =>
      writeables.add(new ParquetWriter.Writable(
        HadoopOutputFile.fromPath(new Path(directory.path, s"file$i.parquet"), new Configuration()),
        records.subList(i*recordsSize, (i+1)*recordsSize)
    ))}

    ParquetWriter.write_batch(schema, writeables, true)
  }

  def computeAnswer() : Unit = {
    // from: https://stackoverflow.com/questions/4805606/how-to-sort-by-two-fields-in-java
    Collections.sort(table, new Comparator[Row]() {
      override def compare(t: Row, t1: Row): Int = {
        val iComp = t.colA.compareTo(t1.colA)
        if (iComp != 0) iComp else t.colB.compareTo(t1.colB)
      }
    })
  }

  def checkFirst(answer: TColumn): Unit = {
    assert(answer.length == table.size())

    0 until answer.length foreach { i =>
      val ans = answer.get(i)
      assert(ans.isDefined)
      val col = table.get(i).colA
      assert(ans.get == col)
    }


  }

  def checkAnswer(answer: Array[ValueVector]) : Unit = {
    assert(answer.length == num_cols)

    val iterators: util.List[util.Iterator[Object]] = new util.ArrayList[util.Iterator[Object]]()
    iterators.add(table.iterator().asInstanceOf[util.Iterator[Object]])
    answer.foreach( vec => iterators.add(vec.asInstanceOf[IntVector].iterator().asInstanceOf[util.Iterator[Object]]))

    val zipper: MultiIterator = new MultiIterator(iterators)
    while (zipper.hasNext) {
      val next = zipper.next()
      assert(next.size() == num_cols+1)
      val row: Row = next.get(0).asInstanceOf[Row]
      val cols = next.subList(1, next.size())
      0 until num_cols foreach { i => assert(row.productElement(i).equals(cols.get(i)))}
    }
  }

  test("Performing the ColumnarSort on a simple Dataset using Lazy Reading") {
    // Generate Dataset
    generateParquets()
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    // Compute answer
    computeAnswer()

    val spark = SparkSession.builder().appName("LazyReaderSmallTest")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "3048576")
      .master("local")
      .withExtensions(SpArrowExtensionWrapper.injectArrowFileSourceStrategy).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val temp_list = new util.ArrayList[StructField](2)
    temp_list.add(StructField("numA", IntegerType, nullable = true))
    temp_list.add(StructField("numB", IntegerType, nullable = true))
    val temp = RowEncoder(StructType(temp_list))

    // Construct DataFrame
    // TODO: make a dataset (ArrowColumnEncoder) specifically for ArrowColumns
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain("formatted")
    val plan = df.queryExecution.executedPlan.execute()
    val firstOfPlan = plan.first()
    checkFirst(df.first())
    // Perform ColumnarSort
    df.sort("numA", "numB")
    df.explain("formatted")

    // Check if result is equal to our computed table
    checkAnswer(df.collect().asInstanceOf[Array[ValueVector]])

    directory.deleteRecursively()
  }


}
