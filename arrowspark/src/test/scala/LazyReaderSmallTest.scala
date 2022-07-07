import io.netty.util.internal.PlatformDependent
import org.apache.arrow.memory.DefaultAllocationManagerOption
import org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.column.{ArrowColumnarBatchRow, ColumnBatch, ColumnDataFrame, ColumnDataFrameReader, TColumn}
import org.apache.spark.sql.util.ArrowSparkExtensionWrapper
import org.apache.spark.sql.{SparkSession, column}
import org.scalatest.funsuite.AnyFunSuite
import utils.ParquetWriter

import java.io.File
import java.util
import java.util.{Collections, Comparator}
import scala.language.postfixOps
import scala.reflect.io.Directory

class LazyReaderSmallTest extends AnyFunSuite {
  private val default_size = 100 * 1000 // 100k
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

  def generateParquets(key: Int => Int, randomValue: Boolean, size: Int = default_size): util.List[Row] = {
    val directory = new Directory(new File(directory_name))
    assert(!directory.exists || directory.deleteRecursively())

    val table: util.List[Row] = new util.ArrayList[Row]()
    val records: util.List[GenericData.Record] = new util.ArrayList[GenericData.Record]()
    0 until size foreach { i =>
      val numA: Int = key(i)
      val numB: Int = if (randomValue) generateRandomNumber() else i*2 + 1
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
    ParquetWriter.close()
    table
  }

  def generateSpark(): SparkSession = {
    val spark = SparkSession.builder().appName("LazyReaderSmallTest")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "1GB")
      .master("local[4]")
      .withExtensions(ArrowSparkExtensionWrapper.injectAll).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def computeAnswer(table: util.List[Row], onColA: Boolean = true): Unit = {
    // from: https://stackoverflow.com/questions/4805606/how-to-sort-by-two-fields-in-java
    Collections.sort(table, new Comparator[Row]() {
      override def compare(t: Row, t1: Row): Int = {
        if (!onColA)
          return t.colB.compareTo(t1.colB)
        val iComp = t.colA.compareTo(t1.colA)
        if (iComp != 0) iComp else t.colB.compareTo(t1.colB)
      }
    })
  }

  def checkFirstNonRandom(answer: ArrowColumnarBatchRow): Unit = {
    val val_one = answer.getArray(0).array(0)
    val val_two = answer.getArray(num_cols-1).array(0)
    assert(val_one.isInstanceOf[Int])
    assert(val_two.isInstanceOf[Int])
    assert(val_one.asInstanceOf[Int] + 1 == val_two.asInstanceOf[Int])
  }

  def checkFirstNonRandom(answer: ColumnBatch): Unit = {
    val ansRow = answer.getRow(0)
    assert(ansRow.exists( row => {
      val valOne = row.get(0)
      val valTwo = row.get(num_cols-1)
      (valOne, valTwo) match {
        case (numOne: Option[Int], numTwo: Option[Int]) =>
          numOne.exists(one => numTwo.isDefined && numTwo.get.equals( one + 1) )
        case _ => false
      }
    }))
  }

  def checkAnswerNonRandom(answer: Array[ColumnBatch], size: Int = default_size): Unit = {
    val cols = TColumn.fromBatches(answer)
    assert(cols.length == num_cols)

    val colOne = cols(0)
    val colTwo = cols(num_cols - 1)
    assert(0 until size forall { index =>
      val valOne = colOne.get(index)
      val valTwo = colTwo.get(index)
      (valOne, valTwo) match {
        case (numOne: Option[Int], numTwo: Option[Int]) =>
          numOne.exists(one => numTwo.isDefined && numTwo.get.equals( one + 1) )
        case _ => false
      }
    })
  }

  def checkAnswer(table: util.List[Row], answer: Array[ColumnBatch], size: Int = default_size): Unit = {
    val cols = TColumn.fromBatches(answer)
    assert(cols.length == num_cols)

    val colOne = cols(0)
    val colTwo = cols(num_cols -1)
    assert(colOne.length == size)
    assert(colTwo.length == size)

    0 until size foreach { index =>
      val valOne = colOne.get(index)
      val valTwo = colTwo.get(index)
      (valOne, valTwo) match {
        case (numOne: Option[Int], numTwo: Option[Int]) =>
          assert(numOne.isDefined)
          val rowIndex = numOne.get
          assert( rowIndex >= 0 && rowIndex < size )
          val rowValue = table.get(rowIndex).colB
          numTwo.exists( num => num.equals(rowValue) )
        case _ => false
      }
    }
  }

  def checkSorted(table: util.List[Row], answer: Array[ColumnBatch], size: Int = default_size, colNrs: Range = 0 until num_cols): Unit = {
    val cols = TColumn.fromBatches(answer)

    colNrs foreach { colIndex =>
      val col = cols(colIndex)
      assert(col.length == size)
      0 until size foreach { rowIndex =>
        val value = col.get(rowIndex)
        value match {
          case numValue: Option[Int] =>
            assert(numValue.isDefined)
            assertResult(table.get(rowIndex).productElement(colIndex))(numValue.get)
        }
      }
    }
  }


  test("Lazy read first row of simple Dataset with ascending numbers through the RDD") {
    System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, AllocationManagerType.Netty.name())

    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    val first = df.queryExecution.executedPlan.execute().first().asInstanceOf[ArrowColumnarBatchRow]
    checkFirstNonRandom(first)
    first.close()
    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Lazy read first row of simple Dataset with ascending numbers through ColumnDataFrame") {
    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)

    checkFirstNonRandom(df.first())

    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Lazy read simple Dataset with ascending numbers through ColumnDataFrame") {
    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    checkAnswerNonRandom(df.collect())

    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Lazy read simple Dataset with ascending key-column and random value-column through ColumnDataFrame") {
    val table = generateParquets(key = i => i, randomValue = true)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    checkAnswer(table, df.collect())

    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, very small, non-random, Dataset using lazy Reading") {
    val size = 10
    val table = generateParquets(key = i => i*2, randomValue = false, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    // Compute answer
    computeAnswer(table)

    // Check if result is equal to our computed table
    checkSorted(table, new_df.collect(), size = size)

    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Performing single-column ColumnarSort on a simple, random, Dataset using Lazy Reading") {
    // Generate Dataset
    val table = generateParquets(key = _ => generateRandomNumber(0, 10), randomValue = true)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numB")
    new_df.explain(true)

    // Compute answer
    computeAnswer(table, onColA = false)

    // Check if result is equal to our computed table
    checkSorted(table, new_df.collect(), colNrs = 1 until num_cols)

    column.resetRootAllocator()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, semi-random, Dataset using Lazy Reading") {
    // Generate Dataset
    val table = generateParquets(key = _ => generateRandomNumber(0, 10), randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    // Compute answer
    computeAnswer(table)

    // Check if result is equal to our computed table
    checkSorted(table, new_df.collect())

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, Dataset using Lazy Reading") {
    // Generate Dataset
    val table = generateParquets(key = _ => generateRandomNumber(0, 10), randomValue = true)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    // Compute answer
    computeAnswer(table)

    // Check if result is equal to our computed table
    checkSorted(table, new_df.collect())

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, somewhat larger Dataset using Lazy Reading") {
    // Generate Dataset
    val size = default_size * 15
    val table = generateParquets(key = _ => generateRandomNumber(0, 10), randomValue = true, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    // Compute answer
    computeAnswer(table)

    // Check if result is equal to our computed table
    checkSorted(table, new_df.collect(), size = size)

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, somewhat larger Dataset using Lazy Reading, on-partition check") {
    System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, AllocationManagerType.Netty.name())

    // Generate Dataset
    val size = default_size * 15
    val table = generateParquets(key = _ => generateRandomNumber(0, 10), randomValue = true, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    println(PlatformDependent.usedDirectMemory())

    val rdd = new_df.queryExecution.executedPlan.execute()
    rdd.count()
    rdd.foreach( batch => batch.asInstanceOf[ArrowColumnarBatchRow].close())

    println(PlatformDependent.usedDirectMemory())

    directory.deleteRecursively()
  }
}
