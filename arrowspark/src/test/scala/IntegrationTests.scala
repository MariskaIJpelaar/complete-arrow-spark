import _root_.utils.ParquetWriter
import io.netty.util.internal.PlatformDependent
import nl.liacs.mijpelaar.utils.RandomUtils
import org.apache.arrow.memory.DefaultAllocationManagerOption
import org.apache.arrow.memory.DefaultAllocationManagerOption.AllocationManagerType
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.column._
import org.apache.spark.sql.util.ArrowSparkExtensionWrapper
import org.apache.spark.sql.{SparkSession, column}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util
import java.util.{Collections, Comparator}
import scala.language.postfixOps
import scala.reflect.io.Directory


class IntegrationTests extends AnyFunSuite {
  private val default_size = 100 * 1000 // 100k
  private val num_files = 10
  private val directory_name = "data/numbers"
  private val schema = SchemaBuilder.builder("simple_double_column")
    .record("record").fields().requiredInt("numA").requiredInt("numB").endRecord()

  private val random = new RandomUtils(scala.util.Random)

  private val num_cols = 2
  case class Row(var colA: Int, var colB: Int)

  def generateParquets(key: Int => Int, randomValue: Boolean, size: Int = default_size): util.List[Row] = {
    try {
      println(s"before generate: ${PlatformDependent.usedDirectMemory()}")
      val directory = new Directory(new File(directory_name))
      assert(!directory.exists || directory.deleteRecursively())

      val table: util.List[Row] = new util.ArrayList[Row]()
      val records: util.List[GenericData.Record] = new util.ArrayList[GenericData.Record]()
      0 until size foreach { i =>
        val numA: Int = key(i)
        val numB: Int = if (randomValue) random.generateRandomNumber() else i*2 + 1
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


      table
    } finally {
      ParquetWriter.close()
      println(s"after close: ${PlatformDependent.usedDirectMemory()}")
    }
  }

  def generateSpark(): SparkSession = {
    val spark = SparkSession.builder().appName("IntegrationTests")
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
      assert(valOne.isDefined)
      assert(valTwo.isDefined)
      (valOne.get, valTwo.get) match {
        case (numOne: Int, numTwo: Int) => numTwo == numOne + 1
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
      assert(valOne.isDefined)
      assert(valTwo.isDefined)
      (valOne.get, valTwo.get) match {
        case (numOne: Int, numTwo: Int) =>
          val rowIndex = numOne
          assert( rowIndex >= 0 && rowIndex < size )
          val rowValue = table.get(rowIndex).colB
          numTwo == rowValue
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
        assert(value.isDefined)
        value.get match {
          case numValue: Int =>
            assertResult(table.get(rowIndex).productElement(colIndex))(numValue)
        }
      }
    }
  }


  test("Lazy read first row of simple Dataset with ascending numbers through the RDD") {
    System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, AllocationManagerType.Netty.name())
    column.AllocationManager.reset()
    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    val first = df.queryExecution.executedPlan.execute().first().asInstanceOf[ArrowColumnarBatchRow]
    checkFirstNonRandom(first)
    val root = first.allocator.getRoot
    first.close()
    root.close()

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
    println(s"after run: ${PlatformDependent.usedDirectMemory()}")
  }

  test("Lazy read first row of simple Dataset with ascending numbers through ColumnDataFrame") {
    column.AllocationManager.reset()
    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)

    checkFirstNonRandom(df.first())

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Lazy read simple Dataset with ascending numbers through ColumnDataFrame") {
    column.AllocationManager.reset()
    generateParquets(key = i => i*2, randomValue = false)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    checkAnswerNonRandom(df.collect())

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Lazy read simple Dataset with ascending key-column and random value-column through ColumnDataFrame") {
    column.AllocationManager.reset()
    val table = generateParquets(key = i => i, randomValue = true)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)
    df.explain(true)
    checkAnswer(table, df.collect())

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing local ColumnarSort on a simple, very small, non-random, Dataset using lazy Reading") {
    column.AllocationManager.reset()
    val size = 10
    val table = generateParquets(key = i => i*2, randomValue = false, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sortWithinPartitions("numA", "numB")
    new_df.explain(true)

    new_df.collect()

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, very small, non-random, Dataset using lazy Reading") {
    column.AllocationManager.reset()
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

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing single-column ColumnarSort on a simple, random, Dataset using Lazy Reading") {
    column.AllocationManager.reset()
    // Generate Dataset
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = true)
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

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, semi-random, Dataset using Lazy Reading") {
    column.AllocationManager.reset()
    // Generate Dataset
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = false)
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

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, Dataset using Lazy Reading") {
    column.AllocationManager.reset()
    // Generate Dataset
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = true)
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

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, somewhat larger Dataset using Lazy Reading") {
    column.AllocationManager.reset()
    // Generate Dataset
    val size = default_size * 15
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = true, size = size)
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

    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, somewhat larger Dataset using Lazy Reading, memory-aware") {
    column.AllocationManager.reset()
    // Generate Dataset
    val size = default_size * 15
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = true, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
    new_df.explain(true)

    val rdd = new_df.queryExecution.executedPlan.execute()
    val func: Iterator[InternalRow] => Int = { case iter: Iterator[ArrowColumnarBatchRow] =>
      iter.map { batch =>
          try {
            batch.numRows
          } finally {
            batch.close()
          }
      }.sum
    }
    spark.sparkContext.runJob(rdd, func).sum
    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()
    directory.deleteRecursively()
  }

  def runTest(): Unit = {
    column.AllocationManager.reset()
    // Generate Dataset
    val size = default_size * 15
    val table = generateParquets(key = _ => random.generateRandomNumber(0, 10), randomValue = true, size = size)
    val directory = new Directory(new File(directory_name))
    assert(directory.exists)

    val spark = generateSpark()

    // Construct DataFrame
    val df: ColumnDataFrame = new ColumnDataFrameReader(spark).format("utils.SimpleArrowFileFormat").loadDF(directory.path)

    // Perform ColumnarSort
    val new_df = df.sort("numA", "numB")
//    val new_df = df

    val rdd = new_df.queryExecution.executedPlan.execute()
    val func: Iterator[InternalRow] => Int = { case iter: Iterator[ArrowColumnarBatchRow] =>
      iter.map { batch =>
        try {
          batch.numRows
        } finally {
          batch.close()
        }
      }.sum
    }
    spark.sparkContext.runJob(rdd, func).sum
    assert(column.AllocationManager.isCleaned)
    column.AllocationManager.reset()

    directory.deleteRecursively()
  }

  test("Performing ColumnarSort on a simple, random, somewhat larger Dataset using Lazy Reading, memory-test") {
    System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, AllocationManagerType.Netty.name())

    var limit: Option[Long] = None
    var current: Long = PlatformDependent.usedDirectMemory()
    val numRuns = 5

    0 until numRuns foreach { _ =>
      runTest()

      current = PlatformDependent.usedDirectMemory()
//      assert(limit.forall( lim => lim >= current))
      println(current)
      if (limit.isEmpty) limit = Option(current)
    }
  }
}
