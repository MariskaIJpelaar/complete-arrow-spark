package org.apache.arrow.util.vector.read

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{IntVector, ValueVector, VectorSchemaRoot}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.api.{GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.vectorized.ArrowColumnVector

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * Note: currently only supports INT32 types
 * Note: according to https://arrow.apache.org/docs/java/memory.html#bufferallocator,
 * each application should "create one RootAllocator at the start of the program,
 * and use it through the BufferAllocator interface"
 * So, we ask the caller for it
 * Implementation of the Iterator is according to the ParquetToArrowConverter,
 * this converter in turn is according to:
 * https://gist.github.com/animeshtrivedi/76de64f9dab1453958e1d4f8eca1605f */
private class DumpConverter extends PrimitiveConverter {
  final override def asGroupConverter = new DumpGroupConverter
}

private class DumpGroupConverter extends GroupConverter {
  final def start(): Unit = {}
  final def end(): Unit = {}
  final def getConverter(fieldIndex: Int) = new DumpConverter
}


class ParquetReaderIterator(protected val file: PartitionedFile, protected val allocator: BufferAllocator) extends Iterator[ArrowColumnarBatchRow] {
  if (file.length > Integer.MAX_VALUE)
    throw new RuntimeException("[IntegerParquetReaderIterator] Partition is too large")


  /** with help from: https://blog.actorsfit.com/a?ID=01000-cf624b9b-13ce-4228-9acb-29b722aec266 */
  private lazy val reader = {
    // make sure the reader conforms to our limits :)
    val options = ParquetReadOptions.builder()
      .withMaxAllocationInBytes(Integer.MAX_VALUE)
      .withMetadataFilter(ParquetMetadataConverter.range(file.start, file.start+file.length)).build()
    ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.filePath), new Configuration()), options)
  }
  private var pageReadStore = reader.readNextRowGroup()
  private lazy val parquetSchema = reader.getFileMetaData.getSchema
  private lazy val schema: Schema = {
    val converter = new SchemaConverter()
    converter.fromParquet(parquetSchema).getArrowSchema
  }
  private lazy val colDesc = parquetSchema.getColumns
  var j = 0

  override def hasNext: Boolean = pageReadStore != null

  override def next(): ArrowColumnarBatchRow = {
    if (!hasNext)
      throw new RuntimeException("[ParquetReaderIterator] has no next")

    val colReader = new ColumnReadStoreImpl(pageReadStore, new DumpGroupConverter(),
      parquetSchema, reader.getFileMetaData.getCreatedBy)

    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val vectors = vectorSchemaRoot.getFieldVectors

    if (pageReadStore.getRowCount > Integer.MAX_VALUE)
      throw new RuntimeException(s"[ParquetReaderIterator] file '${file.filePath}' has too many rows" )

    val rows = pageReadStore.getRowCount.toInt

    0 until colDesc.size() foreach { i =>
      val col = colDesc.get(i)
      val cr = colReader.getColumnReader(col)
      val dmax = col.getMaxDefinitionLevel
      if (col.getPrimitiveType.getPrimitiveTypeName != PrimitiveTypeName.INT32)
        throw new RuntimeException("[IntegerParquetReaderIterator] may only consist of INT32 types")

      val vector = vectors.get(i).asInstanceOf[IntVector]
      vector.setInitialCapacity(rows)
      vector.allocateNew()// required after allocateNew()?
      0 until rows foreach { row =>
        if (cr.getCurrentDefinitionLevel == dmax) vector.setSafe(row, cr.getInteger)
        else vector.setNull(row)
        cr.consume()
      }
      vector.setValueCount(rows)
    }
    pageReadStore = reader.readNextRowGroup()

    vectorSchemaRoot.setRowCount(rows)
    val data = vectorSchemaRoot.getFieldVectors.asInstanceOf[java.util.List[ValueVector]].asScala.toArray
    /** transfer ownership */
    val transferred = data.map { vector =>
      val tp = vector.getTransferPair(vector.getAllocator
        .newChildAllocator(s"ParquetReaderIterator::transfer::$j::${vector.getName}", 0, Integer.MAX_VALUE))
      tp.transfer()
      new ArrowColumnVector(tp.getTo)
    }
    val batch = new ArrowColumnarBatchRow(transferred, rows)
    vectorSchemaRoot.close()
    j += 1
    batch
  }
}

