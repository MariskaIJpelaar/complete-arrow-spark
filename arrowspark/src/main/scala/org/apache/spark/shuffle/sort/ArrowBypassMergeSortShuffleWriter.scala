package org.apache.spark.shuffle.sort

import com.google.common.io.Closeables
import nl.liacs.mijpelaar.utils.Resources
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.SHUFFLE_FILE_BUFFER_SIZE
import org.apache.spark.network.shuffle.checksum.ShuffleChecksumHelper
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShufflePartitionWriter, WritableByteChannelWrapper}
import org.apache.spark.shuffle.checksum.ShuffleChecksumSupport
import org.apache.spark.shuffle.{ShuffleWriteMetricsReporter, ShuffleWriter}
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.storage.{BlockManager, DiskBlockObjectWriter, FileSegment}
import org.apache.spark.util.Utils
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream, OutputStream}

/** Inspiration from: org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriter */
class ArrowBypassMergeSortShuffleWriter[K, V](
                                               private val blockManager: BlockManager,
                                               handle: BypassMergeSortShuffleHandle[K, V],
                                               private val mapId: Long,
                                               conf: SparkConf,
                                               private val writeMetrics: ShuffleWriteMetricsReporter,
                                               private val shuffleExecutorComponents: ShuffleExecutorComponents
                                             ) extends ShuffleWriter[K, V] with ShuffleChecksumSupport {

  /** member variables copied from BypassMergeSortShuffleWriter */
  private val logger = LoggerFactory.getLogger(classOf[BypassMergeSortShuffleWriter[_, _]])
  private val fileBufferSize = conf.get(SHUFFLE_FILE_BUFFER_SIZE).toInt * 1024;
  private val transferToEnabled = conf.getBoolean("spark.file.transferTo", defaultValue = true)
  private val shuffleId = handle.dependency.shuffleId
  private val partitioner = handle.dependency.partitioner
  private val numPartitions = partitioner.numPartitions
  private val serializer = handle.dependency.serializer
  /** Checksum calculator for each partition. Empty when shuffle checksum disabled. */
  private val partitionChecksums = createPartitionChecksums(numPartitions, conf)

  /** Array of file writers, one for each partition */
  private var partitionWriters: Option[Array[DiskBlockObjectWriter]] = None
  private var partitionWriterSegments: Option[Array[FileSegment]] = None
  private var mapStatus: Option[MapStatus] = None
  private var partitionLengths: Option[Array[Long]] = None

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private var stopping = false

  /** copied and modified from BypassMergeSortShuffleWriter.writePartitionedDataWithChannel */
  private def writePartitionedDataWithChannel(file: File, outputChannel: WritableByteChannelWrapper): Unit = {
    var copyThrewException = true
    try {
      val in = new FileInputStream(file)
      Resources.autoCloseTry(in.getChannel) { inputChannel =>
       try {
         Utils.copyFileStreamNIO(inputChannel, outputChannel.channel(), 0L, inputChannel.size())
         copyThrewException = false
       } finally {
         Closeables.close(in, copyThrewException)
       }
      }
    } finally {
      Closeables.close(outputChannel, copyThrewException)
    }
  }
  /** copied and modified from BypassMergeSortShuffleWriter.writePartitionedDataWithStream */
  private def writePartitionedDataWithStream(file: File, writer: ShufflePartitionWriter): Unit = {
    var copyThrewException = true
    val in = new FileInputStream(file)
    var outputStream: OutputStream = null
    try {
      outputStream = writer.openStream
      try {
        Utils.copyStream(in, outputStream, closeStreams = false, transferToEnabled = false)
        copyThrewException = false
      } finally Closeables.close(outputStream, copyThrewException)
    } finally Closeables.close(in, copyThrewException)
  }

  /** copied and modified from BypassMergeSortShuffleWriter.writePartitionedData */
  protected def writePartitionedData(mapOutputWriter: ShuffleMapOutputWriter): Array[Long] = {
    partitionWriters foreach { _ =>
      assert(partitionWriterSegments.isDefined)
      val writeStartTime = System.nanoTime()
      try {
        0 until numPartitions foreach { i =>
          val file = partitionWriterSegments.get(i).file
          val writer = mapOutputWriter.getPartitionWriter(i)
          if (file.exists) {
            if (transferToEnabled) {
              // Using WritableByteChannelWrapper to make resource closing consistent between
              // this implementation and UnsafeShuffleWriter.
              val maybeOutputChannel = writer.openChannelWrapper
              if (maybeOutputChannel.isPresent)
                writePartitionedDataWithChannel(file, maybeOutputChannel.get)
              else
                writePartitionedDataWithStream(file, writer)
            } else {
              writePartitionedDataWithStream(file, writer)
            }
            if (!file.delete) logger.error("Unable to delete file for partition {}", i)
          }
        }
      } finally {
       writeMetrics.incWriteTime(System.nanoTime() - writeStartTime)
      }
      partitionWriters = None
    }
    mapOutputWriter.commitAllPartitions(getChecksumValues(partitionChecksums)).getPartitionLengths
  }

  /** copied and modified from BypassMergeSortShuffleWriter.write */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    assert(partitionWriters.isEmpty)
    val mapOutputWriter = shuffleExecutorComponents.createMapOutputWriter(shuffleId, mapId, numPartitions)
    try {
      if (!records.hasNext) {
        partitionLengths =
          Option(mapOutputWriter.commitAllPartitions(ShuffleChecksumHelper.EMPTY_CHECKSUM_VALUE).getPartitionLengths)
        assert(partitionLengths.isDefined)
        mapStatus = Option(MapStatus.apply(blockManager.shuffleServerId, partitionLengths.get, mapId))
        return
      }

      val serInstance = serializer.newInstance()
      val openStartTime = System.nanoTime()
      partitionWriters = Option( new Array[DiskBlockObjectWriter](numPartitions) )
      assert(partitionWriters.isDefined)
      partitionWriterSegments = Option( new Array[FileSegment](numPartitions) )
      assert(partitionWriterSegments.isDefined)

      0 until numPartitions foreach { i =>
        val (blockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()
        val writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics)
        if (partitionChecksums.nonEmpty) writer.setChecksum(partitionChecksums(i))
        partitionWriters.get(i) = writer
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, and can take a long time in aggregate when we open many files, so should be
      // included in the shuffle write time.
      writeMetrics.incWriteTime(System.nanoTime - openStartTime)

      while (records.hasNext) {
        val (key, value) = records.next()
        // In case of ArrowPartition: key = FilePartition, value = ArrowColumnarBatchRow
        (key, value) match {
          case (partitionIds: Array[Int], partition: ArrowColumnarBatchRow) =>
            ArrowColumnarBatchRow.distribute(partition, partitionIds) foreach { case (partitionId, batch) =>
              val combined = ArrowColumnarBatchRow.create(batch.toIterator)
              partitionWriters.get(partitionId).write(partitionId, combined)
            }
          case _ => partitionWriters.get(partitioner.getPartition(key)).write(key, value)
        }
      }

      0 until numPartitions foreach { i =>
        Resources.autoCloseTry(partitionWriters.get(i)) { writer =>
          partitionWriterSegments.get(i) = writer.commitAndGet()
        }
      }

      partitionLengths = Option(writePartitionedData(mapOutputWriter))
      assert(partitionLengths.isDefined)
      mapStatus = Option(MapStatus.apply(blockManager.shuffleServerId, partitionLengths.get, mapId))
    } catch {
      case e: Exception =>
        try {
          mapOutputWriter.abort(e)
        } catch {
          case e2: Exception =>
            logger.error("Failed to abort the writer after failing to write map output.", e2)
            e.addSuppressed(e2)
        }
        throw e
    }
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) return None

    stopping = true
    if (success)
      return mapStatus.orElse { throw new IllegalStateException("Cannot call stop(true) without having called write()") }

    partitionWriters.foreach { writers =>
      try {
       writers foreach { _.closeAndDelete() }
      } finally {
        partitionWriters = None
      }
    }
    None
  }

  override def getPartitionLengths(): Array[Long] = partitionLengths.getOrElse(Array.emptyLongArray)
}
