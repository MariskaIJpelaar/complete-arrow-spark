package org.apache.spark.shuffle
import org.apache.spark.{Partition, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.api.ShuffleExecutorComponents
import org.apache.spark.shuffle.sort.{ArrowBypassMergeSortShuffleWriter, BypassMergeSortShuffleHandle}
import scala.collection.JavaConverters._

private[spark] class ArrowShuffleWriteProcessor extends ShuffleWriteProcessor {

  /** copied directly from: org.apache.spark.shuffle.sort.SortShuffleManager.loadShuffleExecutorComponents */
  private def loadShuffleExecutorComponents(conf: SparkConf): ShuffleExecutorComponents = {
    val executorComponents = ShuffleDataIOUtils.loadShuffleDataIO(conf).executor()
    val extraConfigs = conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX)
      .toMap
    executorComponents.initializeExecutor(
      conf.getAppId,
      SparkEnv.get.executorId,
      extraConfigs.asJava)
    executorComponents
  }

  /** Copied from ShuffleWriteProcessor.write() and adapted to get an ArrowBypassMergeSortShuffleWriter */
  override def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {

    // TODO: do we need to close our serializer here somewhere?
    var writer: ShuffleWriter[Any, Any] = null
    try {
      val manager = SparkEnv.get.shuffleManager
      val env = SparkEnv.get
      writer = new ArrowBypassMergeSortShuffleWriter(
        env.blockManager,
        dep.shuffleHandle.asInstanceOf[BypassMergeSortShuffleHandle[Any, Any]],
        mapId,
        env.conf,
        createMetricsReporter(context),
        loadShuffleExecutorComponents(env.conf)
      )
      writer.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      val mapStatus = writer.stop(success = true)
      if (mapStatus.isDefined) {
        // Check if sufficient shuffle mergers are available now for the ShuffleMapTask to push
        if (dep.shuffleMergeAllowed && dep.getMergerLocs.isEmpty) {
          val mapOutputTracker = SparkEnv.get.mapOutputTracker
          val mergerLocs =
            mapOutputTracker.getShufflePushMergerLocations(dep.shuffleId)
          if (mergerLocs.nonEmpty) {
            dep.setMergerLocs(mergerLocs)
          }
        }
        // Initiate shuffle push process if push based shuffle is enabled
        // The map task only takes care of converting the shuffle data file into multiple
        // block push requests. It delegates pushing the blocks to a different thread-pool -
        // ShuffleBlockPusher.BLOCK_PUSHER_POOL.
        if (!dep.shuffleMergeFinalized) {
          manager.shuffleBlockResolver match {
            case resolver: IndexShuffleBlockResolver =>
              logInfo(s"Shuffle merge enabled with ${dep.getMergerLocs.size} merger locations " +
                s" for stage ${context.stageId()} with shuffle ID ${dep.shuffleId}")
              logDebug(s"Starting pushing blocks for the task ${context.taskAttemptId()}")
              val dataFile = resolver.getDataFile(dep.shuffleId, mapId)
              new ShuffleBlockPusher(SparkEnv.get.conf)
                .initiateBlockPush(dataFile, writer.getPartitionLengths(), dep, partition.index)
            case _ =>
          }
        }
      }
      mapStatus.get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
