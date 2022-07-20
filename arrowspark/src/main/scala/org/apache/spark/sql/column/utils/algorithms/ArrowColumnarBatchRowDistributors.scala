package org.apache.spark.sql.column.utils.algorithms

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.search.BucketSearcher
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, ExtendedIndexSorter}
import org.apache.arrow.vector.IntVector
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.column.utils.{ArrowColumnarBatchRowBuilder, ArrowColumnarBatchRowConverters, ArrowColumnarBatchRowTransformers, ArrowColumnarBatchRowUtils}

import scala.collection.mutable

object ArrowColumnarBatchRowDistributors {
  /**
   * @param key ArrowColumnarBatchRow to define distribution for, and close
   * @param rangeBounds ArrowColumnarBatchRow containing ranges on which distribution is based, and close
   * @param sortOrders SortOrders on which distribution is based
   * @return Indices containing the distribution for the key given the rangeBounds and sortOrders
   */
  def bucketDistributor(key: ArrowColumnarBatchRow, rangeBounds: ArrowColumnarBatchRow, sortOrders: Seq[SortOrder]): Array[Int] = {
    Resources.autoCloseTryGet(key) (key => Resources.autoCloseTryGet(rangeBounds) { rangeBounds =>
      if (key.numFields < 1 || rangeBounds.numFields < 1)
        return Array.empty

      val names: Array[String] = sortOrders.map( order => order.child.asInstanceOf[AttributeReference].name ).toArray
      val (keyUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
        ArrowColumnarBatchRowTransformers.getColumns(key.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::keyUnion"), names)
      )
      Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(keyUnion) { keyUnion =>
        val comparator = ArrowColumnarBatchRowUtils.getComparator(keyUnion, sortOrders)
        val (rangeUnion, allocator) = ArrowColumnarBatchRowConverters.toUnionVector(
          ArrowColumnarBatchRowTransformers.getColumns(rangeBounds.copyFromCaller("ArrowColumnarBatchRowDistributors::bucketDistributor::rangeUnion"), names))
        Resources.autoCloseTryGet(allocator) ( _ => Resources.autoCloseTryGet(rangeUnion)
          { rangeUnion => new BucketSearcher(keyUnion, rangeUnion, comparator).distribute() }
        )
      })
    })
  }

  /**
   * Distributes a batch to a mapping (partitionId, Batch), according to the provided Array of Ints
   * @param batch [[ArrowColumnarBatchRow]] to distribute, and close
   * @param partitionIds Array of Ints, which represent which index should go to which partition
   * @param numPartitions Number of partitions to distribute on
   *                      We assume for all ids in partitionIds: 0 <= id < numPartitions
   *                      Additionally, we assume there exists at least one id for each partition in partitionIds
   * @return [[Map]] mapping partitionIds ([[Int]]) to batches ([[ArrowColumnarBatchRow]])
   */
  def distributeBySort(batch: ArrowColumnarBatchRow, partitionIds: Array[Int], numPartitions: Int): Map[Int, ArrowColumnarBatchRow]  = {
    Resources.autoCloseTryGet(batch) { _ =>
      Resources.autoCloseTryGet(createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowDistributors::distributeBySort::partitionIds")) { partitionAllocator =>
        Resources.autoCloseTryGet(new IntVector("partitionIds", partitionAllocator)) { partitions =>
          partitions.setInitialCapacity(batch.numRows)
          partitions.allocateNew()
          0 until batch.numRows foreach (index => partitions.set(index, partitionIds(index)))
          partitions.setValueCount(batch.numRows)

          // TODO: make sure tests pass
          val t1 = System.nanoTime()
          Resources.autoCloseTryGet(createAllocator(batch.allocator.getRoot, "ArrowColumnarBatchRowDistributors::distributeBySort::indices")) { indexAllocator =>
            val (indices, borders) = ExtendedIndexSorter.sortManyDuplicates(partitions, DefaultVectorComparators.createDefaultComparator(partitions), indexAllocator)
            Resources.autoCloseTryGet(indices) { _ =>
              Resources.autoCloseTryGet(ArrowColumnarBatchRowTransformers.applyIndices(batch, indices)) { sorted =>
                val distributed = mutable.Map[Int, ArrowColumnarBatchRow]()
                0 until numPartitions foreach { partitionId =>
                  val end = if (partitionId+1 >= borders.length) sorted.numRows else borders(partitionId+1)
                  distributed(partitionId) = sorted
                    .copyFromCaller(s"ArrowColumnarBatchRowDistributors::distributeBySort::copy::$partitionId",
                      borders(partitionId) until end)
                }
                val t2 = System.nanoTime()
                val time = (t2 - t1) / 1e9d
                println("ArrowColumnarBatchRowDistributors::distributeBySort: %04.3f".format(time))
                distributed.toMap
              }
            }
          }
        }
      }
    }
  }

  /**
   * @param key ArrowColumnarBatchRow to distribute and close
   * @param partitionIds Array containing which row corresponds to which partition
   * @return A map from partitionId to its corresponding ArrowColumnarBatchRow
   *
   * Caller should close the batches in the returned map
   */
  def distribute(key: ArrowColumnarBatchRow, partitionIds: Array[Int]): Map[Int, ArrowColumnarBatchRow] = {
    val t1 = System.nanoTime()
    Resources.autoCloseTryGet(key) { key =>
      // FIXME: close builder if we return earlier than expected
      val distributed = mutable.Map[Int, ArrowColumnarBatchRowBuilder]()
      try {
        partitionIds.zipWithIndex foreach { case (partitionId, index) =>
          if (distributed.contains(partitionId))
            distributed(partitionId).append(key
              .copyFromCaller("ArrowColumnarBatchRowDistributors::distribute", index until index+1))
          else
            distributed(partitionId) = new ArrowColumnarBatchRowBuilder(key
              .copyFromCaller("ArrowColumnarBatchRowDistributors::distribute::first", index until index+1))
        }

        distributed.map ( items =>
          (items._1, items._2.build(createAllocator(key.allocator.getRoot, "ArrowColumnarBatchRowDistributors::distribute::build"))) ).toMap
      } finally {
        distributed.foreach ( item => item._2.close() )
        val t2 = System.nanoTime()
        val time = (t2 - t1) / 1e9d
        println("ArrowColumnarBatchRowDistributors::distribute: %04.3f\n".format(time))
      }
    }
  }
}
