package org.apache.spark.sql.column.utils

import nl.liacs.mijpelaar.utils.Resources
import org.apache.arrow.algorithm.sort.{DefaultVectorComparators, SparkComparator, SparkUnionComparator}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.complex.UnionVector
import org.apache.arrow.vector.{ValueVector, ZeroVector}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortOrder}
import org.apache.spark.sql.column.AllocationManager.createAllocator
import org.apache.spark.sql.column.ArrowColumnarBatchRow
import org.apache.spark.sql.vectorized.ArrowColumnVector

import java.util.concurrent.atomic.AtomicLong

/** Methods that do not fit into the other categories */
object ArrowColumnarBatchRowUtils {
  var totalTimeGetComparator: AtomicLong = new AtomicLong(0)

  /**
   * Get the SparkUnionComparator corresponding to given UnionVector and SortOrders
   * @param union UnionVector to make comparator from. Vector is not closed.
   * @param sortOrders SortOrders to make comparator from
   * @return a fresh Comparator, does not require any closing
   */
  def getComparator(union: UnionVector, sortOrders: Seq[SortOrder]): SparkUnionComparator = {
    val t1 = System.nanoTime()
    val comparators = union.getChildrenFromFields.toArray.map { vector =>
      val valueVector = vector.asInstanceOf[ValueVector]
      val name = valueVector.getName
      val sortOrder = sortOrders.find( order => order.child.asInstanceOf[AttributeReference].name.equals(name) )
      (name,
        new SparkComparator(
          sortOrder.getOrElse(throw new RuntimeException("ArrowColumnarBatchRowUtils::getComparator SortOrders do not match UnionVector")),
          DefaultVectorComparators.createDefaultComparator(valueVector)))
    }
    val ret = new SparkUnionComparator(comparators)
    val t2 = System.nanoTime()
    totalTimeGetComparator.addAndGet(t2 - t1)
    ret
  }

  var totalTimeTake: AtomicLong = new AtomicLong(0)

  /**
   * Returns the merged arrays from multiple ArrowColumnarBatchRows
   * @param rootAllocator [[RootAllocator]] to allocate with
   * @param numCols the number of columns to take
   * @param batches batches to create array from
   * @return array of merged columns with used allocator
   * Closes the batches from the iterator
   * WARNING: this is an expensive operation, because it copies all data. Use with care!
   *
   * Callers can define their own functions to process custom data:
   * @param extraTaker split the item from the iterator into (customData, batch)
   * @param extraCollector collect a new item from custom-data, first parameter is new item, second parameter is
   *                       the result from previous calls, None if there were no previous calls. Result of this function
   *                       is passed to other calls of extraCollector.
   * Note: user should close the batch if it consumes it (does not return it)
   *
   * Caller is responsible for closing returned vectors
   */
  def take(rootAllocator: RootAllocator, batches: Iterator[Any], numCols: Option[Int] = None, numRows: Option[Int] = None,
           extraTaker: Any => (Any, ArrowColumnarBatchRow) = batch => (None, batch.asInstanceOf[ArrowColumnarBatchRow]),
           extraCollector: (Any, Option[Any]) => Any = (_: Any, _: Option[Any]) => None): (Any, Array[ArrowColumnVector], BufferAllocator) = {
    val t1 = System.nanoTime()
    Resources.closeOnFailGet(createAllocator(rootAllocator, "ArrowColumnarBatchRowUtils::take")) { allocator =>
      if (!batches.hasNext) {
        if (numCols.isDefined)
          return (None, Array.tabulate[ArrowColumnVector](numCols.get)(i => new ArrowColumnVector( new ZeroVector(i.toString) ) ), allocator)
        return (None, new Array[ArrowColumnVector](0), allocator)
      }

      // FIXME: Resource releaser with custom close function?
      try {
        // get first batch and its extra
        val (extra, first) = extraTaker(batches.next())
        Resources.autoCloseTryGet(first) ( first => Resources.autoCloseTryGet(new ArrowColumnarBatchRowBuilder(first, numCols, numRows)) { builder =>

          var extraCollected = extraCollector(extra, None)
          while (batches.hasNext && numRows.forall( num => builder.length < num)) {
            val (extra, batch) = extraTaker(batches.next())
            Resources.autoCloseTryGet(batch)(batch => builder.append(batch))
            extraCollected = extraCollector(extra, Option(extraCollected))
          }
          (extraCollected, builder.buildColumns(allocator), allocator)
        })
      } finally {
        /** clear up the remainder */
        batches.foreach( extraTaker(_)._2.close() )
        val t2 = System.nanoTime()
        totalTimeTake.addAndGet(t2 - t1)
      }
    }
  }

}
