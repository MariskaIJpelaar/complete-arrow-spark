package org.apache.spark.sql

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

package object column {
  type ColumnDataFrame = Dataset[ColumnBatch]

  /** FIXME: Ugly, but for now, we will try to manage memory allocation through a public variable
   * In fact, it is recommended to use the same allocator throughout the whole program, but we should probably
   * handle accessibility better, and make use of ChildAllocators */

  /** Listener to make sure that allocators are automatically cleaned up if all its children are closed
   * and no memory is allocated anymore */
  private val ourListener = new AllocationListener {
    override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
      super.onChildRemoved(parentAllocator, childAllocator)

      if (parentAllocator.getAllocatedMemory == 0)
        parentAllocator.close()
    }

  }

  val totalSize: Long = 16L * 1024L * 1024L * 1024L // 16 GB
  val perAllocatorSize: Long = totalSize
  var rootAllocator = new RootAllocator(ourListener, totalSize)
//  private def closeAllocator(allocator: BufferAllocator): Unit = {
//    allocator.getChildAllocators.forEach(closeAllocator(_))
//    allocator.close()
//  }
  def resetRootAllocator(): Unit = {
//    closeAllocator(rootAllocator)
    rootAllocator.close()
    rootAllocator = new RootAllocator(ourListener, totalSize)
  }
}
