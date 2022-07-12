package org.apache.spark.sql

import org.apache.arrow.memory.{AllocationListener, BufferAllocator, RootAllocator}

package object column {
  type ColumnDataFrame = Dataset[ColumnBatch]

  /** FIXME: Ugly, but for now, we will try to manage memory allocation through a public variable
   * In fact, it is recommended to use the same allocator throughout the whole program */

  /** Listener to make sure that allocators are automatically cleaned up if all its children are closed
   * and no memory is allocated anymore */
    // TODO: use when required
  private val ourListener = new AllocationListener {
    override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
      super.onChildRemoved(parentAllocator, childAllocator)

      if (parentAllocator.getAllocatedMemory == 0)
        parentAllocator.close()
    }

  }

  val totalSize: Long = 16L * 1024L * 1024L * 1024L // 16 GB
  val perAllocatorSize: Long = totalSize
  var rootAllocator = new RootAllocator(totalSize)
//  private def closeAllocator(allocator: BufferAllocator): Unit = {
//    allocator.getChildAllocators.forEach(closeAllocator(_))
//    allocator.close()
//  }

  /**
   * Resets the rootAllocator to check if we released all memory
   */
  def resetRootAllocator(): Unit = {
//    closeAllocator(rootAllocator)
    rootAllocator.close()
    rootAllocator = new RootAllocator(totalSize)
  }

  /**
   * Creates a new [[BufferAllocator]] as child from the rootAllocator
   * @param name name of the newly created allocator
   * @return the newly created allocator
   */
  def createAllocator(name: String): BufferAllocator = {
    rootAllocator.newChildAllocator(name, 0, perAllocatorSize)
  }

  /**
   * Creates a new [[BufferAllocator]] as a child from the given allocator
   * @param parentAllocator [[BufferAllocator]] as parent for the new child
   * @param name name to give to the child
   * @return the newly created child
   */
  def createAllocator(parentAllocator: BufferAllocator, name: String): BufferAllocator = {
    parentAllocator.newChildAllocator(name, 0, perAllocatorSize)
  }
}
