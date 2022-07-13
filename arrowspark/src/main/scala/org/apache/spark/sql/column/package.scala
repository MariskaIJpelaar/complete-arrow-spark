package org.apache.spark.sql

import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

package object column {
  type ColumnDataFrame = Dataset[ColumnBatch]

  /** FIXME: Ugly, but for now, we will try to manage memory allocation through a public variable
   * In fact, it is recommended to use the same allocator throughout the whole program */

  /** Listener to make sure that allocators are automatically cleaned up if all its children are closed
   * and no memory is allocated anymore */
    // TODO: use when required
//  private val ourListener = new AllocationListener {
//    override def onChildRemoved(parentAllocator: BufferAllocator, childAllocator: BufferAllocator): Unit = {
//      super.onChildRemoved(parentAllocator, childAllocator)
//
//      if (parentAllocator.getAllocatedMemory == 0)
//        parentAllocator.close()
//    }
//  }
//  var rootAllocator = new RootAllocator(totalSize)
//  private def closeAllocator(allocator: BufferAllocator): Unit = {
//    allocator.getChildAllocators.forEach(closeAllocator(_))
//    allocator.close()
//  }

//  /**
//   * Resets the rootAllocator to check if we released all memory
//   */
//  def resetRootAllocator(): Unit = {
////    closeAllocator(rootAllocator)
//    rootAllocator.close()
//    rootAllocator = new RootAllocator(totalSize)
//  }

  object AllocationManager {
    val totalSize: Long = 16L * 1024L * 1024L * 1024L // 16 GB
    val perAllocatorSize: Long = totalSize
//    private val roots: mutable.ListBuffer[RootAllocator] = ListBuffer.empty

    /** Clears all current roots */
    def reset(): Unit = {
//      roots.clear();
    }

    /**
     * Create a new root to keep track of
     * @return the newly created [[RootAllocator]]
     */
   def newRoot(): RootAllocator = {
     val root = new RootAllocator(totalSize)
//     roots.append(root)
     root
   }


    def isCleaned: Boolean = true
//      roots.forall( allocator => {
//      try {
//        // we actually want to assertClosed()...
//        allocator.assertOpen()
//        return false
//      } catch {
//        case _: Throwable => return true
//      }
//    })

    /** Closes all [[RootAllocator]]s in the tracked roots */
    def cleanup(): Unit = {
//      roots.foreach( _.close() )
//      roots.clear()
    }

    /**
     * Creates a new [[BufferAllocator]] as child from the rootAllocator
     * @param rootAllocator [[RootAllocator]] to assign child to
     * @param name name of the newly created allocator
     * @return the newly created allocator
     */
    def createAllocator(rootAllocator: RootAllocator, name: String): BufferAllocator = {
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
}
