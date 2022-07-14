package org.apache.spark

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.column.AllocationManager.newRoot

/** Each [[ArrowPartition]] should have its own [[RootAllocator]] */
trait ArrowPartition extends Partition {
  @transient lazy val allocator: RootAllocator = newRoot()
}
