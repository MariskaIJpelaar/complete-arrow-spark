package org.apache.arrow.algorithm.sort

import org.apache.arrow.memory.BufferAllocator

/** Same as org.apache.arrow.algorithm.sort.OffHeapIntStack, but public :) */
class PublicOffHeapIntStack(allocator: BufferAllocator) extends OffHeapIntStack(allocator) {}
