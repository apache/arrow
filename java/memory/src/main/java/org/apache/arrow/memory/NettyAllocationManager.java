/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import org.apache.arrow.memory.util.LargeMemoryUtil;

import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;

/**
 * The default implementation of AllocationManagerBase. The implementation is responsible for managing when memory
 * is allocated and returned to the Netty-based PooledByteBufAllocatorL.
 */
public class NettyAllocationManager extends AllocationManager {

  public static final Factory FACTORY = new Factory();

  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();
  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final long CHUNK_SIZE = INNER_ALLOCATOR.getChunkSize();

  private final int allocatedSize;
  private final UnsafeDirectLittleEndian memoryChunk;

  NettyAllocationManager(BaseAllocator accountingAllocator, int requestedSize) {
    super(accountingAllocator);
    this.memoryChunk = INNER_ALLOCATOR.allocate(requestedSize);
    this.allocatedSize = memoryChunk.capacity();
  }

  /**
   * Get the underlying memory chunk managed by this AllocationManager.
   * @return buffer
   */
  UnsafeDirectLittleEndian getMemoryChunk() {
    return memoryChunk;
  }

  @Override
  protected long memoryAddress() {
    return memoryChunk.memoryAddress();
  }

  @Override
  protected void release0() {
    memoryChunk.release();
  }

  /**
   * Returns the underlying memory chunk size managed.
   *
   * <p>NettyAllocationManager rounds requested size up to the next power of two.
   */
  @Override
  public long getSize() {
    return allocatedSize;
  }

  /**
   * Factory for creating {@link NettyAllocationManager}.
   */
  public static class Factory implements AllocationManager.Factory {
    private Factory() {}

    @Override
    public AllocationManager create(BaseAllocator accountingAllocator, long size) {
      return new NettyAllocationManager(accountingAllocator, LargeMemoryUtil.checkedCastToInt(size));
    }
  }
}
