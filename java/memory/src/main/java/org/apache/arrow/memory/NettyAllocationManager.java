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

import io.netty.buffer.PooledByteBufAllocatorL;
import io.netty.buffer.UnsafeDirectLittleEndian;
import io.netty.util.internal.PlatformDependent;

/**
 * The default implementation of {@link AllocationManager}. The implementation is responsible for managing when memory
 * is allocated and returned to the Netty-based PooledByteBufAllocatorL.
 */
public class NettyAllocationManager extends AllocationManager {

  public static final Factory FACTORY = new Factory();

  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();
  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final long CHUNK_SIZE = INNER_ALLOCATOR.getChunkSize();

  private final long allocatedSize;

  private final long allocatedAddress;

  NettyAllocationManager(BaseAllocator accountingAllocator, long requestedSize) {
    super(accountingAllocator);
    allocatedAddress = PlatformDependent.allocateMemory(requestedSize);
    allocatedSize = requestedSize;
  }

  @Override
  protected long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  protected void release0() {
    PlatformDependent.freeMemory(allocatedAddress);
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
      return new NettyAllocationManager(accountingAllocator, size);
    }
  }
}
