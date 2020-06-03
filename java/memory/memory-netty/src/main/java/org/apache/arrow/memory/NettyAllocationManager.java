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

  public static final AllocationManager.Factory FACTORY = new AllocationManager.Factory() {

    @Override
    public AllocationManager create(BaseAllocator accountingAllocator, long size) {
      return new NettyAllocationManager(accountingAllocator, size);
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY_BUFFER;
    }
  };

  /**
   * The default cut-off value for switching allocation strategies.
   * If the request size is not greater than the cut-off value, we will allocate memory by
   * {@link PooledByteBufAllocatorL} APIs,
   * otherwise, we will use {@link PlatformDependent} APIs.
   */
  public static final int DEFAULT_ALLOCATION_CUTOFF_VALUE = Integer.MAX_VALUE;

  private static final PooledByteBufAllocatorL INNER_ALLOCATOR = new PooledByteBufAllocatorL();
  static final UnsafeDirectLittleEndian EMPTY = INNER_ALLOCATOR.empty;
  static final ArrowBuf EMPTY_BUFFER = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      NettyAllocationManager.EMPTY.memoryAddress());
  static final long CHUNK_SIZE = INNER_ALLOCATOR.getChunkSize();

  private final long allocatedSize;
  private final UnsafeDirectLittleEndian memoryChunk;
  private final long allocatedAddress;

  /**
   * The cut-off value for switching allocation strategies.
   */
  private final int allocationCutOffValue;

  NettyAllocationManager(BaseAllocator accountingAllocator, long requestedSize, int allocationCutOffValue) {
    super(accountingAllocator);
    this.allocationCutOffValue = allocationCutOffValue;

    if (requestedSize > allocationCutOffValue) {
      this.memoryChunk = null;
      this.allocatedAddress = PlatformDependent.allocateMemory(requestedSize);
      this.allocatedSize = requestedSize;
    } else {
      this.memoryChunk = INNER_ALLOCATOR.allocate(requestedSize);
      this.allocatedAddress = memoryChunk.memoryAddress();
      this.allocatedSize = memoryChunk.capacity();
    }
  }

  NettyAllocationManager(BaseAllocator accountingAllocator, long requestedSize) {
    this(accountingAllocator, requestedSize, DEFAULT_ALLOCATION_CUTOFF_VALUE);
  }

  /**
   * Get the underlying memory chunk managed by this AllocationManager.
   * @return the underlying memory chunk if the request size is not greater than the
   *   {@link NettyAllocationManager#allocationCutOffValue}, or null otherwise.
   *
   * @deprecated this method will be removed in a future release.
   */
  @Deprecated
  UnsafeDirectLittleEndian getMemoryChunk() {
    return memoryChunk;
  }

  @Override
  protected long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  protected void release0() {
    if (memoryChunk == null) {
      PlatformDependent.freeMemory(allocatedAddress);
    } else {
      memoryChunk.release();
    }
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

}
