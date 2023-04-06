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

import java.util.Collection;

import org.apache.arrow.memory.rounding.DefaultRoundingPolicy;
import org.apache.arrow.memory.rounding.RoundingPolicy;

/**
 * Wrapper class to deal with byte buffer allocation. Ensures users only use designated methods.
 */
public interface BufferAllocator extends AutoCloseable {

  /**
   * Allocate a new or reused buffer of the provided size. Note that the buffer may technically
   * be larger than the
   * requested size for rounding purposes. However, the buffer's capacity will be set to the
   * configured size.
   *
   * @param size The size in bytes.
   * @return a new ArrowBuf, or null if the request can't be satisfied
   * @throws OutOfMemoryException if buffer cannot be allocated
   */
  ArrowBuf buffer(long size);

  /**
   * Allocate a new or reused buffer of the provided size. Note that the buffer may technically
   * be larger than the
   * requested size for rounding purposes. However, the buffer's capacity will be set to the
   * configured size.
   *
   * @param size    The size in bytes.
   * @param manager A buffer manager to manage reallocation.
   * @return a new ArrowBuf, or null if the request can't be satisfied
   * @throws OutOfMemoryException if buffer cannot be allocated
   */
  ArrowBuf buffer(long size, BufferManager manager);

  /**
   * Get the root allocator of this allocator. If this allocator is already a root, return
   * this directly.
   *
   * @return The root allocator
   */
  BufferAllocator getRoot();

  /**
   * Create a new child allocator.
   *
   * @param name            the name of the allocator.
   * @param initReservation the initial space reservation (obtained from this allocator)
   * @param maxAllocation   maximum amount of space the new allocator can allocate
   * @return the new allocator, or null if it can't be created
   */
  BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation);

  /**
   * Create a new child allocator.
   *
   * @param name            the name of the allocator.
   * @param listener        allocation listener for the newly created child
   * @param initReservation the initial space reservation (obtained from this allocator)
   * @param maxAllocation   maximum amount of space the new allocator can allocate
   * @return the new allocator, or null if it can't be created
   */
  BufferAllocator newChildAllocator(
      String name,
      AllocationListener listener,
      long initReservation,
      long maxAllocation);

  /**
   * Close and release all buffers generated from this buffer pool.
   *
   * <p>When assertions are on, complains if there are any outstanding buffers; to avoid
   * that, release all buffers before the allocator is closed.</p>
   */
  @Override
  void close();

  /**
   * Returns the amount of memory currently allocated from this allocator.
   *
   * @return the amount of memory currently allocated
   */
  long getAllocatedMemory();

  /**
   * Return the current maximum limit this allocator imposes.
   *
   * @return Limit in number of bytes.
   */
  long getLimit();

  /**
   * Return the initial reservation.
   *
   * @return reservation in bytes.
   */
  long getInitReservation();

  /**
   * Set the maximum amount of memory this allocator is allowed to allocate.
   *
   * @param newLimit The new Limit to apply to allocations
   */
  void setLimit(long newLimit);

  /**
   * Returns the peak amount of memory allocated from this allocator.
   *
   * @return the peak amount of memory allocated
   */
  long getPeakMemoryAllocation();

  /**
   * Returns the amount of memory that can probably be allocated at this moment
   * without exceeding this or any parents allocation maximum.
   *
   * @return Headroom in bytes
   */
  long getHeadroom();

  /**
   * Forcibly allocate bytes. Returns whether the allocation fit within limits.
   *
   * @param size to increase
   * @return Whether the allocation fit within limits.
   */
  boolean forceAllocate(long size);


  /**
   * Release bytes from this allocator.
   *
   * @param size to release
   */
  void releaseBytes(long size);

  /**
   * Returns the allocation listener used by this allocator.
   *
   * @return the {@link AllocationListener} instance. Or {@link AllocationListener#NOOP} by default if no listener
   *         is configured when this allocator was created.
   */
  AllocationListener getListener();

  /**
   * Returns the parent allocator.
   *
   * @return parent allocator
   */
  BufferAllocator getParentAllocator();

  /**
   * Returns the set of child allocators.
   *
   * @return set of child allocators
   */
  Collection<BufferAllocator> getChildAllocators();

  /**
   * Create an allocation reservation. A reservation is a way of building up
   * a request for a buffer whose size is not known in advance. See
   *
   * @return the newly created reservation
   * @see AllocationReservation
   */
  AllocationReservation newReservation();

  /**
   * Get a reference to the empty buffer associated with this allocator. Empty buffers are
   * special because we don't
   * worry about them leaking or managing reference counts on them since they don't actually
   * point to any memory.
   *
   * @return the empty buffer
   */
  ArrowBuf getEmpty();

  /**
   * Return the name of this allocator. This is a human readable name that can help debugging.
   * Typically provides
   * coordinates about where this allocator was created
   *
   * @return the name of the allocator
   */
  String getName();

  /**
   * Return whether or not this allocator (or one if its parents) is over its limits. In the case
   * that an allocator is
   * over its limit, all consumers of that allocator should aggressively try to address the
   * overlimit situation.
   *
   * @return whether or not this allocator (or one if its parents) is over its limits
   */
  boolean isOverLimit();

  /**
   * Return a verbose string describing this allocator. If in DEBUG mode, this will also include
   * relevant stacktraces
   * and historical logs for underlying objects
   *
   * @return A very verbose description of the allocator hierarchy.
   */
  String toVerboseString();

  /**
   * Asserts (using java assertions) that the provided allocator is currently open. If assertions
   * are disabled, this is
   * a no-op.
   */
  void assertOpen();

  /**
   * Gets the rounding policy of the allocator.
   */
  default RoundingPolicy getRoundingPolicy() {
    return DefaultRoundingPolicy.DEFAULT_ROUNDING_POLICY;
  }

  /**
   * EXPERIMENTAL: Wrap an allocation created outside this BufferAllocator.
   *
   * <p>This is useful to integrate allocations from native code into the same memory management framework as
   * Java-allocated buffers, presenting users a consistent API. The created buffer will be tracked by this allocator
   * and can be transferred like Java-allocated buffers.
   *
   * <p>The underlying allocation will be closed when all references to the buffer are released. If this method throws,
   * the underlying allocation will also be closed.
   *
   * @param allocation The underlying allocation.
   */
  default ArrowBuf wrapForeignAllocation(ForeignAllocation allocation) {
    throw new UnsupportedOperationException();
  }
}
