/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.memory;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBufAllocator;

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
  public ArrowBuf buffer(int size);

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
  public ArrowBuf buffer(int size, BufferManager manager);

  /**
   * Returns the allocator this allocator falls back to when it needs more memory.
   *
   * @return the underlying allocator used by this allocator
   */
  public ByteBufAllocator getAsByteBufAllocator();

  /**
   * Create a new child allocator.
   *
   * @param name            the name of the allocator.
   * @param initReservation the initial space reservation (obtained from this allocator)
   * @param maxAllocation   maximum amount of space the new allocator can allocate
   * @return the new allocator, or null if it can't be created
   */
  public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation);

  /**
   * Create a new child allocator.
   *
   * @param name            the name of the allocator.
   * @param listener        allocation listener for the newly created child
   * @param initReservation the initial space reservation (obtained from this allocator)
   * @param maxAllocation   maximum amount of space the new allocator can allocate
   * @return the new allocator, or null if it can't be created
   */
  public BufferAllocator newChildAllocator(
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
  public void close();

  /**
   * Returns the amount of memory currently allocated from this allocator.
   *
   * @return the amount of memory currently allocated
   */
  public long getAllocatedMemory();

  /**
   * Return the current maximum limit this allocator imposes.
   *
   * @return Limit in number of bytes.
   */
  public long getLimit();

  /**
   * Set the maximum amount of memory this allocator is allowed to allocate.
   *
   * @param newLimit The new Limit to apply to allocations
   */
  public void setLimit(long newLimit);

  /**
   * Returns the peak amount of memory allocated from this allocator.
   *
   * @return the peak amount of memory allocated
   */
  public long getPeakMemoryAllocation();

  /**
   * Returns the amount of memory that can probably be allocated at this moment
   * without exceeding this or any parents allocation maximum.
   *
   * @return Headroom in bytes
   */
  public long getHeadroom();

  /**
   * Create an allocation reservation. A reservation is a way of building up
   * a request for a buffer whose size is not known in advance. See
   *
   * @return the newly created reservation
   * @see AllocationReservation
   */
  public AllocationReservation newReservation();

  /**
   * Get a reference to the empty buffer associated with this allocator. Empty buffers are
   * special because we don't
   * worry about them leaking or managing reference counts on them since they don't actually
   * point to any memory.
   *
   * @return the empty buffer
   */
  public ArrowBuf getEmpty();

  /**
   * Return the name of this allocator. This is a human readable name that can help debugging.
   * Typically provides
   * coordinates about where this allocator was created
   *
   * @return the name of the allocator
   */
  public String getName();

  /**
   * Return whether or not this allocator (or one if its parents) is over its limits. In the case
   * that an allocator is
   * over its limit, all consumers of that allocator should aggressively try to addrss the
   * overlimit situation.
   *
   * @return whether or not this allocator (or one if its parents) is over its limits
   */
  public boolean isOverLimit();

  /**
   * Return a verbose string describing this allocator. If in DEBUG mode, this will also include
   * relevant stacktraces
   * and historical logs for underlying objects
   *
   * @return A very verbose description of the allocator hierarchy.
   */
  public String toVerboseString();

  /**
   * Asserts (using java assertions) that the provided allocator is currently open. If assertions
   * are disabled, this is
   * a no-op.
   */
  public void assertOpen();
}
