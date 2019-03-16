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

/**
 * An allocation listener being notified for allocation/deallocation
 *
 * <p>It might be called from multiple threads if the allocator hierarchy shares a listener, in which
 * case, the provider should take care of making the implementation thread-safe.
 */
public interface AllocationListener {

  public static final AllocationListener NOOP = new AllocationListener() {};

  /**
   * Called each time a new buffer has been requested.
   *
   * <p>An exception can be safely thrown by this method to terminate the allocation.
   *
   * @param size the buffer size being allocated
   */
  default void onPreAllocation(long size) {}

  /**
   * Called each time a new buffer has been allocated.
   *
   * <p>An exception cannot be thrown by this method.
   *
   * @param size the buffer size being allocated
   */
  default void onAllocation(long size) {}

  /**
   * Informed each time a buffer is released from allocation.
   *
   * <p>An exception cannot be thrown by this method.
   * @param size The size of the buffer being released.
   */
  default void onRelease(long size) {}


  /**
   * Called whenever an allocation failed, giving the caller a chance to create some space in the
   * allocator (either by freeing some resource, or by changing the limit), and, if successful,
   * allowing the allocator to retry the allocation.
   *
   * @param size     the buffer size that was being allocated
   * @param outcome  the outcome of the failed allocation. Carries information of what failed
   * @return true, if the allocation can be retried; false if the allocation should fail
   */
  default boolean onFailedAllocation(long size, AllocationOutcome outcome) {
    return false;
  }

  /**
   * Called immediately after a child allocator was added to the parent allocator.
   *
   * @param parentAllocator The parent allocator to which a child was added
   * @param childAllocator  The child allocator that was just added
   */
  default void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {}

  /**
   * Called immediately after a child allocator was removed from the parent allocator.
   *
   * @param parentAllocator The parent allocator from which a child was removed
   * @param childAllocator The child allocator that was just removed
   */
  default void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {}
}
