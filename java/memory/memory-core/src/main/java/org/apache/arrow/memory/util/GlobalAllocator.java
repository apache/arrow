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

package org.apache.arrow.memory.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.VisibleForTesting;

/**
 * Default implementation for memory allocation in case you are using a global
 * allocator, otherwise, you should pass root allocator explicitly on demand.
 */
public final class GlobalAllocator {
  private GlobalAllocator() {}

  private static final RootAllocator GLOBAL_ROOT_ALLOCATOR = new RootAllocator();
  private static final AtomicInteger CHILD_NUMBER = new AtomicInteger(0);

  /**
   * Get default new child allocator.
   *
   * @return a new child {@link BufferAllocator}.
   */
  public static BufferAllocator getChildAllocator() {
    return getChildAllocator(0, Long.MAX_VALUE);
  }

  /**
   * Get custom new child allocator with initial reservation and maximum allocation.
   *
   * @return a new child {@link BufferAllocator}.
   */
  public static BufferAllocator getChildAllocator(final long initReservation, final long maxAllocation) {
    return GLOBAL_ROOT_ALLOCATOR.newChildAllocator(nextChildName(), initReservation, maxAllocation);
  }

  private static String nextChildName() {
    return "ChildAllocator-" + CHILD_NUMBER.incrementAndGet();
  }

  /**
   * Get the total allocated memory across all child allocators.
   *
   * @return the number of allocated bytes.
   */
  public static long getAllChildAllocatedMemory() {
    return GLOBAL_ROOT_ALLOCATOR.getAllocatedMemory();
  }

  /**
   * Check allocators that have not been closed.
   *
   * @return <code>true</code> if child allocators exist or <code>false</code> if not.
   */
  public static boolean hasActiveAllocators() {
    return !GLOBAL_ROOT_ALLOCATOR.getChildAllocators().isEmpty();
  }

  @VisibleForTesting
  static void checkGlobalCleanUpResources() {
    if (!GLOBAL_ROOT_ALLOCATOR.getChildAllocators().isEmpty()) {
      throw new IllegalStateException(String.format(
          "Cannot continue with active allocators: %s.",
          GLOBAL_ROOT_ALLOCATOR.getChildAllocators().stream()
              .map(BufferAllocator::getName)
              .collect(Collectors.joining(", "))
      ));
    } else if (GLOBAL_ROOT_ALLOCATOR.getAllocatedMemory() != 0) {
      throw new IllegalStateException(String.format(
          "Cannot continue with allocated memory: %s.",
          GLOBAL_ROOT_ALLOCATOR.getAllocatedMemory()
      ));
    }
  }

  @VisibleForTesting
  static void verifyStateOfAllocator() {
    GLOBAL_ROOT_ALLOCATOR.verify();
  }
}

