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

package org.apache.arrow.memory.util.test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * A utility class used exclusively to detect memory leaks during unit tests.
 * <p>
 * The proposal allocates resources primarily using child allocators.
 * <p>
 * The pros are:
 * - It allows us to create a global allocator that is shared across all unit
 * tests in order to detect memory leaks during unit execution.
 * - Close validation of child allocators also involves checking for unreleased
 * allocators and notifying them appropriately.
 * - The user is responsible for closing resources properly, and it is validated
 * before and after all test cases if this is not done.
 * <p>
 * The cons are:
 * - Validation is only performed on child allocators, not the main root allocator.
 * - The global allocator does not support generate use cases with memory leak
 * scenarios because there is no cleaner way to close resources after the allocator
 * has been closed.
 */
public final class GlobalAllocator {
  private static final RootAllocator GLOBAL_ROOT_ALLOCATOR = new RootAllocator();
  private static final AtomicInteger CHILD_NUMBER = new AtomicInteger(0);

  private GlobalAllocator() {
  }

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

  private static String getClassMethodName() {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    for (StackTraceElement element : stackTraceElements) {
      if (!element.getClassName().equals(GlobalAllocator.class.getName()) &&
          element.getClassName().indexOf("java.lang.Thread") != 0) {
        return element.getClassName() + "-" + element.getMethodName();
      }
    }
    return null;
  }

  private static String nextChildName() {
    return "GlobalAllocator-Child-" + getClassMethodName() + "#" + CHILD_NUMBER.incrementAndGet();
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

  /**
   * Check status of result of clean up resources before to start unit test execution.
   */
  public static void checkGlobalCleanUpResources() {
    verifyStateOfAllocator();
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

  public static void verifyStateOfAllocator() {
    GLOBAL_ROOT_ALLOCATOR.verify();
  }
}
