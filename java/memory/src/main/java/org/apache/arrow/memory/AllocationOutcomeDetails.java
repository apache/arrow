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

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Captures details of allocation for each accountant in the hierarchical chain.
 */
public class AllocationOutcomeDetails {
  Deque<Entry> allocEntries;

  AllocationOutcomeDetails() {
    allocEntries = new ArrayDeque<>();
  }

  void pushEntry(Accountant accountant, long totalUsedBeforeAllocation, long requestedSize,
      long allocatedSize, boolean allocationFailed) {

    Entry top = allocEntries.peekLast();
    if (top != null && top.allocationFailed) {
      // if the allocation has already failed, stop saving the entries.
      return;
    }

    allocEntries.addLast(new Entry(accountant, totalUsedBeforeAllocation, requestedSize,
        allocatedSize, allocationFailed));
  }

  /**
   * Get the allocator that caused the failure.
   * @return the allocator that caused failure, null if there was no failure.
   */
  public BufferAllocator getFailedAllocator() {
    Entry top = allocEntries.peekLast();
    if (top != null && top.allocationFailed && (top.accountant instanceof BufferAllocator)) {
      return (BufferAllocator)top.accountant;
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Allocation outcome details:\n");
    allocEntries.forEach(sb::append);
    return sb.toString();
  }

  /**
   * Outcome of the allocation request at one accountant in the hierarchy.
   */
  public static class Entry {
    private final Accountant accountant;

    // Remember allocator attributes at the time of the request.
    private final long limit;
    private final long used;

    // allocation outcome
    private final long requestedSize;
    private final long allocatedSize;
    private final boolean allocationFailed;

    Entry(Accountant accountant, long totalUsedBeforeAllocation, long requestedSize,
        long allocatedSize, boolean allocationFailed) {
      this.accountant = accountant;
      this.limit = accountant.getLimit();
      this.used = totalUsedBeforeAllocation;

      this.requestedSize = requestedSize;
      this.allocatedSize = allocatedSize;
      this.allocationFailed = allocationFailed;
    }

    public Accountant getAccountant() {
      return accountant;
    }

    public long getLimit() {
      return limit;
    }

    public long getUsed() {
      return used;
    }

    public long getRequestedSize() {
      return requestedSize;
    }

    public long getAllocatedSize() {
      return allocatedSize;
    }

    public boolean isAllocationFailed() {
      return allocationFailed;
    }

    @Override
    public String toString() {
      return new StringBuilder()
          .append("allocator[" + accountant.getName() + "]")
          .append(" reservation: " + accountant.getInitReservation())
          .append(" limit: " + limit)
          .append(" used: " + used)
          .append(" requestedSize: " + requestedSize)
          .append(" allocatedSize: " + allocatedSize)
          .append(" localAllocationStatus: " + (allocationFailed ? "success" : "fail"))
          .append("\n")
          .toString();
    }
  }

}
