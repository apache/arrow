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
 * Counting allocation listener.
 * It counts the number of times it has been invoked, and how much memory allocation it has seen
 * When set to 'expand on fail', it attempts to expand the associated allocator's limit.
 */
final class CountingAllocationListener implements AllocationListener {
  private int numPreCalls;
  private int numCalls;
  private int numReleaseCalls;
  private int numChildren;
  private long totalMem;
  private long currentMem;
  private boolean expandOnFail;
  BufferAllocator expandAlloc;
  long expandLimit;

  CountingAllocationListener() {
    this.numCalls = 0;
    this.numChildren = 0;
    this.totalMem = 0;
    this.currentMem = 0;
    this.expandOnFail = false;
    this.expandAlloc = null;
    this.expandLimit = 0;
  }

  @Override
  public void onPreAllocation(long size) {
    numPreCalls++;
  }

  @Override
  public void onAllocation(long size) {
    numCalls++;
    totalMem += size;
    currentMem += size;
  }

  @Override
  public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
    if (expandOnFail) {
      expandAlloc.setLimit(expandLimit);
      return true;
    }
    return false;
  }


  @Override
  public void onRelease(long size) {
    numReleaseCalls++;
    currentMem -= size;
  }

  @Override
  public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
    ++numChildren;
  }

  @Override
  public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
    --numChildren;
  }

  void setExpandOnFail(BufferAllocator expandAlloc, long expandLimit) {
    this.expandOnFail = true;
    this.expandAlloc = expandAlloc;
    this.expandLimit = expandLimit;
  }

  int getNumPreCalls() {
    return numPreCalls;
  }

  int getNumReleaseCalls() {
    return numReleaseCalls;
  }

  int getNumCalls() {
    return numCalls;
  }

  int getNumChildren() {
    return numChildren;
  }

  long getTotalMem() {
    return totalMem;
  }

  long getCurrentMem() {
    return currentMem;
  }
}
