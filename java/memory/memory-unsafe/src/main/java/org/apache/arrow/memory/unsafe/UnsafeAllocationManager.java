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

package org.apache.arrow.memory.unsafe;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Allocation manager based on unsafe API.
 */
public final class UnsafeAllocationManager extends AllocationManager {

  private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      MemoryUtil.UNSAFE.allocateMemory(0)
  );

  public static final AllocationManager.Factory FACTORY = new Factory() {
    @Override
    public AllocationManager create(BufferAllocator accountingAllocator, long size) {
      return new UnsafeAllocationManager(accountingAllocator, size);
    }

    @Override
    public ArrowBuf empty() {
      return EMPTY;
    }
  };

  private final long allocatedSize;

  private final long allocatedAddress;

  UnsafeAllocationManager(BufferAllocator accountingAllocator, long requestedSize) {
    super(accountingAllocator);
    allocatedAddress = MemoryUtil.UNSAFE.allocateMemory(requestedSize);
    allocatedSize = requestedSize;
  }

  @Override
  public long getSize() {
    return allocatedSize;
  }

  @Override
  protected long memoryAddress() {
    return allocatedAddress;
  }

  @Override
  protected void release0() {
    MemoryUtil.UNSAFE.freeMemory(allocatedAddress);
  }

}
