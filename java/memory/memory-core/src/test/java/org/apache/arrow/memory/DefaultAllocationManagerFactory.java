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

import org.apache.arrow.memory.util.MemoryUtil;

/**
 * The default Allocation Manager Factory for a module.
 *
 * This is only used by tests and contains only a simplistic allocator method.
 *
 */
public class DefaultAllocationManagerFactory implements AllocationManager.Factory {

  public static final AllocationManager.Factory FACTORY = new DefaultAllocationManagerFactory();
  private static final ArrowBuf EMPTY = new ArrowBuf(ReferenceManager.NO_OP,
      null,
      0,
      MemoryUtil.UNSAFE.allocateMemory(0));

  @Override
  public AllocationManager create(BufferAllocator accountingAllocator, long size) {
    return new AllocationManager(accountingAllocator) {
      private final long allocatedSize = size;
      private final long address = MemoryUtil.UNSAFE.allocateMemory(size);

      @Override
      public long getSize() {
        return allocatedSize;
      }

      @Override
      protected long memoryAddress() {
        return address;
      }

      @Override
      protected void release0() {
        MemoryUtil.UNSAFE.freeMemory(address);
      }
    };
  }

  @Override
  public ArrowBuf empty() {
    return EMPTY;
  }
}
