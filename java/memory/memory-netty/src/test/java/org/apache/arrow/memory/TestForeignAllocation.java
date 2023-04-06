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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestForeignAllocation {
  BufferAllocator allocator;

  @Before
  public void before() {
    allocator = new RootAllocator();
  }

  @After
  public void after() {
    allocator.close();
  }

  @Test
  public void wrapForeignAllocation() {
    final long bufferSize = 16;
    UnsafeForeignAllocation allocation = new UnsafeForeignAllocation(bufferSize);
    try {
      assertEquals(0, allocator.getAllocatedMemory());
      ArrowBuf buf = allocator.wrapForeignAllocation(allocation);
      assertEquals(bufferSize, buf.capacity());
      buf.close();
      assertTrue(allocation.released);
    } finally {
      allocation.release0();
    }
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void wrapForeignAllocationWithAllocationListener() {
    final long bufferSize = 16;

    final CountingAllocationListener listener = new CountingAllocationListener();
    try (BufferAllocator listenedAllocator =
        allocator.newChildAllocator("child", listener, 0L, allocator.getLimit())) {
      UnsafeForeignAllocation allocation = new UnsafeForeignAllocation(bufferSize);
      try {
        assertEquals(0, listenedAllocator.getAllocatedMemory());
        ArrowBuf buf = listenedAllocator.wrapForeignAllocation(allocation);
        assertEquals(bufferSize, buf.capacity());
        assertEquals(16, listener.getCurrentMem());
        buf.close();
        assertEquals(0, listener.getCurrentMem());
        assertTrue(allocation.released);
      } finally {
        allocation.release0();
      }
      assertEquals(0, listenedAllocator.getAllocatedMemory());
    }
    assertEquals(1, listener.getNumPreCalls());
    assertEquals(1, listener.getNumCalls());
    assertEquals(1, listener.getNumReleaseCalls());
    assertEquals(16, listener.getTotalMem());
  }

  @Test(expected = OutOfMemoryException.class)
  public void wrapForeignAllocationFailedWithAllocationListener() {
    final long bufferSize = 16;
    final long limit = bufferSize - 1;

    final CountingAllocationListener listener = new CountingAllocationListener();
    try (BufferAllocator listenedAllocator =
        allocator.newChildAllocator("child", listener, 0L, limit)) {
      UnsafeForeignAllocation allocation = new UnsafeForeignAllocation(bufferSize);
      try {
        assertEquals(0, listenedAllocator.getAllocatedMemory());
        ArrowBuf buf = listenedAllocator.wrapForeignAllocation(allocation);
        assertEquals(bufferSize, buf.capacity());
        buf.close();
        assertTrue(allocation.released);
      } finally {
        allocation.release0();
      }
    }
  }

  @Test
  public void wrapForeignAllocationWithAllocationListenerReclaimingSpace() {
    final long bufferSize = 16;
    final long limit = 2 * bufferSize - 1;

    final List<ArrowBuf> buffersToBeFreed = new ArrayList<>();
    final AllocationListener listener = new AllocationListener() {
      @Override
      public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
        buffersToBeFreed.forEach(ArrowBuf::close);
        return true;
      }
    };

    try (BufferAllocator listenedAllocator =
        allocator.newChildAllocator("child", listener, 0L, limit)) {
      final ArrowBuf buffer1 = listenedAllocator.buffer(bufferSize);
      buffersToBeFreed.add(buffer1);
      UnsafeForeignAllocation allocation = new UnsafeForeignAllocation(bufferSize);
      try (final ArrowBuf buffer2 = listenedAllocator.wrapForeignAllocation(allocation)) {
        assertEquals(bufferSize, buffer2.capacity());
        assertEquals(0, buffer1.getReferenceManager().getRefCount()); // buffer1 was closed by listener
      }
    }
  }

  private static class UnsafeForeignAllocation extends ForeignAllocation {
    boolean released = false;

    public UnsafeForeignAllocation(long bufferSize) {
      super(bufferSize, MemoryUtil.UNSAFE.allocateMemory(bufferSize));
    }

    @Override
    protected void release0() {
      if (!released) {
        MemoryUtil.UNSAFE.freeMemory(memoryAddress());
        released = true;
      }
    }
  }
}
