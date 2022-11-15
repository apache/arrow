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
