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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;

import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import sun.misc.Unsafe;

public class TestCustomizedAllocationManager extends TestBaseAllocator {

  private static final int MAX_ALLOCATION = 8 * 1024;

  @Test
  public void testCustomizedAllocationManager() {
    try (final BaseAllocator allocator = createRootAllocator(MAX_ALLOCATION)) {
      final ArrowBuf arrowBuf1 = allocator.buffer(MAX_ALLOCATION);
      assertNotNull("allocation failed", arrowBuf1);

      arrowBuf1.setInt(0, 1);
      assertEquals(1, arrowBuf1.getInt(0));

      try {
        final ArrowBuf arrowBuf2 = allocator.buffer(1);
        fail("allocated memory beyond max allowed");
      } catch (OutOfMemoryException e) {
        // expected
      }

      arrowBuf1.getReferenceManager().release();
      try {
        arrowBuf1.getInt(0);
        fail("data read from released buffer");
      } catch (RuntimeException e) {
        // expected
      }
    }
  }

  @Override
  protected RootAllocator createRootAllocator(long maxAllocation) {
    return new RootAllocator(BaseAllocator.configBuilder()
        .maxAllocation(maxAllocation)
        .allocationManagerFactory((accountingAllocator, size) -> new AllocationManager(accountingAllocator, size) {
          private final Unsafe unsafe = getUnsafe();
          private final long address = unsafe.allocateMemory(size);

          @Override
          protected long memoryAddress() {
            return address;
          }

          @Override
          protected void release0() {
            unsafe.setMemory(address, size, (byte) 0);
            unsafe.freeMemory(address);
          }

          private Unsafe getUnsafe() {
            Field f = null;
            try {
              f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return (Unsafe) f.get(null);
            } catch (NoSuchFieldException | IllegalAccessException e) {
              throw new RuntimeException(e);
            } finally {
              if (f != null) {
                f.setAccessible(false);
              }
            }
          }
        }).create());
  }
}
