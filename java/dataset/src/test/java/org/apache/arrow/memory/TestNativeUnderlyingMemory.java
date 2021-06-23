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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNativeUnderlyingMemory {

  private RootAllocator allocator = null;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  protected RootAllocator rootAllocator() {
    return allocator;
  }

  @Test
  public void testReservation() {
    final RootAllocator root = rootAllocator();

    final int size = 512;
    final AllocationManager am = new MockUnderlyingMemory(root, size);
    final BufferLedger ledger = am.associate(root);

    assertEquals(size, root.getAllocatedMemory());

    ledger.release();
  }

  @Test
  public void testBufferTransfer() {
    final RootAllocator root = rootAllocator();

    ChildAllocator allocator1 = (ChildAllocator) root.newChildAllocator("allocator1", 0, Long.MAX_VALUE);
    ChildAllocator allocator2 = (ChildAllocator) root.newChildAllocator("allocator2", 0, Long.MAX_VALUE);
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(0, allocator2.getAllocatedMemory());

    final int size = 512;
    final AllocationManager am = new MockUnderlyingMemory(allocator1, size);

    final BufferLedger owningLedger = am.associate(allocator1);
    assertEquals(size, owningLedger.getAccountedSize());
    assertEquals(size, owningLedger.getSize());
    assertEquals(size, allocator1.getAllocatedMemory());

    final BufferLedger transferredLedger = am.associate(allocator2);
    owningLedger.release(); // release previous owner
    assertEquals(0, owningLedger.getAccountedSize());
    assertEquals(size, owningLedger.getSize());
    assertEquals(size, transferredLedger.getAccountedSize());
    assertEquals(size, transferredLedger.getSize());
    assertEquals(0, allocator1.getAllocatedMemory());
    assertEquals(size, allocator2.getAllocatedMemory());

    transferredLedger.release();
    allocator1.close();
    allocator2.close();
  }

  /**
   * A mock class of {@link NativeUnderlyingMemory} for unit testing about size-related operations.
   */
  private static class MockUnderlyingMemory extends NativeUnderlyingMemory {

    /**
     * Constructor.
     */
    MockUnderlyingMemory(BaseAllocator accountingAllocator, int size) {
      super(accountingAllocator, size, -1L, -1L);
    }

    @Override
    protected void release0() {
      System.out.println("Underlying memory released. Size: " + getSize());
    }

    @Override
    protected long memoryAddress() {
      throw new UnsupportedOperationException();
    }
  }
}
