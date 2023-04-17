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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.memory.AllocationOutcomeDetails.Entry;
import org.apache.arrow.memory.rounding.RoundingPolicy;
import org.apache.arrow.memory.rounding.SegmentRoundingPolicy;
import org.apache.arrow.memory.util.AssertionUtil;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import sun.misc.Unsafe;

public class TestBaseAllocator {

  private static final int MAX_ALLOCATION = 8 * 1024;

  /*
  // ---------------------------------------- DEBUG -----------------------------------

  @After
  public void checkBuffers() {
    final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
    if (bufferCount != 0) {
      UnsafeDirectLittleEndian.logBuffers(logger);
      UnsafeDirectLittleEndian.releaseBuffers();
    }

    assertEquals(0, bufferCount);
  }

  //  @AfterClass
  //  public static void dumpBuffers() {
  //    UnsafeDirectLittleEndian.logBuffers(logger);
  //  }

  // ---------------------------------------- DEBUG ------------------------------------
  */


  @Test
  public void test_privateMax() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      final ArrowBuf arrowBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
      assertNotNull("allocation failed", arrowBuf1);

      try (final BufferAllocator childAllocator =
               rootAllocator.newChildAllocator("noLimits", 0, MAX_ALLOCATION)) {
        final ArrowBuf arrowBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", arrowBuf2);
        arrowBuf2.getReferenceManager().release();
      }

      arrowBuf1.getReferenceManager().release();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testRootAllocator_closeWithOutstanding() throws Exception {
    try {
      try (final RootAllocator rootAllocator =
               new RootAllocator(MAX_ALLOCATION)) {
        final ArrowBuf arrowBuf = rootAllocator.buffer(512);
        assertNotNull("allocation failed", arrowBuf);
      }
    } finally {
      /*
       * We expect there to be one unreleased underlying buffer because we're closing
       * without releasing it.
       */
      /*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
      */
    }
  }

  @Test
  public void testRootAllocator_getEmpty() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      final ArrowBuf arrowBuf = rootAllocator.buffer(0);
      assertNotNull("allocation failed", arrowBuf);
      assertEquals("capacity was non-zero", 0, arrowBuf.capacity());
      assertTrue("address should be valid", arrowBuf.memoryAddress() != 0);
      arrowBuf.getReferenceManager().release();
    }
  }

  @Ignore // TODO(DRILL-2740)
  @Test(expected = IllegalStateException.class)
  public void testAllocator_unreleasedEmpty() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      @SuppressWarnings("unused")
      final ArrowBuf arrowBuf = rootAllocator.buffer(0);
    }
  }

  @Test
  public void testAllocator_transferOwnership() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator("changeOwnership1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 =
          rootAllocator.newChildAllocator("changeOwnership2", 0, MAX_ALLOCATION);

      final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verify();
      final ReferenceManager referenceManager = arrowBuf1.getReferenceManager();
      OwnershipTransferResult transferOwnership = referenceManager.transferOwnership(arrowBuf1, childAllocator2);
      assertEquiv(arrowBuf1, transferOwnership.getTransferredBuffer());
      final boolean allocationFit = transferOwnership.getAllocationFit();
      rootAllocator.verify();
      assertTrue(allocationFit);

      arrowBuf1.getReferenceManager().release();
      childAllocator1.close();
      rootAllocator.verify();

      transferOwnership.getTransferredBuffer().getReferenceManager().release();
      childAllocator2.close();
    }
  }

  static <T> boolean equalsIgnoreOrder(Collection<T> c1, Collection<T> c2) {
    return (c1.size() == c2.size() && c1.containsAll(c2));
  }

  @Test
  public void testAllocator_getParentAndChild() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      assertEquals(rootAllocator.getParentAllocator(), null);

      try (final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator("child1", 0, MAX_ALLOCATION)) {
        assertEquals(childAllocator1.getParentAllocator(), rootAllocator);
        assertTrue(
            equalsIgnoreOrder(Arrays.asList(childAllocator1), rootAllocator.getChildAllocators()));

        try (final BufferAllocator childAllocator2 =
            rootAllocator.newChildAllocator("child2", 0, MAX_ALLOCATION)) {
          assertEquals(childAllocator2.getParentAllocator(), rootAllocator);
          assertTrue(equalsIgnoreOrder(Arrays.asList(childAllocator1, childAllocator2),
              rootAllocator.getChildAllocators()));

          try (final BufferAllocator grandChildAllocator =
              childAllocator1.newChildAllocator("grand-child", 0, MAX_ALLOCATION)) {
            assertEquals(grandChildAllocator.getParentAllocator(), childAllocator1);
            assertTrue(equalsIgnoreOrder(Arrays.asList(grandChildAllocator),
                childAllocator1.getChildAllocators()));
          }
        }
      }
    }
  }

  @Test
  public void testAllocator_childRemovedOnClose() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator1 =
          rootAllocator.newChildAllocator("child1", 0, MAX_ALLOCATION)) {
        try (final BufferAllocator childAllocator2 =
            rootAllocator.newChildAllocator("child2", 0, MAX_ALLOCATION)) {

          // root has two child allocators
          assertTrue(equalsIgnoreOrder(Arrays.asList(childAllocator1, childAllocator2),
              rootAllocator.getChildAllocators()));

          try (final BufferAllocator grandChildAllocator =
              childAllocator1.newChildAllocator("grand-child", 0, MAX_ALLOCATION)) {

            // child1 has one allocator i.e grand-child
            assertTrue(equalsIgnoreOrder(Arrays.asList(grandChildAllocator),
                childAllocator1.getChildAllocators()));
          }

          // grand-child closed
          assertTrue(
              equalsIgnoreOrder(Collections.EMPTY_SET, childAllocator1.getChildAllocators()));
        }
        // root has only one child left
        assertTrue(
            equalsIgnoreOrder(Arrays.asList(childAllocator1), rootAllocator.getChildAllocators()));
      }
      // all child allocators closed.
      assertTrue(equalsIgnoreOrder(Collections.EMPTY_SET, rootAllocator.getChildAllocators()));
    }
  }

  @Test
  public void testAllocator_shareOwnership() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("shareOwnership1", 0,
          MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("shareOwnership2", 0,
          MAX_ALLOCATION);
      final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 4);
      rootAllocator.verify();

      // share ownership of buffer.
      final ArrowBuf arrowBuf2 = arrowBuf1.getReferenceManager().retain(arrowBuf1, childAllocator2);
      rootAllocator.verify();
      assertNotNull(arrowBuf2);
      assertNotEquals(arrowBuf2, arrowBuf1);
      assertEquiv(arrowBuf1, arrowBuf2);

      // release original buffer (thus transferring ownership to allocator 2. (should leave
      // allocator 1 in empty state)
      arrowBuf1.getReferenceManager().release();
      rootAllocator.verify();
      childAllocator1.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator3 = rootAllocator.newChildAllocator("shareOwnership3", 0,
          MAX_ALLOCATION);
      final ArrowBuf arrowBuf3 = arrowBuf1.getReferenceManager().retain(arrowBuf1, childAllocator3);
      assertNotNull(arrowBuf3);
      assertNotEquals(arrowBuf3, arrowBuf1);
      assertNotEquals(arrowBuf3, arrowBuf2);
      assertEquiv(arrowBuf1, arrowBuf3);
      rootAllocator.verify();

      arrowBuf2.getReferenceManager().release();
      rootAllocator.verify();
      childAllocator2.close();
      rootAllocator.verify();

      arrowBuf3.getReferenceManager().release();
      rootAllocator.verify();
      childAllocator3.close();
    }
  }

  @Test
  public void testRootAllocator_createChildAndUse() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator(
          "createChildAndUse", 0, MAX_ALLOCATION)) {
        final ArrowBuf arrowBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", arrowBuf);
        arrowBuf.getReferenceManager().release();
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testRootAllocator_createChildDontClose() throws Exception {
    try {
      try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
        final BufferAllocator childAllocator = rootAllocator.newChildAllocator(
            "createChildDontClose", 0, MAX_ALLOCATION);
        final ArrowBuf arrowBuf = childAllocator.buffer(512);
        assertNotNull("allocation failed", arrowBuf);
      }
    } finally {
      /*
       * We expect one underlying buffer because we closed a child allocator without
       * releasing the buffer allocated from it.
       */
      /*
      // ------------------------------- DEBUG ---------------------------------
      final int bufferCount = UnsafeDirectLittleEndian.getBufferCount();
      UnsafeDirectLittleEndian.releaseBuffers();
      assertEquals(1, bufferCount);
      // ------------------------------- DEBUG ---------------------------------
      */
    }
  }

  @Test
  public void testSegmentAllocator() {
    RoundingPolicy policy = new SegmentRoundingPolicy(1024);
    try (RootAllocator allocator = new RootAllocator(AllocationListener.NOOP, 1024 * 1024, policy)) {
      ArrowBuf buf = allocator.buffer(798);
      assertEquals(1024, buf.capacity());
      buf.setInt(333, 959);
      assertEquals(959, buf.getInt(333));
      buf.close();

      buf = allocator.buffer(1025);
      assertEquals(2048, buf.capacity());
      buf.setInt(193, 939);
      assertEquals(939, buf.getInt(193));
      buf.close();
    }
  }

  @Test
  public void testSegmentAllocator_childAllocator() {
    RoundingPolicy policy = new SegmentRoundingPolicy(1024);
    try (RootAllocator allocator = new RootAllocator(AllocationListener.NOOP, 1024 * 1024, policy);
        BufferAllocator childAllocator = allocator.newChildAllocator("child", 0, 512 * 1024)) {

      assertEquals("child", childAllocator.getName());

      ArrowBuf buf = childAllocator.buffer(798);
      assertEquals(1024, buf.capacity());
      buf.setInt(333, 959);
      assertEquals(959, buf.getInt(333));
      buf.close();

      buf = childAllocator.buffer(1025);
      assertEquals(2048, buf.capacity());
      buf.setInt(193, 939);
      assertEquals(939, buf.getInt(193));
      buf.close();
    }
  }

  @Test
  public void testSegmentAllocator_smallSegment() {
    IllegalArgumentException e = Assertions.assertThrows(
            IllegalArgumentException.class,
        () -> new SegmentRoundingPolicy(128)
    );
    assertEquals("The segment size cannot be smaller than 1024", e.getMessage());
  }

  @Test
  public void testSegmentAllocator_segmentSizeNotPowerOf2() {
    IllegalArgumentException e = Assertions.assertThrows(
            IllegalArgumentException.class,
        () -> new SegmentRoundingPolicy(4097)
    );
    assertEquals("The segment size must be a power of 2", e.getMessage());
  }

  @Test
  public void testCustomizedAllocationManager() {
    try (BaseAllocator allocator = createAllocatorWithCustomizedAllocationManager()) {
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

  private BaseAllocator createAllocatorWithCustomizedAllocationManager() {
    return new RootAllocator(BaseAllocator.configBuilder()
        .maxAllocation(MAX_ALLOCATION)
        .allocationManagerFactory(new AllocationManager.Factory() {
          @Override
          public AllocationManager create(BufferAllocator accountingAllocator, long requestedSize) {
            return new AllocationManager(accountingAllocator) {
              private final Unsafe unsafe = getUnsafe();
              private final long address = unsafe.allocateMemory(requestedSize);

              @Override
              protected long memoryAddress() {
                return address;
              }

              @Override
              protected void release0() {
                unsafe.setMemory(address, requestedSize, (byte) 0);
                unsafe.freeMemory(address);
              }

              @Override
              public long getSize() {
                return requestedSize;
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
            };
          }

          @Override
          public ArrowBuf empty() {
            return null;
          }
        }).build());
  }

  @Test
  public void testRootAllocator_listeners() throws Exception {
    CountingAllocationListener l1 = new CountingAllocationListener();
    assertEquals(0, l1.getNumPreCalls());
    assertEquals(0, l1.getNumCalls());
    assertEquals(0, l1.getNumReleaseCalls());
    assertEquals(0, l1.getNumChildren());
    assertEquals(0, l1.getTotalMem());
    CountingAllocationListener l2 = new CountingAllocationListener();
    assertEquals(0, l2.getNumPreCalls());
    assertEquals(0, l2.getNumCalls());
    assertEquals(0, l2.getNumReleaseCalls());
    assertEquals(0, l2.getNumChildren());
    assertEquals(0, l2.getTotalMem());
    // root and first-level child share the first listener
    // second-level and third-level child share the second listener
    try (final RootAllocator rootAllocator = new RootAllocator(l1, MAX_ALLOCATION)) {
      try (final BufferAllocator c1 = rootAllocator.newChildAllocator("c1", 0, MAX_ALLOCATION)) {
        assertEquals(1, l1.getNumChildren());
        final ArrowBuf buf1 = c1.buffer(16);
        assertNotNull("allocation failed", buf1);
        assertEquals(1, l1.getNumPreCalls());
        assertEquals(1, l1.getNumCalls());
        assertEquals(0, l1.getNumReleaseCalls());
        assertEquals(16, l1.getTotalMem());
        buf1.getReferenceManager().release();
        try (final BufferAllocator c2 = c1.newChildAllocator("c2", l2, 0, MAX_ALLOCATION)) {
          assertEquals(2, l1.getNumChildren()); // c1 got a new child, so c1's listener (l1) is notified
          assertEquals(0, l2.getNumChildren());
          final ArrowBuf buf2 = c2.buffer(32);
          assertNotNull("allocation failed", buf2);
          assertEquals(1, l1.getNumCalls());
          assertEquals(16, l1.getTotalMem());
          assertEquals(1, l2.getNumPreCalls());
          assertEquals(1, l2.getNumCalls());
          assertEquals(0, l2.getNumReleaseCalls());
          assertEquals(32, l2.getTotalMem());
          buf2.getReferenceManager().release();
          try (final BufferAllocator c3 = c2.newChildAllocator("c3", 0, MAX_ALLOCATION)) {
            assertEquals(2, l1.getNumChildren());
            assertEquals(1, l2.getNumChildren());
            final ArrowBuf buf3 = c3.buffer(64);
            assertNotNull("allocation failed", buf3);
            assertEquals(1, l1.getNumPreCalls());
            assertEquals(1, l1.getNumCalls());
            assertEquals(1, l1.getNumReleaseCalls());
            assertEquals(16, l1.getTotalMem());
            assertEquals(2, l2.getNumPreCalls());
            assertEquals(2, l2.getNumCalls());
            assertEquals(1, l2.getNumReleaseCalls());
            assertEquals(32 + 64, l2.getTotalMem());
            buf3.getReferenceManager().release();
          }
          assertEquals(2, l1.getNumChildren());
          assertEquals(0, l2.getNumChildren()); // third-level child removed
        }
        assertEquals(1, l1.getNumChildren()); // second-level child removed
        assertEquals(0, l2.getNumChildren());
      }
      assertEquals(0, l1.getNumChildren()); // first-level child removed

      assertEquals(2, l2.getNumReleaseCalls());
    }
  }

  @Test
  public void testRootAllocator_listenerAllocationFail() throws Exception {
    CountingAllocationListener l1 = new CountingAllocationListener();
    assertEquals(0, l1.getNumCalls());
    assertEquals(0, l1.getTotalMem());
    // Test attempts to allocate too much from a child whose limit is set to half of the max
    // allocation. The listener's callback triggers, expanding the child allocator's limit, so then
    // the allocation succeeds.
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator c1 = rootAllocator.newChildAllocator("c1", l1, 0,
          MAX_ALLOCATION / 2)) {
        try {
          c1.buffer(MAX_ALLOCATION);
          fail("allocated memory beyond max allowed");
        } catch (OutOfMemoryException e) {
          // expected
        }
        assertEquals(0, l1.getNumCalls());
        assertEquals(0, l1.getTotalMem());

        l1.setExpandOnFail(c1, MAX_ALLOCATION);
        ArrowBuf arrowBuf = c1.buffer(MAX_ALLOCATION);
        assertNotNull("allocation failed", arrowBuf);
        assertEquals(1, l1.getNumCalls());
        assertEquals(MAX_ALLOCATION, l1.getTotalMem());
        arrowBuf.getReferenceManager().release();
      }
    }
  }

  private static void allocateAndFree(final BufferAllocator allocator) {
    final ArrowBuf arrowBuf = allocator.buffer(512);
    assertNotNull("allocation failed", arrowBuf);
    arrowBuf.getReferenceManager().release();

    final ArrowBuf arrowBuf2 = allocator.buffer(MAX_ALLOCATION);
    assertNotNull("allocation failed", arrowBuf2);
    arrowBuf2.getReferenceManager().release();

    final int nBufs = 8;
    final ArrowBuf[] arrowBufs = new ArrowBuf[nBufs];
    for (int i = 0; i < arrowBufs.length; ++i) {
      ArrowBuf arrowBufi = allocator.buffer(MAX_ALLOCATION / nBufs);
      assertNotNull("allocation failed", arrowBufi);
      arrowBufs[i] = arrowBufi;
    }
    for (ArrowBuf arrowBufi : arrowBufs) {
      arrowBufi.getReferenceManager().release();
    }
  }

  @Test
  public void testAllocator_manyAllocations() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator =
               rootAllocator.newChildAllocator("manyAllocations", 0, MAX_ALLOCATION)) {
        allocateAndFree(childAllocator);
      }
    }
  }

  @Test
  public void testAllocator_overAllocate() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator =
               rootAllocator.newChildAllocator("overAllocate", 0, MAX_ALLOCATION)) {
        allocateAndFree(childAllocator);

        try {
          childAllocator.buffer(MAX_ALLOCATION + 1);
          fail("allocated memory beyond max allowed");
        } catch (OutOfMemoryException e) {
          // expected
        }
      }
    }
  }

  @Test
  public void testAllocator_overAllocateParent() throws Exception {
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator =
               rootAllocator.newChildAllocator("overAllocateParent", 0, MAX_ALLOCATION)) {
        final ArrowBuf arrowBuf1 = rootAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", arrowBuf1);
        final ArrowBuf arrowBuf2 = childAllocator.buffer(MAX_ALLOCATION / 2);
        assertNotNull("allocation failed", arrowBuf2);

        try {
          childAllocator.buffer(MAX_ALLOCATION / 4);
          fail("allocated memory beyond max allowed");
        } catch (OutOfMemoryException e) {
          // expected
        }

        arrowBuf1.getReferenceManager().release();
        arrowBuf2.getReferenceManager().release();
      }
    }
  }

  @Test
  public void testAllocator_failureAtParentLimitOutcomeDetails() throws Exception {
    try (final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("child", 0, MAX_ALLOCATION / 2)) {
        try (final BufferAllocator grandChildAllocator =
            childAllocator.newChildAllocator("grandchild", MAX_ALLOCATION / 4, MAX_ALLOCATION)) {
          OutOfMemoryException e = assertThrows(OutOfMemoryException.class,
              () -> grandChildAllocator.buffer(MAX_ALLOCATION));
          // expected
          assertTrue(e.getMessage().contains("Unable to allocate buffer"));

          assertTrue("missing outcome details", e.getOutcomeDetails().isPresent());
          AllocationOutcomeDetails outcomeDetails = e.getOutcomeDetails().get();

          assertEquals(outcomeDetails.getFailedAllocator(), childAllocator);

          // The order of allocators should be child to root (request propagates to parent if
          // child cannot satisfy the request).
          Iterator<Entry> iterator = outcomeDetails.allocEntries.iterator();
          AllocationOutcomeDetails.Entry first = iterator.next();
          assertEquals(MAX_ALLOCATION / 4, first.getAllocatedSize());
          assertEquals(MAX_ALLOCATION, first.getRequestedSize());
          assertEquals(false, first.isAllocationFailed());

          AllocationOutcomeDetails.Entry second = iterator.next();
          assertEquals(MAX_ALLOCATION - MAX_ALLOCATION / 4, second.getRequestedSize());
          assertEquals(0, second.getAllocatedSize());
          assertEquals(true, second.isAllocationFailed());

          assertFalse(iterator.hasNext());
        }
      }
    }
  }

  @Test
  public void testAllocator_failureAtRootLimitOutcomeDetails() throws Exception {
    try (final RootAllocator rootAllocator =
        new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator =
          rootAllocator.newChildAllocator("child", MAX_ALLOCATION / 2, Long.MAX_VALUE)) {
        try (final BufferAllocator grandChildAllocator =
            childAllocator.newChildAllocator("grandchild", MAX_ALLOCATION / 4, Long.MAX_VALUE)) {
          OutOfMemoryException e = assertThrows(OutOfMemoryException.class,
              () -> grandChildAllocator.buffer(MAX_ALLOCATION * 2));

          assertTrue(e.getMessage().contains("Unable to allocate buffer"));
          assertTrue("missing outcome details", e.getOutcomeDetails().isPresent());
          AllocationOutcomeDetails outcomeDetails = e.getOutcomeDetails().get();

          assertEquals(outcomeDetails.getFailedAllocator(), rootAllocator);

          // The order of allocators should be child to root (request propagates to parent if
          // child cannot satisfy the request).
          Iterator<Entry> iterator = outcomeDetails.allocEntries.iterator();
          AllocationOutcomeDetails.Entry first = iterator.next();
          assertEquals(MAX_ALLOCATION / 4, first.getAllocatedSize());
          assertEquals(2 * MAX_ALLOCATION, first.getRequestedSize());
          assertEquals(false, first.isAllocationFailed());

          AllocationOutcomeDetails.Entry second = iterator.next();
          assertEquals(MAX_ALLOCATION / 4, second.getAllocatedSize());
          assertEquals(2 * MAX_ALLOCATION - MAX_ALLOCATION / 4, second.getRequestedSize());
          assertEquals(false, second.isAllocationFailed());

          AllocationOutcomeDetails.Entry third = iterator.next();
          assertEquals(0, third.getAllocatedSize());
          assertEquals(true, third.isAllocationFailed());

          assertFalse(iterator.hasNext());
        }
      }
    }
  }

  private static void testAllocator_sliceUpBufferAndRelease(
      final RootAllocator rootAllocator, final BufferAllocator bufferAllocator) {
    final ArrowBuf arrowBuf1 = bufferAllocator.buffer(MAX_ALLOCATION / 2);
    rootAllocator.verify();

    final ArrowBuf arrowBuf2 = arrowBuf1.slice(16, arrowBuf1.capacity() - 32);
    rootAllocator.verify();
    final ArrowBuf arrowBuf3 = arrowBuf2.slice(16, arrowBuf2.capacity() - 32);
    rootAllocator.verify();
    @SuppressWarnings("unused")
    final ArrowBuf arrowBuf4 = arrowBuf3.slice(16, arrowBuf3.capacity() - 32);
    rootAllocator.verify();

    arrowBuf3.getReferenceManager().release(); // since they share refcounts, one is enough to release them all
    rootAllocator.verify();
  }

  @Test
  public void testAllocator_createSlices() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createSlices", 0,
          MAX_ALLOCATION)) {
        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verify();

      testAllocator_sliceUpBufferAndRelease(rootAllocator, rootAllocator);

      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator("createSlices", 0,
          MAX_ALLOCATION)) {
        try (final BufferAllocator childAllocator2 =
                 childAllocator.newChildAllocator("createSlices", 0, MAX_ALLOCATION)) {
          final ArrowBuf arrowBuf1 = childAllocator2.buffer(MAX_ALLOCATION / 8);
          @SuppressWarnings("unused")
          final ArrowBuf arrowBuf2 = arrowBuf1.slice(MAX_ALLOCATION / 16, MAX_ALLOCATION / 16);
          testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
          arrowBuf1.getReferenceManager().release();
          rootAllocator.verify();
        }
        rootAllocator.verify();

        testAllocator_sliceUpBufferAndRelease(rootAllocator, childAllocator);
      }
      rootAllocator.verify();
    }
  }

  @Test
  public void testAllocator_sliceRanges() throws Exception {
    // final AllocatorOwner allocatorOwner = new NamedOwner("sliceRanges");
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      // Populate a buffer with byte values corresponding to their indices.
      final ArrowBuf arrowBuf = rootAllocator.buffer(256);
      assertEquals(256, arrowBuf.capacity());
      assertEquals(0, arrowBuf.readerIndex());
      assertEquals(0, arrowBuf.readableBytes());
      assertEquals(0, arrowBuf.writerIndex());
      assertEquals(256, arrowBuf.writableBytes());

      final ArrowBuf slice3 = arrowBuf.slice();
      assertEquals(0, slice3.readerIndex());
      assertEquals(0, slice3.readableBytes());
      assertEquals(0, slice3.writerIndex());
      // assertEquals(256, slice3.capacity());
      // assertEquals(256, slice3.writableBytes());

      for (int i = 0; i < 256; ++i) {
        arrowBuf.writeByte(i);
      }
      assertEquals(0, arrowBuf.readerIndex());
      assertEquals(256, arrowBuf.readableBytes());
      assertEquals(256, arrowBuf.writerIndex());
      assertEquals(0, arrowBuf.writableBytes());

      final ArrowBuf slice1 = arrowBuf.slice();
      assertEquals(0, slice1.readerIndex());
      assertEquals(256, slice1.readableBytes());
      for (int i = 0; i < 10; ++i) {
        assertEquals(i, slice1.readByte());
      }
      assertEquals(256 - 10, slice1.readableBytes());
      for (int i = 0; i < 256; ++i) {
        assertEquals((byte) i, slice1.getByte(i));
      }

      final ArrowBuf slice2 = arrowBuf.slice(25, 25);
      assertEquals(0, slice2.readerIndex());
      assertEquals(25, slice2.readableBytes());
      for (int i = 25; i < 50; ++i) {
        assertEquals(i, slice2.readByte());
      }

      /*
      for(int i = 256; i > 0; --i) {
        slice3.writeByte(i - 1);
      }
      for(int i = 0; i < 256; ++i) {
        assertEquals(255 - i, slice1.getByte(i));
      }
      */

      arrowBuf.getReferenceManager().release(); // all the derived buffers share this fate
    }
  }

  @Test
  public void testAllocator_slicesOfSlices() throws Exception {
    // final AllocatorOwner allocatorOwner = new NamedOwner("slicesOfSlices");
    try (final RootAllocator rootAllocator =
             new RootAllocator(MAX_ALLOCATION)) {
      // Populate a buffer with byte values corresponding to their indices.
      final ArrowBuf arrowBuf = rootAllocator.buffer(256);
      for (int i = 0; i < 256; ++i) {
        arrowBuf.writeByte(i);
      }

      // Slice it up.
      final ArrowBuf slice0 = arrowBuf.slice(0, arrowBuf.capacity());
      for (int i = 0; i < 256; ++i) {
        assertEquals((byte) i, arrowBuf.getByte(i));
      }

      final ArrowBuf slice10 = slice0.slice(10, arrowBuf.capacity() - 10);
      for (int i = 10; i < 256; ++i) {
        assertEquals((byte) i, slice10.getByte(i - 10));
      }

      final ArrowBuf slice20 = slice10.slice(10, arrowBuf.capacity() - 20);
      for (int i = 20; i < 256; ++i) {
        assertEquals((byte) i, slice20.getByte(i - 20));
      }

      final ArrowBuf slice30 = slice20.slice(10, arrowBuf.capacity() - 30);
      for (int i = 30; i < 256; ++i) {
        assertEquals((byte) i, slice30.getByte(i - 30));
      }

      arrowBuf.getReferenceManager().release();
    }
  }

  @Test
  public void testAllocator_transferSliced() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferSliced1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferSliced2", 0, MAX_ALLOCATION);

      final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final ArrowBuf arrowBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final ArrowBuf arrowBuf1s = arrowBuf1.slice(0, arrowBuf1.capacity() / 2);
      final ArrowBuf arrowBuf2s = arrowBuf2.slice(0, arrowBuf2.capacity() / 2);

      rootAllocator.verify();

      OwnershipTransferResult result1 = arrowBuf2s.getReferenceManager().transferOwnership(arrowBuf2s, childAllocator1);
      assertEquiv(arrowBuf2s, result1.getTransferredBuffer());
      rootAllocator.verify();
      OwnershipTransferResult result2 = arrowBuf1s.getReferenceManager().transferOwnership(arrowBuf1s, childAllocator2);
      assertEquiv(arrowBuf1s, result2.getTransferredBuffer());
      rootAllocator.verify();

      result1.getTransferredBuffer().getReferenceManager().release();
      result2.getTransferredBuffer().getReferenceManager().release();

      arrowBuf1s.getReferenceManager().release(); // releases arrowBuf1
      arrowBuf2s.getReferenceManager().release(); // releases arrowBuf2

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_shareSliced() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferSliced", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferSliced", 0, MAX_ALLOCATION);

      final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);
      final ArrowBuf arrowBuf2 = childAllocator2.buffer(MAX_ALLOCATION / 8);

      final ArrowBuf arrowBuf1s = arrowBuf1.slice(0, arrowBuf1.capacity() / 2);
      final ArrowBuf arrowBuf2s = arrowBuf2.slice(0, arrowBuf2.capacity() / 2);

      rootAllocator.verify();

      final ArrowBuf arrowBuf2s1 = arrowBuf2s.getReferenceManager().retain(arrowBuf2s, childAllocator1);
      assertEquiv(arrowBuf2s, arrowBuf2s1);
      final ArrowBuf arrowBuf1s2 = arrowBuf1s.getReferenceManager().retain(arrowBuf1s, childAllocator2);
      assertEquiv(arrowBuf1s, arrowBuf1s2);
      rootAllocator.verify();

      arrowBuf1s.getReferenceManager().release(); // releases arrowBuf1
      arrowBuf2s.getReferenceManager().release(); // releases arrowBuf2
      rootAllocator.verify();

      arrowBuf2s1.getReferenceManager().release(); // releases the shared arrowBuf2 slice
      arrowBuf1s2.getReferenceManager().release(); // releases the shared arrowBuf1 slice

      childAllocator1.close();
      childAllocator2.close();
    }
  }

  @Test
  public void testAllocator_transferShared() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator("transferShared1", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator2 = rootAllocator.newChildAllocator("transferShared2", 0, MAX_ALLOCATION);
      final BufferAllocator childAllocator3 = rootAllocator.newChildAllocator("transferShared3", 0, MAX_ALLOCATION);

      final ArrowBuf arrowBuf1 = childAllocator1.buffer(MAX_ALLOCATION / 8);

      boolean allocationFit;

      ArrowBuf arrowBuf2 = arrowBuf1.getReferenceManager().retain(arrowBuf1, childAllocator2);
      rootAllocator.verify();
      assertNotNull(arrowBuf2);
      assertNotEquals(arrowBuf2, arrowBuf1);
      assertEquiv(arrowBuf1, arrowBuf2);

      final ReferenceManager refManager1 = arrowBuf1.getReferenceManager();
      final OwnershipTransferResult result1 = refManager1.transferOwnership(arrowBuf1, childAllocator3);
      allocationFit = result1.getAllocationFit();
      final ArrowBuf arrowBuf3 = result1.getTransferredBuffer();
      assertTrue(allocationFit);
      assertEquiv(arrowBuf1, arrowBuf3);
      rootAllocator.verify();

      // Since childAllocator3 now has childAllocator1's buffer, 1, can close
      arrowBuf1.getReferenceManager().release();
      childAllocator1.close();
      rootAllocator.verify();

      arrowBuf2.getReferenceManager().release();
      childAllocator2.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator4 = rootAllocator.newChildAllocator("transferShared4", 0, MAX_ALLOCATION);
      final ReferenceManager refManager3 = arrowBuf3.getReferenceManager();
      final OwnershipTransferResult result3 = refManager3.transferOwnership(arrowBuf3, childAllocator4);
      allocationFit = result3.getAllocationFit();
      final ArrowBuf arrowBuf4 = result3.getTransferredBuffer();
      assertTrue(allocationFit);
      assertEquiv(arrowBuf3, arrowBuf4);
      rootAllocator.verify();

      arrowBuf3.getReferenceManager().release();
      childAllocator3.close();
      rootAllocator.verify();

      arrowBuf4.getReferenceManager().release();
      childAllocator4.close();
      rootAllocator.verify();
    }
  }

  @Test
  public void testAllocator_unclaimedReservation() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator1 =
               rootAllocator.newChildAllocator("unclaimedReservation", 0, MAX_ALLOCATION)) {
        try (final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(64));
        }
        rootAllocator.verify();
      }
    }
  }

  @Test
  public void testAllocator_claimedReservation() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {

      try (final BufferAllocator childAllocator1 = rootAllocator.newChildAllocator(
          "claimedReservation", 0, MAX_ALLOCATION)) {

        try (final AllocationReservation reservation = childAllocator1.newReservation()) {
          assertTrue(reservation.add(32));
          assertTrue(reservation.add(32));

          final ArrowBuf arrowBuf = reservation.allocateBuffer();
          assertEquals(64, arrowBuf.capacity());
          rootAllocator.verify();

          arrowBuf.getReferenceManager().release();
          rootAllocator.verify();
        }
        rootAllocator.verify();
      }
    }
  }

  @Test
  public void testInitReservationAndLimit() throws Exception {
    try (final RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      try (final BufferAllocator childAllocator = rootAllocator.newChildAllocator(
              "child", 2048, 4096)) {
        assertEquals(2048, childAllocator.getInitReservation());
        assertEquals(4096, childAllocator.getLimit());
      }
    }
  }

  @Test
  public void multiple() throws Exception {
    final String owner = "test";
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      final int op = 100000;

      BufferAllocator frag1 = allocator.newChildAllocator(owner, 1500000, Long.MAX_VALUE);
      BufferAllocator frag2 = allocator.newChildAllocator(owner, 500000, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator11 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b11 = allocator11.buffer(1000000);

      allocator.verify();

      BufferAllocator allocator12 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b12 = allocator12.buffer(500000);

      allocator.verify();

      BufferAllocator allocator21 = frag1.newChildAllocator(owner, op, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator22 = frag2.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b22 = allocator22.buffer(2000000);

      allocator.verify();

      BufferAllocator frag3 = allocator.newChildAllocator(owner, 1000000, Long.MAX_VALUE);

      allocator.verify();

      BufferAllocator allocator31 = frag3.newChildAllocator(owner, op, Long.MAX_VALUE);
      ArrowBuf b31a = allocator31.buffer(200000);

      allocator.verify();

      // Previously running operator completes
      b22.getReferenceManager().release();

      allocator.verify();

      allocator22.close();

      b31a.getReferenceManager().release();
      allocator31.close();

      b12.getReferenceManager().release();
      allocator12.close();

      allocator21.close();

      b11.getReferenceManager().release();
      allocator11.close();

      frag1.close();
      frag2.close();
      frag3.close();

    }
  }

  // This test needs to run in non-debug mode. So disabling the assertion status through class loader for this.
  // The test passes if run individually with -Dtest=TestBaseAllocator#testMemoryLeakWithReservation
  // but fails generally since the assertion status cannot be changed once the class is initialized.
  // So setting the test to @ignore
  @Test(expected = IllegalStateException.class)
  @Ignore
  public void testMemoryLeakWithReservation() throws Exception {
    // disabling assertion status
    AssertionUtil.class.getClassLoader().setClassAssertionStatus(AssertionUtil.class.getName(), false);
    try (RootAllocator rootAllocator = new RootAllocator(MAX_ALLOCATION)) {
      ChildAllocator childAllocator1 = (ChildAllocator) rootAllocator.newChildAllocator(
          "child1", 1024, MAX_ALLOCATION);
      rootAllocator.verify();

      ChildAllocator childAllocator2 = (ChildAllocator) childAllocator1.newChildAllocator(
          "child2", 1024, MAX_ALLOCATION);
      rootAllocator.verify();

      ArrowBuf buff = childAllocator2.buffer(256);

      Exception exception = assertThrows(IllegalStateException.class, () -> {
        childAllocator2.close();
      });
      String exMessage = exception.getMessage();
      assertTrue(exMessage.contains("Memory leaked: (256)"));

      exception = assertThrows(IllegalStateException.class, () -> {
        childAllocator1.close();
      });
      exMessage = exception.getMessage();
      assertTrue(exMessage.contains("Memory leaked: (256)"));
    }
  }

  public void assertEquiv(ArrowBuf origBuf, ArrowBuf newBuf) {
    assertEquals(origBuf.readerIndex(), newBuf.readerIndex());
    assertEquals(origBuf.writerIndex(), newBuf.writerIndex());
  }
}
