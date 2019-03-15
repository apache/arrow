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

import org.junit.Ignore;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ArrowBuf.TransferResult;

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
        arrowBuf2.release();
      }

      arrowBuf1.release();
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
      arrowBuf.release();
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
      TransferResult transferOwnership = arrowBuf1.transferOwnership(childAllocator2);
      assertEquiv(arrowBuf1, transferOwnership.buffer);
      final boolean allocationFit = transferOwnership.allocationFit;
      rootAllocator.verify();
      assertTrue(allocationFit);

      arrowBuf1.release();
      childAllocator1.close();
      rootAllocator.verify();

      transferOwnership.buffer.release();
      childAllocator2.close();
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
      final ArrowBuf arrowBuf2 = arrowBuf1.retain(childAllocator2);
      rootAllocator.verify();
      assertNotNull(arrowBuf2);
      assertNotEquals(arrowBuf2, arrowBuf1);
      assertEquiv(arrowBuf1, arrowBuf2);

      // release original buffer (thus transferring ownership to allocator 2. (should leave
      // allocator 1 in empty state)
      arrowBuf1.release();
      rootAllocator.verify();
      childAllocator1.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator3 = rootAllocator.newChildAllocator("shareOwnership3", 0,
          MAX_ALLOCATION);
      final ArrowBuf arrowBuf3 = arrowBuf1.retain(childAllocator3);
      assertNotNull(arrowBuf3);
      assertNotEquals(arrowBuf3, arrowBuf1);
      assertNotEquals(arrowBuf3, arrowBuf2);
      assertEquiv(arrowBuf1, arrowBuf3);
      rootAllocator.verify();

      arrowBuf2.release();
      rootAllocator.verify();
      childAllocator2.close();
      rootAllocator.verify();

      arrowBuf3.release();
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
        arrowBuf.release();
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

  // Allocation listener
  // It counts the number of times it has been invoked, and how much memory allocation it has seen
  // When set to 'expand on fail', it attempts to expand the associated allocator's limit
  private static final class TestAllocationListener implements AllocationListener {
    private int numPreCalls;
    private int numCalls;
    private int numReleaseCalls;
    private int numChildren;
    private long totalMem;
    private boolean expandOnFail;
    BufferAllocator expandAlloc;
    long expandLimit;

    TestAllocationListener() {
      this.numCalls = 0;
      this.numChildren = 0;
      this.totalMem = 0;
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
    }

    @Override
    public boolean onFailedAllocation(long size,  AllocationOutcome outcome) {
      if (expandOnFail) {
        expandAlloc.setLimit(expandLimit);
        return true;
      }
      return false;
    }


    @Override
    public void onRelease(long size) {
      numReleaseCalls++;
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
  }

  @Test
  public void testRootAllocator_listeners() throws Exception {
    TestAllocationListener l1 = new TestAllocationListener();
    assertEquals(0, l1.getNumPreCalls());
    assertEquals(0, l1.getNumCalls());
    assertEquals(0, l1.getNumReleaseCalls());
    assertEquals(0, l1.getNumChildren());
    assertEquals(0, l1.getTotalMem());
    TestAllocationListener l2 = new TestAllocationListener();
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
        buf1.release();
        try (final BufferAllocator c2 = c1.newChildAllocator("c2", l2, 0, MAX_ALLOCATION)) {
          assertEquals(2, l1.getNumChildren());  // c1 got a new child, so c1's listener (l1) is notified
          assertEquals(0, l2.getNumChildren());
          final ArrowBuf buf2 = c2.buffer(32);
          assertNotNull("allocation failed", buf2);
          assertEquals(1, l1.getNumCalls());
          assertEquals(16, l1.getTotalMem());
          assertEquals(1, l2.getNumPreCalls());
          assertEquals(1, l2.getNumCalls());
          assertEquals(0, l2.getNumReleaseCalls());
          assertEquals(32, l2.getTotalMem());
          buf2.release();
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
            buf3.release();
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
    TestAllocationListener l1 = new TestAllocationListener();
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
        arrowBuf.release();
      }
    }
  }

  private static void allocateAndFree(final BufferAllocator allocator) {
    final ArrowBuf arrowBuf = allocator.buffer(512);
    assertNotNull("allocation failed", arrowBuf);
    arrowBuf.release();

    final ArrowBuf arrowBuf2 = allocator.buffer(MAX_ALLOCATION);
    assertNotNull("allocation failed", arrowBuf2);
    arrowBuf2.release();

    final int nBufs = 8;
    final ArrowBuf[] arrowBufs = new ArrowBuf[nBufs];
    for (int i = 0; i < arrowBufs.length; ++i) {
      ArrowBuf arrowBufi = allocator.buffer(MAX_ALLOCATION / nBufs);
      assertNotNull("allocation failed", arrowBufi);
      arrowBufs[i] = arrowBufi;
    }
    for (ArrowBuf arrowBufi : arrowBufs) {
      arrowBufi.release();
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

        arrowBuf1.release();
        arrowBuf2.release();
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

    arrowBuf3.release(); // since they share refcounts, one is enough to release them all
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
          arrowBuf1.release();
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

      arrowBuf.release();  // all the derived buffers share this fate
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

      arrowBuf.release();
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

      TransferResult result1 = arrowBuf2s.transferOwnership(childAllocator1);
      assertEquiv(arrowBuf2s, result1.buffer);
      rootAllocator.verify();
      TransferResult result2 = arrowBuf1s.transferOwnership(childAllocator2);
      assertEquiv(arrowBuf1s, result2.buffer);
      rootAllocator.verify();

      result1.buffer.release();
      result2.buffer.release();

      arrowBuf1s.release(); // releases arrowBuf1
      arrowBuf2s.release(); // releases arrowBuf2

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

      final ArrowBuf arrowBuf2s1 = arrowBuf2s.retain(childAllocator1);
      assertEquiv(arrowBuf2s, arrowBuf2s1);
      final ArrowBuf arrowBuf1s2 = arrowBuf1s.retain(childAllocator2);
      assertEquiv(arrowBuf1s, arrowBuf1s2);
      rootAllocator.verify();

      arrowBuf1s.release(); // releases arrowBuf1
      arrowBuf2s.release(); // releases arrowBuf2
      rootAllocator.verify();

      arrowBuf2s1.release(); // releases the shared arrowBuf2 slice
      arrowBuf1s2.release(); // releases the shared arrowBuf1 slice

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

      ArrowBuf arrowBuf2 = arrowBuf1.retain(childAllocator2);
      rootAllocator.verify();
      assertNotNull(arrowBuf2);
      assertNotEquals(arrowBuf2, arrowBuf1);
      assertEquiv(arrowBuf1, arrowBuf2);

      TransferResult result = arrowBuf1.transferOwnership(childAllocator3);
      allocationFit = result.allocationFit;
      final ArrowBuf arrowBuf3 = result.buffer;
      assertTrue(allocationFit);
      assertEquiv(arrowBuf1, arrowBuf3);
      rootAllocator.verify();

      // Since childAllocator3 now has childAllocator1's buffer, 1, can close
      arrowBuf1.release();
      childAllocator1.close();
      rootAllocator.verify();

      arrowBuf2.release();
      childAllocator2.close();
      rootAllocator.verify();

      final BufferAllocator childAllocator4 = rootAllocator.newChildAllocator("transferShared4", 0, MAX_ALLOCATION);
      TransferResult result2 = arrowBuf3.transferOwnership(childAllocator4);
      allocationFit = result.allocationFit;
      final ArrowBuf arrowBuf4 = result2.buffer;
      assertTrue(allocationFit);
      assertEquiv(arrowBuf3, arrowBuf4);
      rootAllocator.verify();

      arrowBuf3.release();
      childAllocator3.close();
      rootAllocator.verify();

      arrowBuf4.release();
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

          arrowBuf.release();
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
      b22.release();

      allocator.verify();

      allocator22.close();

      b31a.release();
      allocator31.close();

      b12.release();
      allocator12.close();

      allocator21.close();

      b11.release();
      allocator11.close();

      frag1.close();
      frag2.close();
      frag3.close();

    }
  }

  public void assertEquiv(ArrowBuf origBuf, ArrowBuf newBuf) {
    assertEquals(origBuf.readerIndex(), newBuf.readerIndex());
    assertEquals(origBuf.writerIndex(), newBuf.writerIndex());
  }
}
