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

import org.junit.Test;

/**
 * Test cases for {@link UnsafeAllocationManager}.
 */
public class TestUnsafeAllocationManager {

  private BaseAllocator createUnsafeAllocator() {
    return new RootAllocator(BaseAllocator.configBuilder().allocationManagerFactory(UnsafeAllocationManager.FACTORY)
        .build());
  }

  private void readWriteArrowBuf(ArrowBuf buffer) {
    // write buffer
    for (long i = 0; i < buffer.capacity() / 8; i++) {
      buffer.setLong(i * 8, i);
    }

    // read buffer
    for (long i = 0; i < buffer.capacity() / 8; i++) {
      long val = buffer.getLong(i * 8);
      assertEquals(i, val);
    }
  }

  /**
   * Test the memory allocation for {@link UnsafeAllocationManager}.
   */
  @Test
  public void testBufferAllocation() {
    final long bufSize = 4096L;
    try (BaseAllocator allocator = createUnsafeAllocator();
         ArrowBuf buffer = allocator.buffer(bufSize)) {
      assertTrue(buffer.getReferenceManager() instanceof BufferLedger);
      BufferLedger bufferLedger = (BufferLedger) buffer.getReferenceManager();

      // make sure we are using unsafe allocation manager
      AllocationManager allocMgr = bufferLedger.getAllocationManager();
      assertTrue(allocMgr instanceof UnsafeAllocationManager);
      UnsafeAllocationManager unsafeMgr = (UnsafeAllocationManager) allocMgr;

      assertEquals(bufSize, unsafeMgr.getSize());
      readWriteArrowBuf(buffer);
    }
  }
}
