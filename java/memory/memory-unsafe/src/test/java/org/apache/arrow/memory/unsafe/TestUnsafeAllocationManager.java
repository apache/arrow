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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Test cases for {@link UnsafeAllocationManager}. */
public class TestUnsafeAllocationManager {

  private BufferAllocator createUnsafeAllocator() {
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(UnsafeAllocationManager.FACTORY)
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

  /** Test the memory allocation for {@link UnsafeAllocationManager}. */
  @Test
  public void testBufferAllocation() {
    final long bufSize = 4096L;
    try (BufferAllocator allocator = createUnsafeAllocator();
        ArrowBuf buffer = allocator.buffer(bufSize)) {
      assertInstanceOf(BufferLedger.class, buffer.getReferenceManager());
      BufferLedger bufferLedger = (BufferLedger) buffer.getReferenceManager();

      // make sure we are using unsafe allocation manager
      AllocationManager allocMgr = bufferLedger.getAllocationManager();
      assertInstanceOf(UnsafeAllocationManager.class, allocMgr);
      UnsafeAllocationManager unsafeMgr = (UnsafeAllocationManager) allocMgr;

      assertEquals(bufSize, unsafeMgr.getSize());
      readWriteArrowBuf(buffer);
    }
  }
}
