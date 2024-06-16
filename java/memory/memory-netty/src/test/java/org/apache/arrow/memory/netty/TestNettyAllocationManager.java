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
package org.apache.arrow.memory.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Test cases for {@link NettyAllocationManager}. */
public class TestNettyAllocationManager {

  static int CUSTOMIZED_ALLOCATION_CUTOFF_VALUE = 1024;

  private RootAllocator createCustomizedAllocator() {
    return new RootAllocator(
        RootAllocator.configBuilder()
            .allocationManagerFactory(
                new AllocationManager.Factory() {
                  @Override
                  public AllocationManager create(BufferAllocator accountingAllocator, long size) {
                    return new NettyAllocationManager(
                        accountingAllocator, size, CUSTOMIZED_ALLOCATION_CUTOFF_VALUE);
                  }

                  @Override
                  public ArrowBuf empty() {
                    return null;
                  }
                })
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

  /** Test the allocation strategy for small buffers.. */
  @Test
  public void testSmallBufferAllocation() {
    final long bufSize = CUSTOMIZED_ALLOCATION_CUTOFF_VALUE - 512L;
    try (RootAllocator allocator = createCustomizedAllocator();
        ArrowBuf buffer = allocator.buffer(bufSize)) {

      assertInstanceOf(BufferLedger.class, buffer.getReferenceManager());
      BufferLedger bufferLedger = (BufferLedger) buffer.getReferenceManager();

      // make sure we are using netty allocation manager
      AllocationManager allocMgr = bufferLedger.getAllocationManager();
      assertInstanceOf(NettyAllocationManager.class, allocMgr);
      NettyAllocationManager nettyMgr = (NettyAllocationManager) allocMgr;

      // for the small buffer allocation strategy, the chunk is not null
      assertNotNull(nettyMgr.getMemoryChunk());

      readWriteArrowBuf(buffer);
    }
  }

  /** Test the allocation strategy for large buffers.. */
  @Test
  public void testLargeBufferAllocation() {
    final long bufSize = CUSTOMIZED_ALLOCATION_CUTOFF_VALUE + 1024L;
    try (RootAllocator allocator = createCustomizedAllocator();
        ArrowBuf buffer = allocator.buffer(bufSize)) {
      assertInstanceOf(BufferLedger.class, buffer.getReferenceManager());
      BufferLedger bufferLedger = (BufferLedger) buffer.getReferenceManager();

      // make sure we are using netty allocation manager
      AllocationManager allocMgr = bufferLedger.getAllocationManager();
      assertInstanceOf(NettyAllocationManager.class, allocMgr);
      NettyAllocationManager nettyMgr = (NettyAllocationManager) allocMgr;

      // for the large buffer allocation strategy, the chunk is null
      assertNull(nettyMgr.getMemoryChunk());

      readWriteArrowBuf(buffer);
    }
  }
}
