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

package org.apache.arrow.vector;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests that OversizedAllocationException occurs when a large memory is allocated for a vector.
 * Typically, arrow allows the allocation of the size of at most Integer.MAX_VALUE, but this might cause OOM in tests.
 * Thus, the max allocation size is limited to 1 KB in this class. Please see the surefire option in pom.xml.
 */
public class TestOversizedAllocationForValueVector {

  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testFixedVectorReallocation() {
    assertThrows(OversizedAllocationException.class, () -> {
      final UInt4Vector vector = new UInt4Vector(EMPTY_SCHEMA_PATH, allocator);
      // edge case 1: buffer size = max value capacity
      final int expectedValueCapacity = checkedCastToInt(BaseValueVector.MAX_ALLOCATION_SIZE / 4);
      try {
        vector.allocateNew(expectedValueCapacity);
        assertEquals(expectedValueCapacity, vector.getValueCapacity());
        vector.reAlloc();
        assertEquals(expectedValueCapacity * 2, vector.getValueCapacity());
      } finally {
        vector.close();
      }

      // common case: value count < max value capacity
      try {
        vector.allocateNew(checkedCastToInt(BaseValueVector.MAX_ALLOCATION_SIZE / 8));
        vector.reAlloc(); // value allocation reaches to MAX_VALUE_ALLOCATION
        vector.reAlloc(); // this should throw an IOOB
      } finally {
        vector.close();
      }
    });
  }

  @Test
  public void testBitVectorReallocation() {
    assertThrows(OversizedAllocationException.class, () -> {
      final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator);
      // edge case 1: buffer size ~ max value capacity
      final int expectedValueCapacity = 1 << 29;
      try {
        vector.allocateNew(expectedValueCapacity);
        assertEquals(expectedValueCapacity, vector.getValueCapacity());
        vector.reAlloc();
        assertEquals(expectedValueCapacity * 2, vector.getValueCapacity());
      } finally {
        vector.close();
      }

      // common: value count < MAX_VALUE_ALLOCATION
      try {
        vector.allocateNew(expectedValueCapacity);
        for (int i = 0; i < 3; i++) {
          vector.reAlloc(); // expand buffer size
        }
        assertEquals(Integer.MAX_VALUE, vector.getValueCapacity());
        vector.reAlloc(); // buffer size ~ max allocation
        assertEquals(Integer.MAX_VALUE, vector.getValueCapacity());
        vector.reAlloc(); // overflow
      } finally {
        vector.close();
      }
    });
  }


  @Test
  public void testVariableVectorReallocation() {
    assertThrows(OversizedAllocationException.class, () -> {
      final VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator);
      // edge case 1: value count = MAX_VALUE_ALLOCATION
      final long expectedAllocationInBytes = BaseValueVector.MAX_ALLOCATION_SIZE;
      final int expectedOffsetSize = 10;
      try {
        vector.allocateNew(expectedAllocationInBytes, 10);
        assertTrue(expectedOffsetSize <= vector.getValueCapacity());
        assertTrue(expectedAllocationInBytes <= vector.getDataBuffer().capacity());
        vector.reAlloc();
        assertTrue(expectedOffsetSize * 2 <= vector.getValueCapacity());
        assertTrue(expectedAllocationInBytes * 2 <= vector.getDataBuffer().capacity());
      } finally {
        vector.close();
      }

      // common: value count < MAX_VALUE_ALLOCATION
      try {
        vector.allocateNew(BaseValueVector.MAX_ALLOCATION_SIZE / 2, 0);
        vector.reAlloc(); // value allocation reaches to MAX_VALUE_ALLOCATION
        vector.reAlloc(); // this tests if it overflows
      } finally {
        vector.close();
      }
    });
  }
}
