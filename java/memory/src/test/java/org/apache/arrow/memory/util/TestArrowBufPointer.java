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

package org.apache.arrow.memory.util;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

/**
 * Test cases for {@link ArrowBufPointer}.
 */
public class TestArrowBufPointer {

  private final int BUFFER_LENGTH = 1024;

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testArrowBufPointersEqual() {
    try (ArrowBuf buf1 = allocator.buffer(BUFFER_LENGTH);
    ArrowBuf buf2 = allocator.buffer(BUFFER_LENGTH)) {
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        buf1.setInt(i * 4, i * 1234);
        buf2.setInt(i * 4, i * 1234);
      }

      ArrowBufPointer ptr1 = new ArrowBufPointer(null, 0, 100);
      ArrowBufPointer ptr2 = new ArrowBufPointer(null, 100, 5032);
      assertTrue(ptr1.equals(ptr2));
      for (int i = 0; i < BUFFER_LENGTH / 4; i++) {
        ptr1.set(buf1, i * 4, 4);
        ptr2.set(buf2, i * 4, 4);
        assertTrue(ptr1.equals(ptr2));
      }
    }
  }

  @Test
  public void testArrowBufPointersHashCode() {
    final int vectorLength = 100;
    try (ArrowBuf buf1 = allocator.buffer(vectorLength * 4);
         ArrowBuf buf2 = allocator.buffer(vectorLength * 4)) {
      for (int i = 0; i < vectorLength; i++) {
        buf1.setInt(i * 4, i);
        buf2.setInt(i * 4, i);
      }

      CounterHasher hasher1 = new CounterHasher();
      CounterHasher hasher2 = new CounterHasher();

      ArrowBufPointer pointer1 = new ArrowBufPointer(hasher1);
      assertEquals(ArrowBufPointer.NULL_HASH_CODE, pointer1.hashCode());

      ArrowBufPointer pointer2 = new ArrowBufPointer(hasher2);
      assertEquals(ArrowBufPointer.NULL_HASH_CODE, pointer2.hashCode());

      for (int i = 0; i < vectorLength; i++) {
        pointer1.set(buf1, i * 4, 4);
        pointer2.set(buf2, i * 4, 4);

        assertEquals(pointer1.hashCode(), pointer2.hashCode());

        // verify that the hash codes have been re-computed
        assertEquals(hasher1.counter, i + 1);
        assertEquals(hasher2.counter, i + 1);
      }
    }
  }

  @Test
  public void testNullPointersHashCode() {
    ArrowBufPointer pointer = new ArrowBufPointer();
    assertEquals(ArrowBufPointer.NULL_HASH_CODE, pointer.hashCode());

    pointer.set(null, 0, 0);
    assertEquals(ArrowBufPointer.NULL_HASH_CODE, pointer.hashCode());
  }

  @Test
  public void testReuseHashCode() {
    try (ArrowBuf buf = allocator.buffer(10)) {
      buf.setInt(0, 10);
      buf.setInt(4, 20);

      CounterHasher hasher = new CounterHasher();
      ArrowBufPointer pointer = new ArrowBufPointer(hasher);

      pointer.set(buf, 0, 4);
      pointer.hashCode();

      // hash code computed
      assertEquals(1, hasher.counter);

      // no hash code re-compute
      pointer.hashCode();
      assertEquals(1, hasher.counter);

      // hash code re-computed
      pointer.set(buf, 4, 4);
      pointer.hashCode();
      assertEquals(2, hasher.counter);
    }
  }

  @Test
  public void testHashersForEquality() {
    try (ArrowBuf buf = allocator.buffer(10)) {
      // pointer 1 uses the default hasher
      ArrowBufPointer pointer1 = new ArrowBufPointer(buf, 0, 10);

      // pointer 2 uses the counter hasher
      ArrowBufPointer pointer2 = new ArrowBufPointer(buf, 0, 10, new CounterHasher());

      // the two pointers cannot be equal, since they have different hashers
      assertFalse(pointer1.equals(pointer2));
    }
  }

  /**
   * Hasher with a counter that increments each time a hash code is calculated.
   * This is to validate that the hash code in {@link ArrowBufPointer} is reused.
   */
  class CounterHasher implements ArrowBufHasher {

    protected int counter = 0;

    @Override
    public int hashCode(long address, int length) {
      counter += 1;
      return SimpleHasher.INSTANCE.hashCode(address, length);
    }

    @Override
    public int hashCode(ArrowBuf buf, int offset, int length) {
      counter += 1;
      return SimpleHasher.INSTANCE.hashCode(buf, offset, length);
    }

    @Override
    public boolean equals(Object o) {
      return o != null && this.getClass() == o.getClass();
    }
  }
}
