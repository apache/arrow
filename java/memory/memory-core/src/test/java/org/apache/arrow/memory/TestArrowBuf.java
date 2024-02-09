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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.apache.arrow.memory.util.Float16;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

public class TestArrowBuf {

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSliceOutOfBoundsLength_RaisesIndexOutOfBoundsException() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(2)
    ) {
      assertEquals(2, buf.capacity());
      buf.slice(0, 3);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSliceOutOfBoundsIndexPlusLength_RaisesIndexOutOfBoundsException() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(2)
    ) {
      assertEquals(2, buf.capacity());
      buf.slice(1, 2);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testSliceOutOfBoundsIndex_RaisesIndexOutOfBoundsException() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(2)
    ) {
      assertEquals(2, buf.capacity());
      buf.slice(3, 0);
    }
  }

  @Test
  public void testSliceWithinBoundsLength_ReturnsSlice() {
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(2)
    ) {
      assertEquals(2, buf.capacity());
      assertEquals(1, buf.slice(1, 1).capacity());
      assertEquals(2, buf.slice(0, 2).capacity());
    }
  }

  @Test
  public void testSetBytesSliced() {
    int arrLength = 64;
    byte[] expected = new byte[arrLength];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    ByteBuffer data = ByteBuffer.wrap(expected);
    try (BufferAllocator allocator = new RootAllocator(128);
         ArrowBuf buf = allocator.buffer(expected.length)) {
      buf.setBytes(0, data, 0, data.capacity());

      byte[] actual = new byte[expected.length];
      buf.getBytes(0, actual);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  public void testSetBytesUnsliced() {
    int arrLength = 64;
    byte[] arr = new byte[arrLength];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = (byte) i;
    }
    ByteBuffer data = ByteBuffer.wrap(arr);

    int from = 10;
    int to = arrLength;
    byte[] expected = Arrays.copyOfRange(arr, from, to);
    try (BufferAllocator allocator = new RootAllocator(128);
        ArrowBuf buf = allocator.buffer(expected.length)) {
      buf.setBytes(0, data, from, to - from);

      byte[] actual = new byte[expected.length];
      buf.getBytes(0, actual);
      assertArrayEquals(expected, actual);
    }
  }

  /** ARROW-9221: guard against big-endian byte buffers. */
  @Test
  public void testSetBytesBigEndian() {
    final byte[] expected = new byte[64];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    // Only this code path is susceptible: others use unsafe or byte-by-byte copies, while this override copies longs.
    final ByteBuffer data = ByteBuffer.wrap(expected).asReadOnlyBuffer();
    assertFalse(data.hasArray());
    assertFalse(data.isDirect());
    assertEquals(ByteOrder.BIG_ENDIAN, data.order());
    try (BufferAllocator allocator = new RootAllocator(128);
        ArrowBuf buf = allocator.buffer(expected.length)) {
      buf.setBytes(0, data);
      byte[] actual = new byte[expected.length];
      buf.getBytes(0, actual);
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  /**
   * Test that allocation history is not recorded even though
   * assertions are enabled in tests (GH-34338).
   */
  public void testEnabledAssertion() {
    ((Logger) LoggerFactory.getLogger("org.apache.arrow")).setLevel(Level.TRACE);
    try (BufferAllocator allocator = new RootAllocator(128)) {
      allocator.buffer(2);
      Exception e = assertThrows(IllegalStateException.class, () -> allocator.close());
      assertFalse(e.getMessage().contains("event log for:"));
    } finally {
      ((Logger) LoggerFactory.getLogger("org.apache.arrow")).setLevel(null);
    }
  }

  @Test
  public void testEnabledHistoricalLog() {
    ((Logger) LoggerFactory.getLogger("org.apache.arrow")).setLevel(Level.TRACE);
    try {
      Field fieldDebug = BaseAllocator.class.getField("DEBUG");
      fieldDebug.setAccessible(true);
      Field modifiersDebug = Field.class.getDeclaredField("modifiers");
      modifiersDebug.setAccessible(true);
      modifiersDebug.setInt(fieldDebug, fieldDebug.getModifiers() & ~Modifier.FINAL);
      fieldDebug.set(null, true);
      try (BufferAllocator allocator = new RootAllocator(128)) {
        allocator.buffer(2);
        Exception e = assertThrows(IllegalStateException.class, allocator::close);
        assertTrue("Exception had the following message: " + e.getMessage(),
            e.getMessage().contains("event log for:")); // JDK8, JDK11
      } finally {
        fieldDebug.set(null, false);
      }
    } catch (Exception e) {
      assertTrue("Exception had the following toString(): " + e.toString(),
          e.toString().contains("java.lang.NoSuchFieldException: modifiers")); // JDK17+
    } finally {
      ((Logger) LoggerFactory.getLogger("org.apache.arrow")).setLevel(null);
    }
  }

  @Test
  public void testArrowBufFloat16() {
    try (BufferAllocator allocator = new RootAllocator();
         ArrowBuf buf = allocator.buffer(1024)
    ) {
      buf.setShort(0, Float16.toFloat16(+32.875f));
      assertEquals((short) 0x501c, buf.getShort(0));
    }
  }
}
