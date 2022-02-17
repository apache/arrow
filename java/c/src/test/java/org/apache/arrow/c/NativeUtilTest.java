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

package org.apache.arrow.c;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.c.NativeUtil;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.memory.util.MemoryUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NativeUtilTest {

  private RootAllocator allocator = null;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testString() {
    String javaString = "abc";
    byte[] nativeString = new byte[] { 97, 98, 99, 0 };
    try (ArrowBuf buffer = NativeUtil.toNativeString(allocator, javaString)) {
      int totalSize = LargeMemoryUtil.checkedCastToInt(buffer.readableBytes());
      ByteBuffer reader = MemoryUtil.directBuffer(buffer.memoryAddress(), totalSize).order(ByteOrder.nativeOrder());
      byte[] result = new byte[totalSize];
      reader.get(result);
      assertArrayEquals(nativeString, result);

      assertEquals(javaString, NativeUtil.toJavaString(buffer.memoryAddress()));
    }
  }

  @Test
  public void testToJavaArray() {
    long[] nativeArray = new long[] { 1, 2, 3 };
    try (ArrowBuf buffer = allocator.buffer(Long.BYTES * nativeArray.length, null)) {
      for (long value : nativeArray) {
        buffer.writeLong(value);
      }
      long[] actual = NativeUtil.toJavaArray(buffer.memoryAddress(), nativeArray.length);
      assertArrayEquals(nativeArray, actual);
    }
  }

  @Test
  public void testToZeroJavaArray() {
    long[] actual = NativeUtil.toJavaArray(0xDEADBEEF, 0);
    assertEquals(0, actual.length);
  }

}
