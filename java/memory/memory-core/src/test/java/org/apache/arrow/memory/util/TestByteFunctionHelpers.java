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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestByteFunctionHelpers {

  private BufferAllocator allocator;

  private static final int SIZE = 100;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testEquals() {
    ArrowBuf buffer1 = allocator.buffer(SIZE);
    ArrowBuf buffer2 = allocator.buffer(SIZE);

    for (int i = 0; i < SIZE; i++) {
      buffer1.setByte(i, i);
      buffer2.setByte(i, i);
    }

    // test three cases, length>8, length>3, length<3

    assertEquals(1, ByteFunctionHelpers.equal(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(1, ByteFunctionHelpers.equal(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(1, ByteFunctionHelpers.equal(buffer1, 0, 2, buffer2, 0, 2));

    // change value at index1
    buffer1.setByte(1, 10);

    assertEquals(0, ByteFunctionHelpers.equal(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(0, ByteFunctionHelpers.equal(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(0, ByteFunctionHelpers.equal(buffer1, 0, 2, buffer2, 0, 2));

    buffer1.close();
    buffer2.close();
  }

  @Test
  public void testCompare() {
    ArrowBuf buffer1 = allocator.buffer(SIZE);
    ArrowBuf buffer2 = allocator.buffer(SIZE);

    for (int i = 0; i < SIZE; i++) {
      buffer1.setByte(i, i);
      buffer2.setByte(i, i);
    }

    // test three cases, length>8, length>3, length<3

    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, 2, buffer2, 0, 2));

    // change value at index 1
    buffer1.setByte(1, 0);

    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, 2, buffer2, 0, 2));

    buffer1.close();
    buffer2.close();
  }

  @Test
  public void testStringCompare() {
    String[] leftStrings = {"cat", "cats", "catworld", "dogs", "bags"};
    String[] rightStrings = {"dog", "dogs", "dogworld", "dog", "sgab"};

    for (int i = 0; i < leftStrings.length; ++i) {
      String leftStr = leftStrings[i];
      String rightStr = rightStrings[i];

      ArrowBuf left = allocator.buffer(SIZE);
      left.setBytes(0, leftStr.getBytes(StandardCharsets.UTF_8));
      ArrowBuf right = allocator.buffer(SIZE);
      right.setBytes(0, rightStr.getBytes(StandardCharsets.UTF_8));

      assertEquals(
          leftStr.compareTo(rightStr) < 0 ? -1 : 1,
          ByteFunctionHelpers.compare(left, 0, leftStr.length(), right, 0, rightStr.length()));

      left.close();
      right.close();
    }
  }

  @Test
  public void testCompareWithByteArray() {
    ArrowBuf buffer1 = allocator.buffer(SIZE);
    byte[] buffer2 = new byte[SIZE];

    for (int i = 0; i < SIZE; i++) {
      buffer1.setByte(i, i);
      buffer2[i] = (byte) i;
    }

    // test three cases, length>8, length>3, length<3

    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(0, ByteFunctionHelpers.compare(buffer1, 0, 2, buffer2, 0, 2));

    // change value at index 1
    buffer1.setByte(1, 0);

    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, SIZE - 1, buffer2, 0, SIZE - 1));
    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, 6, buffer2, 0, 6));
    assertEquals(-1, ByteFunctionHelpers.compare(buffer1, 0, 2, buffer2, 0, 2));

    buffer1.close();
  }
}
