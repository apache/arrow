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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Test cases for {@link BinaryPointerVector}.
 */
public class TestBinaryPointerVector {

  private static final int MAX_LENGTH = 20;

  private BufferAllocator allocator;

  private ArrowBuf buffer;

  @Before
  public void init() {
    allocator = new RootAllocator(1024 * 1024);
    buffer = allocator.buffer((int) allocator.getLimit() / 2);
  }

  @After
  public void terminate() throws Exception {
    buffer.close();
    allocator.close();
  }

  private void writeDataToBuffer(String[] inputData) {
    buffer.setZero(0, buffer.capacity());
    for (int i = 0; i < inputData.length; i++) {
      if (inputData[i] != null) {
        byte[] strBytes = inputData[i].getBytes();
        PlatformDependent.copyMemory(
                strBytes, 0, buffer.memoryAddress() + i * MAX_LENGTH, strBytes.length);
      }
    }
  }

  @Test
  public void testSetGet() {
    String[] input = new String[]{
      "abc", null, "123", "aaaaa", "hello", "hi", null, "world", "2019", "716"
    };
    writeDataToBuffer(input);

    try (BinaryPointerVector vector = new BinaryPointerVector("vec", allocator)) {
      vector.allocateNew(input.length);

      // set data
      for (int i = 0; i < input.length; i++) {
        if (input[i] == null) {
          vector.setNull(i);
        } else {
          vector.setAddress(i, buffer.memoryAddress() + i * MAX_LENGTH);
          vector.setLength(i, input[i].length());
        }
      }

      // get and verify data
      for (int i = 0; i < input.length; i++) {
        if (vector.isNull(i)) {
          assertNull(vector.get(i));
        } else {
          long address = vector.getAddress(i);
          assertEquals(buffer.memoryAddress() + i * MAX_LENGTH, address);

          int length = vector.getLength(i);
          assertEquals(input[i].length(), length);

          assertArrayEquals(input[i].getBytes(), vector.get(i));
        }
      }
    }
  }

  @Test
  public void testSetGetNullableBinaryPointerHolder() {
    String[] input = new String[]{
      "abc", null, "123", "aaaaa", "hello", "hi", null, "world", "2019", "716"
    };
    writeDataToBuffer(input);

    BinaryPointerVector.NullableBinaryPointerHolder inputHolder =
            new BinaryPointerVector.NullableBinaryPointerHolder();
    BinaryPointerVector.NullableBinaryPointerHolder outputHolder =
            new BinaryPointerVector.NullableBinaryPointerHolder();

    try (BinaryPointerVector vector = new BinaryPointerVector("vec", allocator)) {
      vector.allocateNew(input.length);

      // set data
      for (int i = 0; i < input.length; i++) {
        if (input[i] == null) {
          inputHolder.isSet = 0;
        } else {
          inputHolder.isSet = 1;
          inputHolder.address = buffer.memoryAddress() + i * MAX_LENGTH;
          inputHolder.length = input[i].length();
        }
        vector.set(i, inputHolder);
      }

      // get and verify data
      for (int i = 0; i < input.length; i++) {
        vector.get(i, outputHolder);
        if (vector.isNull(i)) {
          assertEquals(0, outputHolder.isSet);
        } else {
          assertNotEquals(0, outputHolder.isSet);
          assertEquals(buffer.memoryAddress() + i * MAX_LENGTH, outputHolder.address);
          assertEquals(input[i].length(), outputHolder.length);

          assertArrayEquals(input[i].getBytes(), vector.get(i));
        }
      }
    }
  }

  @Test
  public void testSetGetBinaryPointerHolder() {
    String[] input = new String[]{
      "abc", "123", "aaaaa", "hello", "hi", "world", "2019", "716"
    };
    writeDataToBuffer(input);

    BinaryPointerVector.BinaryPointerHolder inputHolder =
            new BinaryPointerVector.BinaryPointerHolder();
    BinaryPointerVector.NullableBinaryPointerHolder outputHolder =
            new BinaryPointerVector.NullableBinaryPointerHolder();

    try (BinaryPointerVector vector = new BinaryPointerVector("vec", allocator)) {
      vector.allocateNew(input.length);

      // set data
      for (int i = 0; i < input.length; i++) {
        inputHolder.address = buffer.memoryAddress() +  i * MAX_LENGTH;
        inputHolder.length = input[i].length();
        vector.set(i, inputHolder);
      }

      // get and verify data
      for (int i = 0; i < input.length; i++) {
        vector.get(i, outputHolder);

        assertNotEquals(0, outputHolder.isSet);
        assertFalse(vector.isNull(i));
        assertEquals(buffer.memoryAddress() + i * MAX_LENGTH, outputHolder.address);
        assertEquals(input[i].length(), outputHolder.length);

        assertArrayEquals(input[i].getBytes(), vector.get(i));
      }
    }
  }

  @Test
  public void testToVarBinaryVector() {
    String[] input = new String[]{
      "abc", "123", "aaaaa", "hello", "hi", "world", "2019", "716"
    };
    writeDataToBuffer(input);

    BinaryPointerVector.NullableBinaryPointerHolder inputHolder =
            new BinaryPointerVector.NullableBinaryPointerHolder();

    try (BinaryPointerVector vector = new BinaryPointerVector("vec", allocator);
         VarBinaryVector varBinaryVector = new VarBinaryVector("var bin", allocator);) {
      vector.allocateNew(input.length);

      // set data
      for (int i = 0; i < input.length; i++) {
        if (input[i] == null) {
          inputHolder.isSet = 0;
        } else {
          inputHolder.isSet = 1;
          inputHolder.address = buffer.memoryAddress() + i * MAX_LENGTH;
          inputHolder.length = input[i].length();
        }
        vector.set(i, inputHolder);
      }

      vector.toVarBinaryVector(varBinaryVector);

      // verify results
      assertEquals(vector.getValueCount(), varBinaryVector.getValueCount());
      for (int i = 0; i < vector.getValueCount(); i++) {
        if (vector.isNull(i)) {
          assertTrue(varBinaryVector.isNull(i));
        } else {
          assertArrayEquals(vector.get(i), varBinaryVector.get(i));
        }
      }
    }
  }
}
