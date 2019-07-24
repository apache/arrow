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

import static org.junit.Assert.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestFixedSizeBinaryVector {
  private static final int numValues = 123;
  private static final int typeWidth = 9;
  private static final int smallDataSize = 6;
  private static final int largeDataSize = 12;

  private static byte[][] values;

  static {
    values = new byte[numValues][typeWidth];
    for (int i = 0; i < numValues; i++) {
      for (int j = 0; j < typeWidth; j++) {
        values[i][j] = ((byte) i);
      }
    }
  }

  private ArrowBuf[] bufs = new ArrowBuf[numValues];
  private FixedSizeBinaryHolder[] holders = new FixedSizeBinaryHolder[numValues];
  private NullableFixedSizeBinaryHolder[] nullableHolders = new NullableFixedSizeBinaryHolder[numValues];

  private static byte[] smallValue;

  static {
    smallValue = new byte[smallDataSize];
    for (int i = 0; i < smallDataSize; i++) {
      smallValue[i] = ((byte) i);
    }
  }

  private ArrowBuf smallBuf;
  private FixedSizeBinaryHolder smallHolder;
  private NullableFixedSizeBinaryHolder smallNullableHolder;

  private static byte[] largeValue;

  static {
    largeValue = new byte[largeDataSize];
    for (int i = 0; i < largeDataSize; i++) {
      largeValue[i] = ((byte) i);
    }
  }

  private ArrowBuf largeBuf;
  private FixedSizeBinaryHolder largeHolder;
  private NullableFixedSizeBinaryHolder largeNullableHolder;

  private BufferAllocator allocator;
  private FixedSizeBinaryVector vector;

  private static void failWithException(String message) throws Exception {
    throw new Exception(message);
  }


  @Before
  public void init() throws Exception {
    allocator = new DirtyRootAllocator(Integer.MAX_VALUE, (byte) 100);
    vector = new FixedSizeBinaryVector("fixedSizeBinary", allocator, typeWidth);
    vector.allocateNew();

    for (int i = 0; i < numValues; i++) {
      bufs[i] = allocator.buffer(typeWidth);
      bufs[i].setBytes(0, values[i]);

      holders[i] = new FixedSizeBinaryHolder();
      holders[i].byteWidth = typeWidth;
      holders[i].buffer = bufs[i];

      nullableHolders[i] = new NullableFixedSizeBinaryHolder();
      nullableHolders[i].byteWidth = typeWidth;
      nullableHolders[i].buffer = bufs[i];
      nullableHolders[i].isSet = 1;
    }

    smallBuf = allocator.buffer(smallDataSize);
    smallBuf.setBytes(0, smallValue);

    smallHolder = new FixedSizeBinaryHolder();
    smallHolder.byteWidth = smallDataSize;
    smallHolder.buffer = smallBuf;

    smallNullableHolder = new NullableFixedSizeBinaryHolder();
    smallNullableHolder.byteWidth = smallDataSize;
    smallNullableHolder.buffer = smallBuf;

    largeBuf = allocator.buffer(largeDataSize);
    largeBuf.setBytes(0, largeValue);

    largeHolder = new FixedSizeBinaryHolder();
    largeHolder.byteWidth = typeWidth;
    largeHolder.buffer = largeBuf;

    largeNullableHolder = new NullableFixedSizeBinaryHolder();
    largeNullableHolder.byteWidth = typeWidth;
    largeNullableHolder.buffer = largeBuf;
  }

  @After
  public void terminate() throws Exception {
    for (int i = 0; i < numValues; i++) {
      bufs[i].close();
    }
    smallBuf.close();
    largeBuf.close();

    vector.close();
    allocator.close();
  }

  @Test
  public void testSetUsingByteArray() {
    for (int i = 0; i < numValues; i++) {
      vector.set(i, values[i]);
    }
    vector.setValueCount(numValues);
    for (int i = 0; i < numValues; i++) {
      assertArrayEquals(values[i], vector.getObject(i));
    }
  }

  @Test
  public void testSetUsingHolder() {
    for (int i = 0; i < numValues; i++) {
      vector.set(i, holders[i]);
    }
    vector.setValueCount(numValues);
    for (int i = 0; i < numValues; i++) {
      assertArrayEquals(values[i], vector.getObject(i));
    }
  }

  @Test
  public void testSetUsingNullableHolder() {
    for (int i = 0; i < numValues; i++) {
      vector.set(i, nullableHolders[i]);
    }
    vector.setValueCount(numValues);
    for (int i = 0; i < numValues; i++) {
      assertArrayEquals(values[i], vector.getObject(i));
    }
  }

  @Test
  public void testGetUsingNullableHolder() {
    for (int i = 0; i < numValues; i++) {
      vector.set(i, holders[i]);
    }
    vector.setValueCount(numValues);
    for (int i = 0; i < numValues; i++) {
      vector.get(i, nullableHolders[i]);
      assertEquals(typeWidth, nullableHolders[i].byteWidth);
      assertTrue(nullableHolders[i].isSet > 0);
      byte[] actual = new byte[typeWidth];
      nullableHolders[i].buffer.getBytes(0, actual, 0, typeWidth);
      assertArrayEquals(values[i], actual);
    }
  }

  @Test
  public void testSetWithInvalidInput() throws Exception {
    String errorMsg = "input data needs to be at least " + typeWidth + " bytes";

    // test small inputs, byteWidth matches but value or buffer is too small
    try {
      vector.set(0, smallValue);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.set(0, smallHolder);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.set(0, smallNullableHolder);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.set(0, smallBuf);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    // test large inputs, byteWidth matches but value or buffer is bigger than byteWidth
    vector.set(0, largeValue);
    vector.set(0, largeHolder);
    vector.set(0, largeNullableHolder);
    vector.set(0, largeBuf);
  }

  @Test
  public void setSetSafeWithInvalidInput() throws Exception {
    String errorMsg = "input data needs to be at least " + typeWidth + " bytes";

    // test small inputs, byteWidth matches but value or buffer is too small
    try {
      vector.setSafe(0, smallValue);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.setSafe(0, smallHolder);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.setSafe(0, smallNullableHolder);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    try {
      vector.setSafe(0, smallBuf);
      failWithException(errorMsg);
    } catch (AssertionError ignore) {
    }

    // test large inputs, byteWidth matches but value or buffer is bigger than byteWidth
    vector.setSafe(0, largeValue);
    vector.setSafe(0, largeHolder);
    vector.setSafe(0, largeNullableHolder);
    vector.setSafe(0, largeBuf);
  }

  @Test
  public void testGetNull() {
    vector.setNull(0);
    assertNull(vector.get(0));
  }
}
