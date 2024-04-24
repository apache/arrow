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

import static org.apache.arrow.vector.TestUtils.newVector;
import static org.apache.arrow.vector.TestUtils.newViewVarBinaryVector;
import static org.apache.arrow.vector.TestUtils.newViewVarCharVector;
import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.ReusableByteArray;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestVarCharViewVector {

  private static final byte[] STR0 = "0123456".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR1 = "012345678912".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR2 = "0123456789123".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR3 = "01234567891234567".getBytes(StandardCharsets.UTF_8);
  private static final byte[] STR4 = "01234567".getBytes(StandardCharsets.UTF_8);
  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  public static void setBytes(int index, byte[] bytes, ViewVarCharVector vector) {
    BitVectorHelper.setBit(vector.validityBuffer, index);
    vector.setBytes(index, bytes, 0, bytes.length);
  }

  @Test
  public void testInlineAllocation() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 3);
      final int valueCount = 3;
      viewVarCharVector.set(0, STR0);
      viewVarCharVector.set(1, STR1);
      viewVarCharVector.set(2, STR4);
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;

      String str1 = new String(STR0, StandardCharsets.UTF_8);
      String str2 = new String(STR1, StandardCharsets.UTF_8);
      String str3 = new String(STR4, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      
      assert viewVarCharVector.dataBuffers.isEmpty();
      
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
    }
  }

  @Test
  public void testReferenceAllocationInSameBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      String str4 = generateRandomString(34);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);

      assert viewVarCharVector.dataBuffers.size() == 1;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
    }
  }

  @Test
  public void testReferenceAllocationInOtherBuffer() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(48, 4);
      final int valueCount = 4;
      String str4 = generateRandomString(35);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);

      assert viewVarCharVector.dataBuffers.size() == 2;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
    }
  }

  @Test
  public void testMixedAllocation() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(128, 6);
      final int valueCount = 6;
      String str4 = generateRandomString(35);
      String str6 = generateRandomString(40);
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR3);
      viewVarCharVector.set(3, str4.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.set(4, STR1);
      viewVarCharVector.set(5, str6.getBytes(StandardCharsets.UTF_8));
      viewVarCharVector.setValueCount(valueCount);

      byte[] view1 = viewVarCharVector.get(0);
      byte[] view2 = viewVarCharVector.get(1);
      byte[] view3 = viewVarCharVector.get(2);
      byte[] view4 = viewVarCharVector.get(3);
      byte[] view5 = viewVarCharVector.get(4);
      byte[] view6 = viewVarCharVector.get(5);

      assert view1 != null;
      assert view2 != null;
      assert view3 != null;
      assert view4 != null;
      assert view5 != null;
      assert view6 != null;

      String str1 = new String(STR1, StandardCharsets.UTF_8);
      String str2 = new String(STR2, StandardCharsets.UTF_8);
      String str3 = new String(STR3, StandardCharsets.UTF_8);

      Assert.assertEquals(new String(view1, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view2, StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(view3, StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(view4, StandardCharsets.UTF_8), str4);
      Assert.assertEquals(new String(view5, StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(view6, StandardCharsets.UTF_8), str6);

      assert viewVarCharVector.dataBuffers.size() == 1;

      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(0)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(1)).getBuffer(),
          StandardCharsets.UTF_8), str2);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(2)).getBuffer(),
          StandardCharsets.UTF_8), str3);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(3)).getBuffer(),
          StandardCharsets.UTF_8), str4);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(4)).getBuffer(),
          StandardCharsets.UTF_8), str1);
      Assert.assertEquals(new String(Objects.requireNonNull(viewVarCharVector.getObject(5)).getBuffer(),
          StandardCharsets.UTF_8), str6);
    }
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAllocationIndexOutOfBounds() {
    try (final ViewVarCharVector viewVarCharVector = new ViewVarCharVector("myvector", allocator)) {
      viewVarCharVector.allocateNew(32, 3);
      final int valueCount = 3;
      viewVarCharVector.set(0, STR1);
      viewVarCharVector.set(1, STR2);
      viewVarCharVector.set(2, STR2);
      viewVarCharVector.setValueCount(valueCount);
    }
  }

  @Test
  public void testSizeOfViewBufferElements() {
    try (final ViewVarCharVector vector = new ViewVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      int valueCount = 100;
      int currentSize = 0;
      vector.setInitialCapacity(valueCount);
      vector.allocateNew();
      vector.setValueCount(valueCount);
      for (int i = 0; i < valueCount; i++) {
        currentSize += i;
        vector.setSafe(i, new byte[i]);
      }
      assertEquals(currentSize, vector.sizeOfViewBufferElements());
    }
  }

  @Test
  public void testNullableVarType1() {

    // Create a new value vector for 1024 integers.
    // VarCharVector
    try (final ViewVarCharVector vector = newViewVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      vector.set(0, STR1);
      vector.set(1, STR2);
      vector.set(2, STR3);
      vector.setSafe(3, STR3, 1, STR3.length - 1);
      vector.setSafe(4, STR3, 2, STR3.length - 2);
      ByteBuffer str3ByteBuffer = ByteBuffer.wrap(STR3);
      vector.setSafe(5, str3ByteBuffer, 1, STR3.length - 1);
      vector.setSafe(6, str3ByteBuffer, 2, STR3.length - 2);

      // Set with convenience function
      Text txt = new Text("foo");
      vector.setSafe(7, txt.getBytes(), 0, (int) txt.getLength());

      // Check the sample strings.
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(3));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(4));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(5));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(6));

      // Check returning a Text object
      assertEquals(txt, vector.getObject(7));

      // Ensure null value throws.
      assertNull(vector.get(8));
    }
  }

  @Test
  public void testGetTextRepeatedly() {
    try (final ViewVarCharVector vector = new ViewVarCharVector("myviewvector", allocator)) {
      ValueVectorDataPopulator.setVector(vector, STR1, STR2);
      vector.setValueCount(2);

      /* check the vector output */
      Text text = new Text();
      vector.read(0, text);
      assertArrayEquals(STR1, text.getBytes());
      vector.read(1, text);
      assertArrayEquals(STR2, text.getBytes());
    }
  }

  @Test
  public void testNullableVarType2() {
    try (final ViewVarBinaryVector vector = newViewVarBinaryVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024 * 10, 1024);
      vector.set(0, STR1);
      vector.set(1, STR2);
      vector.set(2, STR3);
      vector.setSafe(3, STR3, 1, STR3.length - 1);
      vector.setSafe(4, STR3, 2, STR3.length - 2);
      ByteBuffer str3ByteBuffer = ByteBuffer.wrap(STR3);
      vector.setSafe(5, str3ByteBuffer, 1, STR3.length - 1);
      vector.setSafe(6, str3ByteBuffer, 2, STR3.length - 2);

      // Check the sample strings.
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(3));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(4));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(5));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(6));

      // Ensure null value throws.
      assertNull(vector.get(7));
    }
  }

  @Test
  public void testGetBytesRepeatedly() {
    try (ViewVarBinaryVector vector = new ViewVarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      final String str = "hello world";
      final String str2 = "foo";
      vector.setSafe(0, str.getBytes(StandardCharsets.UTF_8));
      vector.setSafe(1, str2.getBytes(StandardCharsets.UTF_8));

      // verify results
      ReusableByteArray reusableByteArray = new ReusableByteArray();
      vector.read(0, reusableByteArray);
      assertArrayEquals(
          str.getBytes(StandardCharsets.UTF_8),
          Arrays.copyOfRange(
              reusableByteArray.getBuffer(), 0, (int) reusableByteArray.getLength()));
      byte[] oldBuffer = reusableByteArray.getBuffer();

      vector.read(1, reusableByteArray);
      assertArrayEquals(
          str2.getBytes(StandardCharsets.UTF_8),
          Arrays.copyOfRange(
              reusableByteArray.getBuffer(), 0, (int) reusableByteArray.getLength()));

      // There should not have been any reallocation since the newer value is smaller in length.
      assertSame(oldBuffer, reusableByteArray.getBuffer());
    }
  }

  @Test
  public void testReAllocVariableWidthViewVector() {
    try (final ViewVarCharVector vector = newVector(ViewVarCharVector.class, EMPTY_SCHEMA_PATH,
            Types.MinorType.VIEWVARCHAR, allocator)) {
      final int capacityLimit = 4095;
      final int overLimitIndex = 200;
      vector.setInitialCapacity(capacityLimit);
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();
      assertTrue(initialCapacity >= capacityLimit);

      /* Put values in indexes that fall within the initial allocation */
      vector.setSafe(0, STR1, 0, STR1.length);
      vector.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

      /* the set calls above should NOT have triggered a realloc */
      assertEquals(initialCapacity, vector.getValueCapacity());

      /* Now try to put values in space that falls beyond the initial allocation */
      vector.setSafe(initialCapacity + overLimitIndex, STR3, 0, STR3.length);

      /* Check valueCapacity is more than initial allocation */
      assertTrue(initialCapacity * 2 <= vector.getValueCapacity());

      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(initialCapacity - 1));
      assertArrayEquals(STR3, vector.get(initialCapacity + overLimitIndex));

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for ValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
      vector.setValueCount(vector.getValueCapacity() + overLimitIndex);
    }
  }

  @Test
  public void testSetSafeWithArrowBufNoExcessAllocs() {
    final int numValues = BaseVariableWidthViewVector.INITIAL_VALUE_ALLOCATION * 2;
    final byte[] valueBytes = "hello world".getBytes(StandardCharsets.UTF_8);
    final int valueBytesLength = valueBytes.length;
    final int isSet = 1;
    try (final ViewVarCharVector fromVector =
            newVector(
                ViewVarCharVector.class,
                EMPTY_SCHEMA_PATH,
                Types.MinorType.VIEWVARCHAR,
                allocator);
        final ViewVarCharVector toVector =
            newVector(
                ViewVarCharVector.class,
                EMPTY_SCHEMA_PATH,
                Types.MinorType.VIEWVARCHAR,
                allocator)) {
      /*
       * Populate the `fromVector` with `numValues` with byte-arrays, each of size `valueBytesLength`.
       */
      fromVector.setInitialCapacity(numValues);
      fromVector.allocateNew();
      for (int i = 0; i < numValues; ++i) {
        fromVector.setSafe(i, valueBytes, 0 /*start*/, valueBytesLength);
      }
      fromVector.setValueCount(numValues);
      ArrowBuf fromDataBuffer = fromVector.getDataBuffer();
      assertTrue(numValues * valueBytesLength <= fromDataBuffer.capacity());

      /*
       * Copy the entries one-by-one from 'fromVector' to 'toVector', but use the setSafe with
       * ArrowBuf API (instead of setSafe with byte-array).
       */
      toVector.setInitialCapacity(numValues);
      toVector.allocateNew();
      for (int i = 0; i < numValues; i++) {
        int start = fromVector.getTotalValueLengthUpToIndex(i);
        // across variable
        // width implementations
        int end = fromVector.getTotalValueLengthUpToIndex(i + 1);
        toVector.setSafe(i, isSet, start, end, fromDataBuffer);
      }

      /*
       * Since the 'fromVector' and 'toVector' have the same initial capacity, and were populated
       * with the same varchar elements, the allocations and hence, the final capacity should be
       * the same.
       */
      assertEquals(fromDataBuffer.capacity(), toVector.getDataBuffer().capacity());
    }
  }

  @Test
  public void testSetLastSetUsage() {
    try (final ViewVarCharVector vector = new ViewVarCharVector("myvector", allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);

      /* Check current lastSet */
      assertEquals(-1, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));

      /*
       * If we don't do setLastSe(3) before setValueCount(), then the latter will corrupt
       * the value vector by filling in all positions [0,valuecount-1] will empty byte arrays.
       * Run the test by commenting on the next line, and we should see incorrect vector output.
       */
      vector.setLastSet(3);
      vector.setValueCount(20);

      /* Check current lastSet */
      assertEquals(19, vector.getLastSet());

      /* Check the vector output again */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));

      assertEquals(0, vector.getValueLength(4));
      assertEquals(0, vector.getValueLength(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));
      assertEquals(0, vector.getValueLength(10));
      assertEquals(0, vector.getValueLength(11));
      assertEquals(0, vector.getValueLength(12));
      assertEquals(0, vector.getValueLength(13));
      assertEquals(0, vector.getValueLength(14));
      assertEquals(0, vector.getValueLength(15));
      assertEquals(0, vector.getValueLength(16));
      assertEquals(0, vector.getValueLength(17));
      assertEquals(0, vector.getValueLength(18));
      assertEquals(0, vector.getValueLength(19));
    }
  }

  @Test
  public void testFillEmptiesUsage() {
    try (final ViewVarCharVector vector = new ViewVarCharVector("myvector", allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);

      /* Check current lastSet */
      assertEquals(-1, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));

      vector.setLastSet(3);
      /* fill empty byte arrays from index [4, 9] */
      vector.fillEmpties(10);

      /* Check current lastSet */
      assertEquals(9, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertEquals(0, vector.getValueLength(4));
      assertEquals(0, vector.getValueLength(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));

      setBytes(10, STR1, vector);
      setBytes(11, STR2, vector);

      vector.setLastSet(11);
      /* fill empty byte arrays from index [12, 14] */
      vector.setValueCount(15);

      /* Check current lastSet */
      assertEquals(14, vector.getLastSet());

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertEquals(0, vector.getValueLength(4));
      assertEquals(0, vector.getValueLength(5));
      assertEquals(0, vector.getValueLength(6));
      assertEquals(0, vector.getValueLength(7));
      assertEquals(0, vector.getValueLength(8));
      assertEquals(0, vector.getValueLength(9));
      assertArrayEquals(STR1, vector.get(10));
      assertArrayEquals(STR2, vector.get(11));
      assertEquals(0, vector.getValueLength(12));
      assertEquals(0, vector.getValueLength(13));
      assertEquals(0, vector.getValueLength(14));
    }
  }

  @Test
  public void testGetBufferAddress1() {
    try (final ViewVarCharVector vector = new ViewVarCharVector("myviewvector", allocator)) {

      setVector(vector, STR1, STR2, STR3, STR4);
      vector.setValueCount(15);

      /* check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));

      List<ArrowBuf> buffers = vector.getFieldBuffers();
      long bitAddress = vector.getValidityBufferAddress();
      long dataAddress = vector.getDataBufferAddress();

      assertEquals(3, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(dataAddress, buffers.get(1).memoryAddress());
    }
  }

  @Test
  public void testSetInitialCapacityInViews() {
    try (final ViewVarCharVector vector = new ViewVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {

      /* use the default 16 data bytes on average per element */
      final int viewSize = BaseVariableWidthViewVector.ELEMENT_SIZE;
      int defaultCapacity = BaseVariableWidthViewVector.INITIAL_VIEW_VALUE_ALLOCATION / viewSize;
      vector.setInitialCapacity(defaultCapacity);
      vector.allocateNew();
      assertEquals(defaultCapacity, vector.getValueCapacity());
      assertEquals(CommonUtil.nextPowerOfTwo(defaultCapacity * viewSize), vector.getDataBuffer().capacity());

      double density = 4.0;
      final int valueCount = 5;
      vector.setInitialCapacity(valueCount, density);
      vector.allocateNew();
      assertEquals(8, vector.getValueCapacity());
      assertEquals(128, vector.getDataBuffer().capacity());
      int initialDataBufferSize = (int) (valueCount * density);
      // making sure a databuffer is allocated
      vector.set(4, "01234567890123456".getBytes(StandardCharsets.UTF_8));
      assertEquals(vector.dataBuffers.size(), 1);
      ArrowBuf dataBuf = vector.dataBuffers.get(0);
      try (ArrowBuf tempBuf = vector.allocator.buffer(initialDataBufferSize)) {
        // replicating a new buffer allocation process when a new buffer is added to the
        // data buffer when inserting an element with length > 12
        assertEquals(tempBuf.capacity(), dataBuf.capacity());
      }
    }
  }

  private String generateRandomString(int length) {
    Random random = new Random();
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(random.nextInt(10)); // 0-9
    }
    return sb.toString();
  }
}
