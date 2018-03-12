/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;
import org.apache.arrow.vector.util.OversizedAllocationException;

import static org.apache.arrow.vector.TestUtils.newVarBinaryVector;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.apache.arrow.vector.TestUtils.newVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;


public class TestValueVector {

  private final static String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private final static Charset utf8Charset = Charset.forName("UTF-8");
  private final static byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private final static byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private final static byte[] STR3 = "CCCC3".getBytes(utf8Charset);
  private final static byte[] STR4 = "DDDDDDDD4".getBytes(utf8Charset);
  private final static byte[] STR5 = "EEE5".getBytes(utf8Charset);
  private final static byte[] STR6 = "FFFFF6".getBytes(utf8Charset);
  private final static int MAX_VALUE_COUNT =
            Integer.getInteger("arrow.vector.max_allocation_bytes", Integer.MAX_VALUE)/4;
  private final static int MAX_VALUE_COUNT_8BYTE = MAX_VALUE_COUNT/2;

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  /*
   * Tests for Fixed-Width vectors
   *
   * Covered types as of now
   *
   *  -- UInt4Vector
   *  -- IntVector
   *  -- Float4Vector
   *  -- Float8Vector
   *
   *  -- UInt4Vector
   *  -- IntVector
   *  -- Float4Vector
   *
   * TODO:
   *
   *  -- SmallIntVector
   *  -- BigIntVector
   *  -- TinyIntVector
   */

  @Test /* UInt4Vector */
  public void testFixedType1() {

    // Create a new value vector for 1024 integers.
    try (final UInt4Vector vector = new UInt4Vector(EMPTY_SCHEMA_PATH, allocator)) {

      boolean error = false;
      int initialCapacity = 0;

      vector.allocateNew(1024);
      initialCapacity = vector.getValueCapacity();
      assertEquals(1024, initialCapacity);

      // Put and set a few values
      vector.setSafe(0, 100);
      vector.setSafe(1, 101);
      vector.setSafe(100, 102);
      vector.setSafe(1022, 103);
      vector.setSafe(1023, 104);

      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(1022));
      assertEquals(104, vector.get(1023));

      try {
        vector.set(1024, 10000);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      try {
        vector.get(1024);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      vector.setSafe(1024, 10000);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* check vector data after realloc */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(1022));
      assertEquals(104, vector.get(1023));
      assertEquals(10000, vector.get(1024));

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        // TODO: test vector.get(i) is 0 after unsafe get added
        assertEquals("non-zero data not expected at index: " + i, true, vector.isNull(i));
      }
    }
  }

  @Test /* IntVector */
  public void testFixedType2() {
    try (final IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {
      boolean error = false;
      int initialCapacity = 16;

      /* we should not throw exception for these values of capacity */
      intVector.setInitialCapacity(MAX_VALUE_COUNT - 1);
      intVector.setInitialCapacity(MAX_VALUE_COUNT);

      try {
        intVector.setInitialCapacity(MAX_VALUE_COUNT + 1);
      }
      catch (OversizedAllocationException oe) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      intVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, intVector.getValueCapacity());

      /* allocate 64 bytes (16 * 4) */
      intVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertEquals(initialCapacity, intVector.getValueCapacity());

      /* populate the vector */
      int j = 1;
      for(int i = 0; i < 16; i += 2) {
        intVector.set(i, j);
        j++;
      }

      try {
        intVector.set(16, 9);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      j = 1;
      for(int i = 0; i < 16; i += 2) {
        assertEquals("unexpected value at index: " + i, j, intVector.get(i));
        j++;
      }

      try {
        intVector.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      intVector.setSafe(16, 9);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, intVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      j = 1;
      for(int i = 0; i <= 16; i += 2) {
        assertEquals("unexpected value at index: " + i, j, intVector.get(i));
        j++;
      }

      /* reset the vector */
      intVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, intVector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, true, intVector.isNull(i));
      }
    }
  }

  @Test /* Float4Vector */
  public void testFixedType3() {
    try (final Float4Vector floatVector = new Float4Vector(EMPTY_SCHEMA_PATH, allocator)) {
      boolean error = false;
      int initialCapacity = 16;

      /* we should not throw exception for these values of capacity */
      floatVector.setInitialCapacity(MAX_VALUE_COUNT - 1);
      floatVector.setInitialCapacity(MAX_VALUE_COUNT);

      try {
        floatVector.setInitialCapacity(MAX_VALUE_COUNT + 1);
      }
      catch (OversizedAllocationException oe) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      floatVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, floatVector.getValueCapacity());

      /* allocate 64 bytes (16 * 4) */
      floatVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertEquals(initialCapacity, floatVector.getValueCapacity());

      floatVector.zeroVector();

      /* populate the floatVector */
      floatVector.set(0, 1.5f);
      floatVector.set(2, 2.5f);
      floatVector.set(4, 3.3f);
      floatVector.set(6, 4.8f);
      floatVector.set(8, 5.6f);
      floatVector.set(10, 6.6f);
      floatVector.set(12, 7.8f);
      floatVector.set(14, 8.5f);

      try {
        floatVector.set(16, 9.5f);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      assertEquals(1.5f, floatVector.get(0), 0);
      assertEquals(2.5f, floatVector.get(2), 0);
      assertEquals(3.3f, floatVector.get(4), 0);
      assertEquals(4.8f, floatVector.get(6), 0);
      assertEquals(5.6f, floatVector.get(8), 0);
      assertEquals(6.6f, floatVector.get(10), 0);
      assertEquals(7.8f, floatVector.get(12), 0);
      assertEquals(8.5f, floatVector.get(14), 0);

      try {
        floatVector.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      floatVector.setSafe(16, 9.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(1.5f, floatVector.get(0), 0);
      assertEquals(2.5f, floatVector.get(2), 0);
      assertEquals(3.3f, floatVector.get(4), 0);
      assertEquals(4.8f, floatVector.get(6), 0);
      assertEquals(5.6f, floatVector.get(8), 0);
      assertEquals(6.6f, floatVector.get(10), 0);
      assertEquals(7.8f, floatVector.get(12), 0);
      assertEquals(8.5f, floatVector.get(14), 0);
      assertEquals(9.5f, floatVector.get(16), 0);

      /* reset the vector */
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, true, floatVector.isNull(i));
      }
    }
  }

  @Test /* Float8Vector */
  public void testFixedType4() {
    try (final Float8Vector floatVector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {
      boolean error = false;
      int initialCapacity = 16;

      /* we should not throw exception for these values of capacity */
      floatVector.setInitialCapacity(MAX_VALUE_COUNT_8BYTE - 1);
      floatVector.setInitialCapacity(MAX_VALUE_COUNT_8BYTE);

      try {
        floatVector.setInitialCapacity(MAX_VALUE_COUNT_8BYTE + 1);
      }
      catch (OversizedAllocationException oe) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      floatVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, floatVector.getValueCapacity());

      /* allocate 128 bytes (16 * 8) */
      floatVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertEquals(initialCapacity, floatVector.getValueCapacity());

      /* populate the vector */
      floatVector.set(0, 1.55);
      floatVector.set(2, 2.53);
      floatVector.set(4, 3.36);
      floatVector.set(6, 4.82);
      floatVector.set(8, 5.67);
      floatVector.set(10, 6.67);
      floatVector.set(12, 7.87);
      floatVector.set(14, 8.56);

      try {
        floatVector.set(16, 9.53);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check floatVector contents */
      assertEquals(1.55, floatVector.get(0), 0);
      assertEquals(2.53, floatVector.get(2), 0);
      assertEquals(3.36, floatVector.get(4), 0);
      assertEquals(4.82, floatVector.get(6), 0);
      assertEquals(5.67, floatVector.get(8), 0);
      assertEquals(6.67, floatVector.get(10), 0);
      assertEquals(7.87, floatVector.get(12), 0);
      assertEquals(8.56, floatVector.get(14), 0);

      try {
        floatVector.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      floatVector.setSafe(16, 9.53);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(1.55, floatVector.get(0), 0);
      assertEquals(2.53, floatVector.get(2), 0);
      assertEquals(3.36, floatVector.get(4), 0);
      assertEquals(4.82, floatVector.get(6), 0);
      assertEquals(5.67, floatVector.get(8), 0);
      assertEquals(6.67, floatVector.get(10), 0);
      assertEquals(7.87, floatVector.get(12), 0);
      assertEquals(8.56, floatVector.get(14), 0);
      assertEquals(9.53, floatVector.get(16), 0);

      /* reset the vector */
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, true, floatVector.isNull(i));
      }
    }
  }

  @Test /* UInt4Vector */
  public void testNullableFixedType1() {

    // Create a new value vector for 1024 integers.
    try (final UInt4Vector vector = newVector(UInt4Vector.class, EMPTY_SCHEMA_PATH, new ArrowType.Int(32, false), allocator);) {
      boolean error = false;
      int initialCapacity = 1024;

      vector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      assertEquals(initialCapacity, vector.getValueCapacity());

      // Put and set a few values
      vector.set(0, 100);
      vector.set(1, 101);
      vector.set(100, 102);
      vector.set(1022, 103);
      vector.set(1023, 104);

      /* check vector contents */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(1022));
      assertEquals(104, vector.get(1023));

      int val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i <= 99 || j <= 1021; i++, j++) {
        if (i <= 99) {
          assertTrue(vector.isNull(i));
        }
        if(j <= 1021) {
          assertTrue(vector.isNull(j));
        }
      }

      try {
        vector.set(1024, 10000);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      try {
        vector.get(1024);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* should trigger a realloc of the underlying bitvector and valuevector */
      vector.setSafe(1024, 10000);

      /* check new capacity */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector contents should still be intact after realloc */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(1022));
      assertEquals(104, vector.get(1023));
      assertEquals(10000, vector.get(1024));

      val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i < 99 || j < 1021; i++, j++) {
        if (i <= 99) {
          assertTrue(vector.isNull(i));
        }
        if(j <= 1021) {
          assertTrue(vector.isNull(j));
        }
      }

      /* reset the vector */
      vector.reset();

       /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }
    }
  }

  @Test /* Float4Vector */
  public void testNullableFixedType2() {
    // Create a new value vector for 1024 integers
    try (final Float4Vector vector = newVector(Float4Vector.class, EMPTY_SCHEMA_PATH, MinorType.FLOAT4, allocator);) {
      boolean error = false;
      int initialCapacity = 16;

      vector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      assertEquals(initialCapacity, vector.getValueCapacity());

      /* populate the vector */
      vector.set(0, 100.5f);
      vector.set(2, 201.5f);
      vector.set(4, 300.3f);
      vector.set(6, 423.8f);
      vector.set(8, 555.6f);
      vector.set(10, 66.6f);
      vector.set(12, 78.8f);
      vector.set(14, 89.5f);

      try {
        vector.set(16, 90.5f);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      assertEquals(100.5f, vector.get(0), 0);
      assertTrue(vector.isNull(1));
      assertEquals(201.5f, vector.get(2), 0);
      assertTrue(vector.isNull(3));
      assertEquals(300.3f, vector.get(4), 0);
      assertTrue(vector.isNull(5));
      assertEquals(423.8f, vector.get(6), 0);
      assertTrue(vector.isNull(7));
      assertEquals(555.6f, vector.get(8), 0);
      assertTrue(vector.isNull(9));
      assertEquals(66.6f, vector.get(10), 0);
      assertTrue(vector.isNull(11));
      assertEquals(78.8f, vector.get(12), 0);
      assertTrue(vector.isNull(13));
      assertEquals(89.5f, vector.get(14), 0);
      assertTrue(vector.isNull(15));

      try {
        vector.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      vector.setSafe(16, 90.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(100.5f, vector.get(0), 0);
      assertTrue(vector.isNull(1));
      assertEquals(201.5f, vector.get(2), 0);
      assertTrue(vector.isNull(3));
      assertEquals(300.3f, vector.get(4), 0);
      assertTrue(vector.isNull(5));
      assertEquals(423.8f, vector.get(6), 0);
      assertTrue(vector.isNull(7));
      assertEquals(555.6f, vector.get(8), 0);
      assertTrue(vector.isNull(9));
      assertEquals(66.6f, vector.get(10), 0);
      assertTrue(vector.isNull(11));
      assertEquals(78.8f, vector.get(12), 0);
      assertTrue(vector.isNull(13));
      assertEquals(89.5f, vector.get(14), 0);
      assertTrue(vector.isNull(15));

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }
    }
  }

  @Test /* IntVector */
  public void testNullableFixedType3() {
    // Create a new value vector for 1024 integers
    try (final IntVector vector = newVector(IntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, allocator)) {
      boolean error = false;
      int initialCapacity = 1024;

      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, vector.getValueCapacity());
      /* allocate space for 4KB data (1024 * 4) */
      vector.allocateNew(initialCapacity);
      /* underlying buffer should be able to store 16 values */
      assertEquals(initialCapacity, vector.getValueCapacity());

      vector.set(0, 1);
      vector.set(1, 2);
      vector.set(100, 3);
      vector.set(1022, 4);
      vector.set(1023, 5);

      /* check vector contents */
      int j = 1;
      for(int i = 0; i <= 1023; i++) {
        if((i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
        }
        else {
          assertFalse("null data not expected at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, j, vector.get(i));
          j++;
        }
      }

      vector.setValueCount(1024);
      Field field = vector.getField();

      List<ArrowBuf> buffers = vector.getFieldBuffers();

      assertEquals(2, buffers.size());

      ArrowBuf validityVectorBuf = buffers.get(0);

      /* bitvector tracks 1024 integers --> 1024 bits --> 128 bytes */
      assertEquals(128, validityVectorBuf.readableBytes());
      assertEquals(3, validityVectorBuf.getByte(0)); // 1st and second bit defined
      for (int i = 1; i < 12; i++) {
        assertEquals(0, validityVectorBuf.getByte(i)); // nothing defined until 100
      }
      assertEquals(16, validityVectorBuf.getByte(12)); // 100th bit is defined (12 * 8 + 4)
      for (int i = 13; i < 127; i++) {
        assertEquals(0, validityVectorBuf.getByte(i)); // nothing defined between 100th and 1022nd
      }
      assertEquals(-64, validityVectorBuf.getByte(127)); // 1022nd and 1023rd bit defined

      /* this should trigger a realloc() */
      vector.setSafe(1024, 6);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should still be intact after realloc */
      j = 1;
      for(int i = 0; i < (initialCapacity * 2); i++) {
        if((i > 1024) || (i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
        }
        else {
          assertFalse("null data not expected at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, j, vector.get(i));
          j++;
        }
      }

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }

      vector.allocateNew(4096);
      // vector has been erased
      for(int i = 0; i < 4096; i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }
    }
  }

  @Test /* IntVector */
  public void testNullableFixedType4() {
    try (final IntVector vector = newVector(IntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, allocator)) {

      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      int valueCapacity = vector.getValueCapacity();
      assertEquals(vector.INITIAL_VALUE_ALLOCATION, valueCapacity);

      int baseValue = 20000;

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.set(i, baseValue + i);
        }
      }

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, (baseValue + i), vector.get(i));
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      vector.setSafe(valueCapacity, 20000000);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      for (int i = 0; i < vector.getValueCapacity(); i++) {
        if (i == valueCapacity) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, 20000000, vector.get(i));
        } else if (i < valueCapacity) {
          if ((i & 1) == 1) {
            assertFalse("unexpected null value at index: " + i, vector.isNull(i));
            assertEquals("unexpected value at index: " + i, (baseValue + i), vector.get(i));
          }
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      vector.zeroVector();

      for (int i = 0; i < vector.getValueCapacity(); i+=2) {
          vector.set(i, baseValue + i);
      }

      for (int i = 0; i < vector.getValueCapacity(); i++) {
        if (i%2 == 0) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, (baseValue + i), vector.get(i));
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      vector.setSafe((valueCapacity *  2) + 1000, 400000000);
      assertEquals(valueCapacity * 4, vector.getValueCapacity());

      for (int i = 0; i < vector.getValueCapacity(); i++) {
        if (i == (valueCapacity*2 + 1000)) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, 400000000, vector.get(i));
        } else if (i < valueCapacity*2 && (i%2) == 0) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, baseValue + i, vector.get(i));
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      /* reset the vector */
      vector.reset();

       /* capacity shouldn't change after reset */
      assertEquals(valueCapacity * 4, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (valueCapacity * 4); i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }
    }
  }

  /*
   * Tests for Variable Width Vectors
   *
   * Covered types as of now
   *
   *  -- VarCharVector
   *  -- VarBinaryVector
   *
   * TODO:
   *
   *  -- VarCharVector
   *  -- VarBinaryVector
   */

  @Test /* VarCharVector */
  public void testNullableVarType1() {

    // Create a new value vector for 1024 integers.
    try (final VarCharVector vector = newVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      vector.set(0, STR1);
      vector.set(1, STR2);
      vector.set(2, STR3);
      vector.setSafe(3, STR3, 1, STR3.length - 1);
      vector.setSafe(4, STR3, 2, STR3.length - 2);
      ByteBuffer STR3ByteBuffer = ByteBuffer.wrap(STR3);
      vector.setSafe(5, STR3ByteBuffer, 1, STR3.length - 1);
      vector.setSafe(6, STR3ByteBuffer, 2, STR3.length - 2);

      // Check the sample strings.
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(3));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(4));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(5));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(6));

      // Ensure null value throws.
      boolean b = false;
      try {
        vector.get(7);
      } catch (IllegalStateException e) {
        b = true;
      } finally {
        assertTrue(b);
      }
    }
  }

  @Test /* VarBinaryVector */
  public void testNullableVarType2() {

    // Create a new value vector for 1024 integers.
    try (final VarBinaryVector vector = newVarBinaryVector(EMPTY_SCHEMA_PATH, allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      vector.set(0, STR1);
      vector.set(1, STR2);
      vector.set(2, STR3);
      vector.setSafe(3, STR3, 1, STR3.length - 1);
      vector.setSafe(4, STR3, 2, STR3.length - 2);
      ByteBuffer STR3ByteBuffer = ByteBuffer.wrap(STR3);
      vector.setSafe(5, STR3ByteBuffer, 1, STR3.length - 1);
      vector.setSafe(6, STR3ByteBuffer, 2, STR3.length - 2);

      // Check the sample strings.
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(3));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(4));
      assertArrayEquals(Arrays.copyOfRange(STR3, 1, STR3.length), vector.get(5));
      assertArrayEquals(Arrays.copyOfRange(STR3, 2, STR3.length), vector.get(6));

      // Ensure null value throws.
      boolean b = false;
      try {
        vector.get(7);
      } catch (IllegalStateException e) {
        b = true;
      } finally {
        assertTrue(b);
      }
    }
  }


  /*
   * generic tests
   *
   *  -- lastSet() and setValueCount()
   *  -- fillEmpties()
   *  -- VectorLoader and VectorUnloader
   *  -- some realloc tests
   *
   *  TODO:
   *
   *  The realloc() related tests below should be moved up and we need to
   *  add realloc related tests (edge cases) for more vector types.
   */

  @Test /* Float8Vector */
  public void testReallocAfterVectorTransfer1() {
    try (final Float8Vector vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final int initialDefaultCapacity = 4096;
      boolean error = false;

      /* use the default capacity; 4096*8 => 32KB */
      vector.allocateNew();

      assertEquals(initialDefaultCapacity, vector.getValueCapacity());

      double baseValue = 100.375;

      for (int i = 0; i < initialDefaultCapacity; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      /* the above setSafe calls should not have triggered a realloc as
       * we are within the capacity. check the vector contents
       */
      assertEquals(initialDefaultCapacity, vector.getValueCapacity());

      for (int i = 0; i < initialDefaultCapacity; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialDefaultCapacity, baseValue + (double)initialDefaultCapacity);
      assertEquals(initialDefaultCapacity * 2, vector.getValueCapacity());

      for (int i = initialDefaultCapacity + 1; i < (initialDefaultCapacity * 2); i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < (initialDefaultCapacity * 2); i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialDefaultCapacity * 2, baseValue + (double)(initialDefaultCapacity * 2));
      assertEquals(initialDefaultCapacity * 4, vector.getValueCapacity());

      for (int i = (initialDefaultCapacity * 2) + 1; i < (initialDefaultCapacity * 4); i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < (initialDefaultCapacity * 4); i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* at this point we are working with a 128KB buffer data for this
       * vector. now let's transfer this vector
       */

      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();

      Float8Vector toVector = (Float8Vector)transferPair.getTo();

      /* now let's realloc the toVector */
      toVector.reAlloc();
      assertEquals(initialDefaultCapacity * 8, toVector.getValueCapacity());

      for (int i = 0; i < (initialDefaultCapacity * 8); i++) {
        if (i < (initialDefaultCapacity * 4)) {
          assertEquals(baseValue + (double)i, toVector.get(i), 0);
        }
        else {
          assertTrue(toVector.isNull(i));
        }
      }

      toVector.close();
    }
  }

  @Test /* Float8Vector */
  public void testReallocAfterVectorTransfer2() {
    try (final Float8Vector vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final int initialDefaultCapacity = 4096;
      boolean error = false;

      vector.allocateNew(initialDefaultCapacity);

      assertEquals(initialDefaultCapacity, vector.getValueCapacity());

      double baseValue = 100.375;

      for (int i = 0; i < initialDefaultCapacity; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      /* the above setSafe calls should not have triggered a realloc as
       * we are within the capacity. check the vector contents
       */
      assertEquals(initialDefaultCapacity, vector.getValueCapacity());

      for (int i = 0; i < initialDefaultCapacity; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialDefaultCapacity, baseValue + (double)initialDefaultCapacity);
      assertEquals(initialDefaultCapacity * 2, vector.getValueCapacity());

      for (int i = initialDefaultCapacity + 1; i < (initialDefaultCapacity * 2); i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < (initialDefaultCapacity * 2); i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialDefaultCapacity * 2, baseValue + (double)(initialDefaultCapacity * 2));
      assertEquals(initialDefaultCapacity * 4, vector.getValueCapacity());

      for (int i = (initialDefaultCapacity * 2) + 1; i < (initialDefaultCapacity * 4); i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < (initialDefaultCapacity * 4); i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* at this point we are working with a 128KB buffer data for this
       * vector. now let's transfer this vector
       */

      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();

      Float8Vector toVector = (Float8Vector)transferPair.getTo();

      /* check toVector contents before realloc */
      for (int i = 0; i < (initialDefaultCapacity * 4); i++) {
        assertFalse("unexpected null value at index: " + i, toVector.isNull(i));
        double value = toVector.get(i);
        assertEquals("unexpected value at index: " + i, baseValue + (double)i, value, 0);
      }

      /* now let's realloc the toVector and check contents again */
      toVector.reAlloc();
      assertEquals(initialDefaultCapacity * 8, toVector.getValueCapacity());

      for (int i = 0; i < (initialDefaultCapacity * 8); i++) {
        if (i < (initialDefaultCapacity * 4)) {
          assertFalse("unexpected null value at index: " + i, toVector.isNull(i));
          double value = toVector.get(i);
          assertEquals("unexpected value at index: " + i, baseValue + (double)i, value, 0);
        }
        else {
          assertTrue("unexpected non-null value at index: " + i, toVector.isNull(i));
        }
      }

      toVector.close();
    }
  }

  @Test /* VarCharVector */
  public void testReallocAfterVectorTransfer3() {
    try (final VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      /* 4096 values with 10 byte per record */
      vector.allocateNew(4096 * 10, 4096);
      int valueCapacity = vector.getValueCapacity();
      assertEquals(4096, valueCapacity);

      /* populate the vector */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        }
        else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        }
        else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, STR2, 0, STR2.length);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        }
        else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        }
        else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity + 10, STR2, 0, STR2.length);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        }
        else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        }
        else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* we are potentially working with 4x the size of vector buffer
       * that we initially started with. Now let's transfer the vector.
       */

      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();
      VarCharVector toVector = (VarCharVector)transferPair.getTo();
      valueCapacity = toVector.getValueCapacity();

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, toVector.get(i));
        }
        else {
          assertArrayEquals(STR2, toVector.get(i));
        }
      }

      toVector.close();
    }
  }

  @Test /* IntVector */
  public void testReallocAfterVectorTransfer4() {
    try (final IntVector vector = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {

      /* 4096 values  */
      vector.allocateNew(4096);
      int valueCapacity = vector.getValueCapacity();
      assertEquals(4096, valueCapacity);

      /* populate the vector */
      int baseValue = 1000;
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 0) {
          vector.set(i, 1000 + i);
        }
      }

      /* Check the vector output */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 0) {
          assertEquals(1000 + i, vector.get(i));
        }
        else {
          assertTrue(vector.isNull(i));
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, 10000000);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 0) {
          vector.set(i, 1000 + i);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 0) {
          assertEquals(1000 + i, vector.get(i));
        }
        else {
          assertTrue(vector.isNull(i));
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity, 10000000);
      assertEquals(valueCapacity * 2, vector.getValueCapacity());

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 0) {
          vector.set(i, 1000 + i);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 0) {
          assertEquals(1000 + i, vector.get(i));
        }
        else {
          assertTrue(vector.isNull(i));
        }
      }

      /* we are potentially working with 4x the size of vector buffer
       * that we initially started with. Now let's transfer the vector.
       */

      TransferPair transferPair = vector.getTransferPair(allocator);
      transferPair.transfer();
      IntVector toVector = (IntVector)transferPair.getTo();
      /* value capacity of source and target vectors should be same after
       * the transfer.
       */
      assertEquals(valueCapacity, toVector.getValueCapacity());

      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 0) {
          assertEquals(1000 + i, toVector.get(i));
        }
        else {
          assertTrue(toVector.isNull(i));
        }
      }

      toVector.close();
    }
  }

  @Test
  public void testReAllocFixedWidthVector() {
    // Create a new value vector for 1024 integers
    try (final Float4Vector vector = newVector(Float4Vector.class, EMPTY_SCHEMA_PATH, MinorType.FLOAT4, allocator)) {
      vector.allocateNew(1024);

      assertEquals(1024, vector.getValueCapacity());

      // Put values in indexes that fall within the initial allocation
      vector.setSafe(0, 100.1f);
      vector.setSafe(100, 102.3f);
      vector.setSafe(1023, 104.5f);

      // Now try to put values in space that falls beyond the initial allocation
      vector.setSafe(2000, 105.5f);

      // Check valueCapacity is more than initial allocation
      assertEquals(1024 * 2, vector.getValueCapacity());

      assertEquals(100.1f, vector.get(0), 0);
      assertEquals(102.3f, vector.get(100), 0);
      assertEquals(104.5f, vector.get(1023), 0);
      assertEquals(105.5f, vector.get(2000), 0);

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for ValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when all values are inserted into the
      // vector
      vector.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testReAllocVariableWidthVector() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();
      assertEquals(4095, initialCapacity);

      /* Put values in indexes that fall within the initial allocation */
      vector.setSafe(0, STR1, 0, STR1.length);
      vector.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

      /* the above set calls should NOT have triggered a realloc */
      initialCapacity = vector.getValueCapacity();
      assertEquals(4095, initialCapacity);

      /* Now try to put values in space that falls beyond the initial allocation */
      vector.setSafe(initialCapacity + 200, STR3, 0, STR3.length);

      /* Check valueCapacity is more than initial allocation */
      assertEquals(((initialCapacity + 1) * 2) - 1, vector.getValueCapacity());

      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(initialCapacity - 1));
      assertArrayEquals(STR3, vector.get(initialCapacity + 200));

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for ValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
      vector.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testFillEmptiesNotOverfill() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();
      assertEquals(4095, initialCapacity);

      vector.setSafe(4094, "hello".getBytes(), 0, 5);
      /* the above set method should NOT have trigerred a realloc */
      initialCapacity = vector.getValueCapacity();
      assertEquals(4095, initialCapacity);

      vector.setValueCount(4095);
      assertEquals(4096 * vector.OFFSET_WIDTH, vector.getFieldBuffers().get(1).capacity());
      initialCapacity = vector.getValueCapacity();
      assertEquals(4095, initialCapacity);
    }
  }

  @Test
  public void testCopyFromWithNulls() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final VarCharVector vector2 = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {

      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      vector.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector.getObject(i).toString());
        }
      }

      vector2.allocateNew();
      capacity = vector2.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* NO reAlloc() should have happened in copyFrom */
      capacity = vector2.getValueCapacity();
      assertEquals(4095, capacity);

      vector2.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }
    }
  }

  @Test
  public void testCopyFromWithNulls1() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final VarCharVector vector2 = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {

      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      vector.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector.getObject(i).toString());
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024 * 10, 1024);

      capacity = vector2.getValueCapacity();
      assertEquals(1024, capacity);

      for (int i = 0; i < 4095; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* 2 reAllocs should have happened in copyFromSafe() */
      capacity = vector2.getValueCapacity();
      assertEquals(4096, capacity);

      vector2.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }
    }
  }

  @Test
  public void testSetLastSetUsage() {
    try (final VarCharVector vector = new VarCharVector("myvector", allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(Integer.toString(-1), Integer.toString(vector.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      /*
       * If we don't do setLastSe(5) before setValueCount(), then the latter will corrupt
       * the value vector by filling in all positions [0,valuecount-1] will empty byte arrays.
       * Run the test by commenting out next line and we should see incorrect vector output.
       */
      vector.setLastSet(5);
      vector.setValueCount(20);

      /* Check current lastSet */
      assertEquals(Integer.toString(19), Integer.toString(vector.getLastSet()));

      /* Check the vector output again */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(6)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(7)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(8)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(9)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(10)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(11)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(12)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(13)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(14)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(15)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(16)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(17)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(18)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(19)));

      /* Check offsets */
      assertEquals(Integer.toString(0),
              Integer.toString(vector.offsetBuffer.getInt(0 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(6),
              Integer.toString(vector.offsetBuffer.getInt(1 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(16),
              Integer.toString(vector.offsetBuffer.getInt(2 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(21),
              Integer.toString(vector.offsetBuffer.getInt(3 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(30),
              Integer.toString(vector.offsetBuffer.getInt(4 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(34),
              Integer.toString(vector.offsetBuffer.getInt(5 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(6 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(7 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(8 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(9 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(10 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(11 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(12 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(13 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(14 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(15 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(16 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(17 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(18 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(19 * vector.OFFSET_WIDTH)));

      vector.set(19, STR6);
      assertArrayEquals(STR6, vector.get(19));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(19 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(46),
              Integer.toString(vector.offsetBuffer.getInt(20 * vector.OFFSET_WIDTH)));
    }
  }

  @Test
  public void testVectorLoadUnload() {

    try (final VarCharVector vector1 = new VarCharVector("myvector", allocator)) {
      vector1.allocateNew(1024 * 10, 1024);

      vector1.set(0, STR1);
      vector1.set(1, STR2);
      vector1.set(2, STR3);
      vector1.set(3, STR4);
      vector1.set(4, STR5);
      vector1.set(5, STR6);
      assertEquals(Integer.toString(5), Integer.toString(vector1.getLastSet()));
      vector1.setValueCount(15);
      assertEquals(Integer.toString(14), Integer.toString(vector1.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, vector1.get(0));
      assertArrayEquals(STR2, vector1.get(1));
      assertArrayEquals(STR3, vector1.get(2));
      assertArrayEquals(STR4, vector1.get(3));
      assertArrayEquals(STR5, vector1.get(4));
      assertArrayEquals(STR6, vector1.get(5));

      Field field = vector1.getField();
      String fieldName = field.getName();

      List<Field> fields = new ArrayList<Field>();
      List<FieldVector> fieldVectors = new ArrayList<FieldVector>();

      fields.add(field);
      fieldVectors.add(vector1);

      Schema schema = new Schema(fields);

      VectorSchemaRoot schemaRoot1 = new VectorSchemaRoot(schema, fieldVectors, vector1.getValueCount());
      VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("new vector", 0, Long.MAX_VALUE);
          VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {

        VectorLoader vectorLoader = new VectorLoader(schemaRoot2);
        vectorLoader.load(recordBatch);

        VarCharVector vector2 = (VarCharVector) schemaRoot2.getVector(fieldName);
        /*
         * lastSet would have internally been set by VectorLoader.load() when it invokes
         * loadFieldBuffers.
         */
        assertEquals(Integer.toString(14), Integer.toString(vector2.getLastSet()));
        vector2.setValueCount(25);
        assertEquals(Integer.toString(24), Integer.toString(vector2.getLastSet()));

        /* Check the vector output */
        assertArrayEquals(STR1, vector2.get(0));
        assertArrayEquals(STR2, vector2.get(1));
        assertArrayEquals(STR3, vector2.get(2));
        assertArrayEquals(STR4, vector2.get(3));
        assertArrayEquals(STR5, vector2.get(4));
        assertArrayEquals(STR6, vector2.get(5));
      }
    }
  }

  @Test
  public void testFillEmptiesUsage() {
    try (final VarCharVector vector = new VarCharVector("myvector", allocator)) {

      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(Integer.toString(-1), Integer.toString(vector.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      vector.setLastSet(5);
      /* fill empty byte arrays from index [6, 9] */
      vector.fillEmpties(10);

      /* Check current lastSet */
      assertEquals(Integer.toString(9), Integer.toString(vector.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(6)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(7)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(8)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(9)));

      setBytes(10, STR1, vector);
      setBytes(11, STR2, vector);

      vector.setLastSet(11);
      /* fill empty byte arrays from index [12, 14] */
      vector.setValueCount(15);

      /* Check current lastSet */
      assertEquals(Integer.toString(14), Integer.toString(vector.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(6)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(7)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(8)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(9)));
      assertArrayEquals(STR1, vector.get(10));
      assertArrayEquals(STR2, vector.get(11));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(12)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(13)));
      assertEquals(Integer.toString(0), Integer.toString(vector.getValueLength(14)));

      /* Check offsets */
      assertEquals(Integer.toString(0),
              Integer.toString(vector.offsetBuffer.getInt(0 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(6),
              Integer.toString(vector.offsetBuffer.getInt(1 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(16),
              Integer.toString(vector.offsetBuffer.getInt(2 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(21),
              Integer.toString(vector.offsetBuffer.getInt(3 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(30),
              Integer.toString(vector.offsetBuffer.getInt(4 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(34),
              Integer.toString(vector.offsetBuffer.getInt(5 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(6 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(7 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(8 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(9 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(40),
              Integer.toString(vector.offsetBuffer.getInt(10 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(46),
              Integer.toString(vector.offsetBuffer.getInt(11 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(56),
              Integer.toString(vector.offsetBuffer.getInt(12 * vector.OFFSET_WIDTH)));

      assertEquals(Integer.toString(56),
              Integer.toString(vector.offsetBuffer.getInt(13 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(56),
              Integer.toString(vector.offsetBuffer.getInt(14 * vector.OFFSET_WIDTH)));
      assertEquals(Integer.toString(56),
              Integer.toString(vector.offsetBuffer.getInt(15 * vector.OFFSET_WIDTH)));
    }
  }

  @Test /* VarCharVector */
  public void testGetBufferAddress1() {

    try (final VarCharVector vector = new VarCharVector("myvector", allocator)) {
      vector.allocateNew(1024 * 10, 1024);

      /* populate the vector */
      vector.set(0, STR1);
      vector.set(1, STR2);
      vector.set(2, STR3);
      vector.set(3, STR4);
      vector.set(4, STR5);
      vector.set(5, STR6);

      vector.setValueCount(15);

      /* check the vector output */
      assertArrayEquals(STR1, vector.get(0));
      assertArrayEquals(STR2, vector.get(1));
      assertArrayEquals(STR3, vector.get(2));
      assertArrayEquals(STR4, vector.get(3));
      assertArrayEquals(STR5, vector.get(4));
      assertArrayEquals(STR6, vector.get(5));

      List<ArrowBuf> buffers = vector.getFieldBuffers();
      long bitAddress = vector.getValidityBufferAddress();
      long offsetAddress = vector.getOffsetBufferAddress();
      long dataAddress = vector.getDataBufferAddress();

      assertEquals(3, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
      assertEquals(dataAddress, buffers.get(2).memoryAddress());
    }
  }

  @Test /* IntVector */
  public void testGetBufferAddress2() {
    try (final IntVector vector = new IntVector("myvector", allocator)) {
      boolean error = false;
      vector.allocateNew(16);

      /* populate the vector */
      for(int i = 0; i < 16; i += 2) {
        vector.set(i, i+10);
      }

      /* check the vector output */
      for(int i = 0; i < 16; i += 2) {
        assertEquals(i+10, vector.get(i));
      }

      List<ArrowBuf> buffers = vector.getFieldBuffers();
      long bitAddress = vector.getValidityBufferAddress();
      long dataAddress = vector.getDataBufferAddress();

      try {
        long offsetAddress = vector.getOffsetBufferAddress();
      }
      catch (UnsupportedOperationException ue) {
        error = true;
      }
      finally {
        assertTrue(error);
      }

      assertEquals(2, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(dataAddress, buffers.get(1).memoryAddress());
    }
  }

  @Test
  public void testMultipleClose() {
    BufferAllocator vectorAllocator = allocator.newChildAllocator("vector_allocator", 0, Long.MAX_VALUE);
    IntVector vector = newVector(IntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, vectorAllocator);
    vector.close();
    vectorAllocator.close();
    vector.close();
    vectorAllocator.close();
  }

  /* this method is used by the tests to bypass the vector set methods that manipulate
   * lastSet. The method is to test the lastSet property and that's why we load the vector
   * in a way that lastSet is not set automatically.
   */
  public static void setBytes(int index, byte[] bytes, VarCharVector vector) {
    final int currentOffset = vector.offsetBuffer.getInt(index * vector.OFFSET_WIDTH);

    BitVectorHelper.setValidityBitToOne(vector.validityBuffer, index);
    vector.offsetBuffer.setInt((index + 1) * vector.OFFSET_WIDTH, currentOffset + bytes.length);
    vector.valueBuffer.setBytes(currentOffset, bytes, 0, bytes.length);
  }

  @Test /* VarCharVector */
  public void testSetInitialCapacity() {
    try (final VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator)) {

      /* use the default 8 data bytes on average per element */
      vector.setInitialCapacity(4096);
      vector.allocateNew();
      assertEquals(4096, vector.getValueCapacity());
      assertEquals(4096 * 8, vector.getDataBuffer().capacity());

      vector.setInitialCapacity(4096, 1);
      vector.allocateNew();
      assertEquals(4096, vector.getValueCapacity());
      assertEquals(4096, vector.getDataBuffer().capacity());

      vector.setInitialCapacity(4096, 0.1);
      vector.allocateNew();
      assertEquals(4096, vector.getValueCapacity());
      assertEquals(512, vector.getDataBuffer().capacity());

      vector.setInitialCapacity(4096, 0.01);
      vector.allocateNew();
      assertEquals(4096, vector.getValueCapacity());
      assertEquals(64, vector.getDataBuffer().capacity());

      vector.setInitialCapacity(5, 0.01);
      vector.allocateNew();
      assertEquals(7, vector.getValueCapacity());
      assertEquals(2, vector.getDataBuffer().capacity());
    }
  }
}
