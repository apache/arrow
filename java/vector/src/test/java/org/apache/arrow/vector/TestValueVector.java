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

import static org.apache.arrow.vector.TestUtils.newVarBinaryVector;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.apache.arrow.vector.TestUtils.newVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestValueVector {

  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private static final Charset utf8Charset = Charset.forName("UTF-8");
  private static final byte[] STR1 = "AAAAA1".getBytes(utf8Charset);
  private static final byte[] STR2 = "BBBBBBBBB2".getBytes(utf8Charset);
  private static final byte[] STR3 = "CCCC3".getBytes(utf8Charset);
  private static final byte[] STR4 = "DDDDDDDD4".getBytes(utf8Charset);
  private static final byte[] STR5 = "EEE5".getBytes(utf8Charset);
  private static final byte[] STR6 = "FFFFF6".getBytes(utf8Charset);
  private static final int MAX_VALUE_COUNT =
      (int)(Integer.getInteger("arrow.vector.max_allocation_bytes", Integer.MAX_VALUE) / 7);
  private static final int MAX_VALUE_COUNT_8BYTE = (int)(MAX_VALUE_COUNT / 2);

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
      assertTrue(initialCapacity >= 1024);

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
        vector.set(initialCapacity, 10000);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      try {
        vector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      vector.setSafe(initialCapacity, 10000);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

      /* check vector data after realloc */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(1022));
      assertEquals(104, vector.get(1023));
      assertEquals(10000, vector.get(initialCapacity));

      /* reset the vector */
      int capacityBeforeReset = vector.getValueCapacity();
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
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
        intVector.setInitialCapacity(MAX_VALUE_COUNT * 2);
      } catch (OversizedAllocationException oe) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      intVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, intVector.getValueCapacity());

      /* allocate 64 bytes (16 * 4) */
      intVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertTrue(intVector.getValueCapacity() >= initialCapacity);
      initialCapacity = intVector.getValueCapacity();

      /* populate the vector */
      int j = 1;
      for (int i = 0; i < initialCapacity; i += 2) {
        intVector.set(i, j);
        j++;
      }

      try {
        intVector.set(initialCapacity, j);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      j = 1;
      for (int i = 0; i < initialCapacity; i += 2) {
        assertEquals("unexpected value at index: " + i, j, intVector.get(i));
        j++;
      }

      try {
        intVector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      intVector.setSafe(initialCapacity, j);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(intVector.getValueCapacity() >= initialCapacity * 2);

      /* vector data should still be intact after realloc */
      j = 1;
      for (int i = 0; i <= initialCapacity; i += 2) {
        assertEquals("unexpected value at index: " + i, j, intVector.get(i));
        j++;
      }

      /* reset the vector */
      int capacityBeforeRealloc = intVector.getValueCapacity();
      intVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeRealloc, intVector.getValueCapacity());

      /* vector data should have been zeroed out */
      for (int i = 0; i < capacityBeforeRealloc; i++) {
        assertEquals("non-zero data not expected at index: " + i, true, intVector.isNull(i));
      }
    }
  }

  @Test /* VarCharVector */
  public void testSizeOfValueBuffer() {
    try (final VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      int valueCount = 100;
      int currentSize = 0;
      vector.setInitialCapacity(valueCount);
      vector.allocateNew();
      vector.setValueCount(valueCount);
      for (int i = 0; i < valueCount; i++) {
        currentSize += i;
        vector.setSafe(i, new byte[i]);
      }

      assertEquals(currentSize, vector.sizeOfValueBuffer());
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
        floatVector.setInitialCapacity(MAX_VALUE_COUNT * 2);
      } catch (OversizedAllocationException oe) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      floatVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, floatVector.getValueCapacity());

      /* allocate 64 bytes (16 * 4) */
      floatVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertTrue(floatVector.getValueCapacity() >= initialCapacity);
      initialCapacity = floatVector.getValueCapacity();

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
        floatVector.set(initialCapacity, 9.5f);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
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
        floatVector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      floatVector.setSafe(initialCapacity, 9.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(floatVector.getValueCapacity() >= initialCapacity * 2);

      /* vector data should still be intact after realloc */
      assertEquals(1.5f, floatVector.get(0), 0);
      assertEquals(2.5f, floatVector.get(2), 0);
      assertEquals(3.3f, floatVector.get(4), 0);
      assertEquals(4.8f, floatVector.get(6), 0);
      assertEquals(5.6f, floatVector.get(8), 0);
      assertEquals(6.6f, floatVector.get(10), 0);
      assertEquals(7.8f, floatVector.get(12), 0);
      assertEquals(8.5f, floatVector.get(14), 0);
      assertEquals(9.5f, floatVector.get(initialCapacity), 0);

      /* reset the vector */
      int capacityBeforeReset = floatVector.getValueCapacity();
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
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
        floatVector.setInitialCapacity(MAX_VALUE_COUNT_8BYTE * 2);
      } catch (OversizedAllocationException oe) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      floatVector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, floatVector.getValueCapacity());

      /* allocate 128 bytes (16 * 8) */
      floatVector.allocateNew();
      /* underlying buffer should be able to store 16 values */
      assertTrue(floatVector.getValueCapacity() >= initialCapacity);
      initialCapacity = floatVector.getValueCapacity();

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
        floatVector.set(initialCapacity, 9.53);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
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
        floatVector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      floatVector.setSafe(initialCapacity, 9.53);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(floatVector.getValueCapacity() >= initialCapacity * 2);

      /* vector data should still be intact after realloc */
      assertEquals(1.55, floatVector.get(0), 0);
      assertEquals(2.53, floatVector.get(2), 0);
      assertEquals(3.36, floatVector.get(4), 0);
      assertEquals(4.82, floatVector.get(6), 0);
      assertEquals(5.67, floatVector.get(8), 0);
      assertEquals(6.67, floatVector.get(10), 0);
      assertEquals(7.87, floatVector.get(12), 0);
      assertEquals(8.56, floatVector.get(14), 0);
      assertEquals(9.53, floatVector.get(initialCapacity), 0);

      /* reset the vector */
      int capacityBeforeReset = floatVector.getValueCapacity();
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
        assertEquals("non-zero data not expected at index: " + i, true, floatVector.isNull(i));
      }
    }
  }

  @Test /* UInt4Vector */
  public void testNullableFixedType1() {

    // Create a new value vector for 1024 integers.
    try (final UInt4Vector vector = newVector(UInt4Vector.class, EMPTY_SCHEMA_PATH, new ArrowType.Int(32, false),
      allocator);) {
      boolean error = false;
      int initialCapacity = 1024;

      vector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      assertTrue(vector.getValueCapacity() >= initialCapacity);
      initialCapacity = vector.getValueCapacity();

      // Put and set a few values
      vector.set(0, 100);
      vector.set(1, 101);
      vector.set(100, 102);
      vector.set(initialCapacity - 2, 103);
      vector.set(initialCapacity - 1, 104);

      /* check vector contents */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(initialCapacity - 2));
      assertEquals(104, vector.get(initialCapacity - 1));

      int val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i <= 99 || j <= initialCapacity - 3; i++, j++) {
        if (i <= 99) {
          assertTrue(vector.isNull(i));
        }
        if (j <= initialCapacity - 3) {
          assertTrue(vector.isNull(j));
        }
      }

      try {
        vector.set(initialCapacity, 10000);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      try {
        vector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* should trigger a realloc of the underlying bitvector and valuevector */
      vector.setSafe(initialCapacity, 10000);

      /* check new capacity */
      assertTrue(vector.getValueCapacity() >= initialCapacity * 2);

      /* vector contents should still be intact after realloc */
      assertEquals(100, vector.get(0));
      assertEquals(101, vector.get(1));
      assertEquals(102, vector.get(100));
      assertEquals(103, vector.get(initialCapacity - 2));
      assertEquals(104, vector.get(initialCapacity - 1));
      assertEquals(10000, vector.get(initialCapacity));

      val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i < 99 || j < initialCapacity - 3; i++, j++) {
        if (i <= 99) {
          assertTrue(vector.isNull(i));
        }
        if (j <= initialCapacity - 3) {
          assertTrue(vector.isNull(j));
        }
      }

      /* reset the vector */
      int capacityBeforeReset = vector.getValueCapacity();
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
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
      assertTrue(vector.getValueCapacity() >= initialCapacity);
      initialCapacity = vector.getValueCapacity();

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
        vector.set(initialCapacity, 90.5f);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
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
        vector.get(initialCapacity);
      } catch (IndexOutOfBoundsException ie) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      vector.setSafe(initialCapacity, 90.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

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
      int capacityBeforeReset = vector.getValueCapacity();
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
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
      /* underlying buffer should be able to store 1024 values */
      assertTrue(vector.getValueCapacity() >= initialCapacity);
      initialCapacity = vector.getValueCapacity();

      vector.set(0, 1);
      vector.set(1, 2);
      vector.set(100, 3);
      vector.set(1022, 4);
      vector.set(1023, 5);

      /* check vector contents */
      int j = 1;
      for (int i = 0; i <= 1023; i++) {
        if ((i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
        } else {
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
      assertTrue(validityVectorBuf.readableBytes() >= 128);
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
      vector.setSafe(initialCapacity, 6);

      /* underlying buffer should now be able to store double the number of values */
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

      /* vector data should still be intact after realloc */
      j = 1;
      for (int i = 0; i < (initialCapacity * 2); i++) {
        if ((i > 1023 && i != initialCapacity) || (i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
        } else {
          assertFalse("null data not expected at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, j, vector.get(i));
          j++;
        }
      }

      /* reset the vector */
      int capacityBeforeReset = vector.getValueCapacity();
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(capacityBeforeReset, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for (int i = 0; i < capacityBeforeReset; i++) {
        assertTrue("non-null data not expected at index: " + i, vector.isNull(i));
      }

      vector.allocateNew(initialCapacity * 4);
      // vector has been erased
      for (int i = 0; i < initialCapacity * 4; i++) {
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
      assertTrue(vector.getValueCapacity() >= valueCapacity * 2);

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

      for (int i = 0; i < vector.getValueCapacity(); i += 2) {
        vector.set(i, baseValue + i);
      }

      for (int i = 0; i < vector.getValueCapacity(); i++) {
        if (i % 2 == 0) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, (baseValue + i), vector.get(i));
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      int valueCapacityBeforeRealloc = vector.getValueCapacity();
      vector.setSafe(valueCapacityBeforeRealloc + 1000, 400000000);
      assertTrue(vector.getValueCapacity() >= valueCapacity * 4);

      for (int i = 0; i < vector.getValueCapacity(); i++) {
        if (i == (valueCapacityBeforeRealloc + 1000)) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, 400000000, vector.get(i));
        } else if (i < valueCapacityBeforeRealloc && (i % 2) == 0) {
          assertFalse("unexpected null value at index: " + i, vector.isNull(i));
          assertEquals("unexpected value at index: " + i, baseValue + i, vector.get(i));
        } else {
          assertTrue("unexpected non-null value at index: " + i, vector.isNull(i));
        }
      }

      /* reset the vector */
      int valueCapacityBeforeReset = vector.getValueCapacity();
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(valueCapacityBeforeReset, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for (int i = 0; i < valueCapacityBeforeReset; i++) {
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
      ByteBuffer str3ByteBuffer = ByteBuffer.wrap(STR3);
      vector.setSafe(5, str3ByteBuffer, 1, STR3.length - 1);
      vector.setSafe(6, str3ByteBuffer, 2, STR3.length - 2);

      // Set with convenience function
      Text txt = new Text("foo");
      vector.setSafe(7, txt);

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
      boolean b = false;
      assertNull(vector.get(8));
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
      int initialCapacity = 4096;
      boolean error = false;

      /* use the default capacity; 4096*8 => 32KB */
      vector.setInitialCapacity(initialCapacity);
      vector.allocateNew();

      assertTrue(vector.getValueCapacity() >= initialCapacity);
      initialCapacity = vector.getValueCapacity();

      double baseValue = 100.375;

      for (int i = 0; i < initialCapacity; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      /* the above setSafe calls should not have triggered a realloc as
       * we are within the capacity. check the vector contents
       */
      assertEquals(initialCapacity, vector.getValueCapacity());

      for (int i = 0; i < initialCapacity; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialCapacity, baseValue + (double)initialCapacity);
      assertTrue(vector.getValueCapacity() >= initialCapacity * 2);
      int capacityAfterRealloc1 = vector.getValueCapacity();

      for (int i = initialCapacity + 1; i < capacityAfterRealloc1; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < capacityAfterRealloc1; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(capacityAfterRealloc1, baseValue + (double)(capacityAfterRealloc1));
      assertTrue(vector.getValueCapacity() >= initialCapacity * 4);
      int capacityAfterRealloc2 = vector.getValueCapacity();

      for (int i = capacityAfterRealloc1 + 1; i < capacityAfterRealloc2; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < capacityAfterRealloc2; i++) {
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
      assertTrue(toVector.getValueCapacity() >= initialCapacity * 8);

      for (int i = 0; i < toVector.getValueCapacity(); i++) {
        if (i < capacityAfterRealloc2) {
          assertEquals(baseValue + (double)i, toVector.get(i), 0);
        } else {
          assertTrue(toVector.isNull(i));
        }
      }

      toVector.close();
    }
  }

  @Test /* Float8Vector */
  public void testReallocAfterVectorTransfer2() {
    try (final Float8Vector vector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {
      int initialCapacity = 4096;
      boolean error = false;

      vector.allocateNew(initialCapacity);
      assertTrue(vector.getValueCapacity() >= initialCapacity);
      initialCapacity = vector.getValueCapacity();

      double baseValue = 100.375;

      for (int i = 0; i < initialCapacity; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      /* the above setSafe calls should not have triggered a realloc as
       * we are within the capacity. check the vector contents
       */
      assertEquals(initialCapacity, vector.getValueCapacity());

      for (int i = 0; i < initialCapacity; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(initialCapacity, baseValue + (double)initialCapacity);
      assertTrue(vector.getValueCapacity() >= initialCapacity * 2);
      int capacityAfterRealloc1 = vector.getValueCapacity();

      for (int i = initialCapacity + 1; i < capacityAfterRealloc1; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < capacityAfterRealloc1; i++) {
        double value = vector.get(i);
        assertEquals(baseValue + (double)i, value, 0);
      }

      /* this should trigger a realloc */
      vector.setSafe(capacityAfterRealloc1, baseValue + (double)(capacityAfterRealloc1));
      assertTrue(vector.getValueCapacity() >= initialCapacity * 4);
      int capacityAfterRealloc2 = vector.getValueCapacity();

      for (int i = capacityAfterRealloc1 + 1; i < capacityAfterRealloc2; i++) {
        vector.setSafe(i, baseValue + (double)i);
      }

      for (int i = 0; i < capacityAfterRealloc2; i++) {
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
      for (int i = 0; i < toVector.getValueCapacity(); i++) {
        assertFalse("unexpected null value at index: " + i, toVector.isNull(i));
        double value = toVector.get(i);
        assertEquals("unexpected value at index: " + i, baseValue + (double)i, value, 0);
      }

      /* now let's realloc the toVector and check contents again */
      toVector.reAlloc();
      assertTrue(toVector.getValueCapacity() >= initialCapacity * 8);

      for (int i = 0; i < toVector.getValueCapacity(); i++) {
        if (i < capacityAfterRealloc2) {
          assertFalse("unexpected null value at index: " + i, toVector.isNull(i));
          double value = toVector.get(i);
          assertEquals("unexpected value at index: " + i, baseValue + (double)i, value, 0);
        } else {
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
      assertTrue(valueCapacity >= 4096);

      /* populate the vector */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, STR2, 0, STR2.length);
      assertTrue(vector.getValueCapacity() >= 2 * valueCapacity);
      while (vector.getByteCapacity() < 10 * vector.getValueCapacity()) {
        vector.reallocDataBuffer();
      }

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
          assertArrayEquals(STR2, vector.get(i));
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity + 10, STR2, 0, STR2.length);
      assertTrue(vector.getValueCapacity() >= 2 *  valueCapacity);
      while (vector.getByteCapacity() < 10 * vector.getValueCapacity()) {
        vector.reallocDataBuffer();
      }

      /* populate the remaining vector */
      for (int i = valueCapacity; i < vector.getValueCapacity(); i++) {
        if ((i & 1) == 1) {
          vector.set(i, STR1);
        } else {
          vector.set(i, STR2);
        }
      }

      /* Check the vector output */
      valueCapacity = vector.getValueCapacity();
      for (int i = 0; i < valueCapacity; i++) {
        if ((i & 1) == 1) {
          assertArrayEquals(STR1, vector.get(i));
        } else {
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
        } else {
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
      assertTrue(valueCapacity >= 4096);

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
        } else {
          assertTrue(vector.isNull(i));
        }
      }

      /* trigger first realloc */
      vector.setSafe(valueCapacity, 10000000);
      assertTrue(vector.getValueCapacity() >= valueCapacity * 2);

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
        } else {
          assertTrue(vector.isNull(i));
        }
      }

      /* trigger second realloc */
      vector.setSafe(valueCapacity, 10000000);
      assertTrue(vector.getValueCapacity() >= valueCapacity * 2);

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
        } else {
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
        } else {
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

      assertTrue(vector.getValueCapacity() >= 1024);
      int initialCapacity = vector.getValueCapacity();

      // Put values in indexes that fall within the initial allocation
      vector.setSafe(0, 100.1f);
      vector.setSafe(100, 102.3f);
      vector.setSafe(1023, 104.5f);

      // Now try to put values in space that falls beyond the initial allocation
      vector.setSafe(2000, 105.5f);

      // Check valueCapacity is more than initial allocation
      assertTrue(vector.getValueCapacity() >= 2 * initialCapacity);

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
      vector.setInitialCapacity(4095);
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();
      assertTrue(initialCapacity >= 4095);

      /* Put values in indexes that fall within the initial allocation */
      vector.setSafe(0, STR1, 0, STR1.length);
      vector.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

      /* the above set calls should NOT have triggered a realloc */
      assertEquals(initialCapacity, vector.getValueCapacity());

      /* Now try to put values in space that falls beyond the initial allocation */
      vector.setSafe(initialCapacity + 200, STR3, 0, STR3.length);

      /* Check valueCapacity is more than initial allocation */
      assertTrue(initialCapacity  * 2 <=  vector.getValueCapacity());

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
      vector.setInitialCapacity(4095);
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();
      assertTrue(initialCapacity >= 4095);

      vector.setSafe(4094, "hello".getBytes(), 0, 5);
      /* the above set method should NOT have trigerred a realloc */
      assertEquals(initialCapacity, vector.getValueCapacity());

      int bufSizeBefore = vector.getFieldBuffers().get(1).capacity();
      vector.setValueCount(initialCapacity);
      assertEquals(bufSizeBefore, vector.getFieldBuffers().get(1).capacity());
      assertEquals(initialCapacity, vector.getValueCapacity());
    }
  }

  @Test
  public void testSetSafeWithArrowBufNoExcessAllocs() {
    final int numValues = BaseFixedWidthVector.INITIAL_VALUE_ALLOCATION * 2;
    final byte[] valueBytes = "hello world".getBytes();
    final int valueBytesLength = valueBytes.length;
    final int isSet = 1;

    try (
        final VarCharVector fromVector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH,
            MinorType.VARCHAR, allocator);
        final VarCharVector toVector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH,
            MinorType.VARCHAR, allocator)) {
      /*
       * Populate the from vector with 'numValues' with byte-arrays, each of size 'valueBytesLength'.
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
        int start = fromVector.getStartOffset(i);
        int end = fromVector.getStartOffset(i + 1);
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
  public void testCopyFromWithNulls() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final VarCharVector vector2 =
             newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {

      vector.setInitialCapacity(4095);
      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertTrue(capacity >= 4095);

      for (int i = 0; i < capacity; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      assertEquals(capacity, vector.getValueCapacity());

      vector.setValueCount(capacity);

      for (int i = 0; i < capacity; i++) {
        if (i % 3 == 0) {
          assertNull(vector.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector.getObject(i).toString());
        }
      }

      vector2.setInitialCapacity(4095);
      vector2.allocateNew();
      int capacity2 = vector2.getValueCapacity();
      assertEquals(capacity2, capacity);

      for (int i = 0; i < capacity; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* NO reAlloc() should have happened in copyFrom */
      assertEquals(capacity, vector2.getValueCapacity());

      vector2.setValueCount(capacity);

      for (int i = 0; i < capacity; i++) {
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
         final VarCharVector vector2 =
             newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {

      vector.setInitialCapacity(4095);
      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertTrue(capacity >= 4095);

      for (int i = 0; i < capacity; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      assertEquals(capacity, vector.getValueCapacity());

      vector.setValueCount(capacity);

      for (int i = 0; i < capacity; i++) {
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

      int capacity2 = vector2.getValueCapacity();
      assertTrue(capacity2 >= 1024);
      assertTrue(capacity2 <= capacity);

      for (int i = 0; i < capacity; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* 2 reAllocs should have happened in copyFromSafe() */
      assertEquals(capacity, vector2.getValueCapacity());

      vector2.setValueCount(capacity);

      for (int i = 0; i < capacity; i++) {
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
      for (int i = 0; i < 16; i += 2) {
        vector.set(i, i + 10);
      }

      /* check the vector output */
      for (int i = 0; i < 16; i += 2) {
        assertEquals(i + 10, vector.get(i));
      }

      List<ArrowBuf> buffers = vector.getFieldBuffers();
      long bitAddress = vector.getValidityBufferAddress();
      long dataAddress = vector.getDataBufferAddress();

      try {
        long offsetAddress = vector.getOffsetBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
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
      int defaultCapacity = BaseValueVector.INITIAL_VALUE_ALLOCATION - 1;
      vector.setInitialCapacity(defaultCapacity);
      vector.allocateNew();
      assertEquals(defaultCapacity, vector.getValueCapacity());
      assertEquals(BaseAllocator.nextPowerOfTwo(defaultCapacity * 8), vector.getDataBuffer().capacity());

      vector.setInitialCapacity(defaultCapacity, 1);
      vector.allocateNew();
      assertEquals(defaultCapacity, vector.getValueCapacity());
      assertEquals(BaseAllocator.nextPowerOfTwo(defaultCapacity), vector.getDataBuffer().capacity());

      vector.setInitialCapacity(defaultCapacity, 0.1);
      vector.allocateNew();
      assertEquals(defaultCapacity, vector.getValueCapacity());
      assertEquals(BaseAllocator.nextPowerOfTwo((int)(defaultCapacity * 0.1)), vector.getDataBuffer().capacity());

      vector.setInitialCapacity(defaultCapacity, 0.01);
      vector.allocateNew();
      assertEquals(defaultCapacity, vector.getValueCapacity());
      assertEquals(BaseAllocator.nextPowerOfTwo((int)(defaultCapacity * 0.01)), vector.getDataBuffer().capacity());

      vector.setInitialCapacity(5, 0.01);
      vector.allocateNew();
      assertEquals(5, vector.getValueCapacity());
      assertEquals(2, vector.getDataBuffer().capacity());
    }
  }

  @Test
  public void testDefaultAllocNewAll() {
    int defaultCapacity = BaseFixedWidthVector.INITIAL_VALUE_ALLOCATION;
    int expectedSize;
    long beforeSize;
    try (BufferAllocator childAllocator = allocator.newChildAllocator("defaultAllocs", 0, Long.MAX_VALUE);
        final IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, childAllocator);
        final BigIntVector bigIntVector = new BigIntVector(EMPTY_SCHEMA_PATH, childAllocator);
        final BitVector bitVector = new BitVector(EMPTY_SCHEMA_PATH, childAllocator);
        final DecimalVector decimalVector = new DecimalVector(EMPTY_SCHEMA_PATH, childAllocator, 38, 6);
        final VarCharVector varCharVector = new VarCharVector(EMPTY_SCHEMA_PATH, childAllocator)) {

      // verify that the wastage is within bounds for IntVector.
      beforeSize = childAllocator.getAllocatedMemory();
      intVector.allocateNew();
      assertTrue(intVector.getValueCapacity() >= defaultCapacity);
      expectedSize = (defaultCapacity * IntVector.TYPE_WIDTH) +
          BaseFixedWidthVector.getValidityBufferSizeFromCount(defaultCapacity);
      assertTrue(childAllocator.getAllocatedMemory() - beforeSize <= expectedSize * 1.05);

      // verify that the wastage is within bounds for BigIntVector.
      beforeSize = childAllocator.getAllocatedMemory();
      bigIntVector.allocateNew();
      assertTrue(bigIntVector.getValueCapacity() >= defaultCapacity);
      expectedSize = (defaultCapacity * bigIntVector.TYPE_WIDTH) +
          BaseFixedWidthVector.getValidityBufferSizeFromCount(defaultCapacity);
      assertTrue(childAllocator.getAllocatedMemory() - beforeSize <= expectedSize * 1.05);

      // verify that the wastage is within bounds for DecimalVector.
      beforeSize = childAllocator.getAllocatedMemory();
      decimalVector.allocateNew();
      assertTrue(decimalVector.getValueCapacity() >= defaultCapacity);
      expectedSize = (defaultCapacity * decimalVector.TYPE_WIDTH) +
          BaseFixedWidthVector.getValidityBufferSizeFromCount(defaultCapacity);
      assertTrue(childAllocator.getAllocatedMemory() - beforeSize <= expectedSize * 1.05);

      // verify that the wastage is within bounds for VarCharVector.
      // var char vector have an offsets array that is 1 less than defaultCapacity
      beforeSize = childAllocator.getAllocatedMemory();
      varCharVector.allocateNew();
      assertTrue(varCharVector.getValueCapacity() >= defaultCapacity - 1);
      expectedSize = (defaultCapacity * VarCharVector.OFFSET_WIDTH) +
          BaseFixedWidthVector.getValidityBufferSizeFromCount(defaultCapacity) +
          defaultCapacity * 8;
      // wastage should be less than 5%.
      assertTrue(childAllocator.getAllocatedMemory() - beforeSize <= expectedSize * 1.05);

      // verify that the wastage is within bounds for BitVector.
      beforeSize = childAllocator.getAllocatedMemory();
      bitVector.allocateNew();
      assertTrue(bitVector.getValueCapacity() >= defaultCapacity);
      expectedSize = BaseFixedWidthVector.getValidityBufferSizeFromCount(defaultCapacity) * 2;
      assertTrue(childAllocator.getAllocatedMemory() - beforeSize <= expectedSize * 1.05);

    }
  }

  @Test
  public void testSetNullableVarCharHolder() {
    try (VarCharVector vector = new VarCharVector("", allocator)) {
      vector.allocateNew(100, 10);

      NullableVarCharHolder nullHolder = new NullableVarCharHolder();
      nullHolder.isSet = 0;

      NullableVarCharHolder stringHolder = new NullableVarCharHolder();
      stringHolder.isSet = 1;

      String str = "hello";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      stringHolder.start = 0;
      stringHolder.end = str.length();
      stringHolder.buffer = buf;

      vector.set(0, nullHolder);
      vector.set(1, stringHolder);

      // verify results
      assertTrue(vector.isNull(0));
      assertEquals(str, new String(vector.get(1)));

      buf.close();
    }
  }

  @Test
  public void testSetNullableVarCharHolderSafe() {
    try (VarCharVector vector = new VarCharVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableVarCharHolder nullHolder = new NullableVarCharHolder();
      nullHolder.isSet = 0;

      NullableVarCharHolder stringHolder = new NullableVarCharHolder();
      stringHolder.isSet = 1;

      String str = "hello world";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      stringHolder.start = 0;
      stringHolder.end = str.length();
      stringHolder.buffer = buf;

      vector.setSafe(0, stringHolder);
      vector.setSafe(1, nullHolder);

      // verify results
      assertEquals(str, new String(vector.get(0)));
      assertTrue(vector.isNull(1));

      buf.close();
    }
  }

  @Test
  public void testSetNullableVarBinaryHolder() {
    try (VarBinaryVector vector = new VarBinaryVector("", allocator)) {
      vector.allocateNew(100, 10);

      NullableVarBinaryHolder nullHolder = new NullableVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableVarBinaryHolder binHolder = new NullableVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      binHolder.start = 0;
      binHolder.end = str.length();
      binHolder.buffer = buf;

      vector.set(0, nullHolder);
      vector.set(1, binHolder);

      // verify results
      assertTrue(vector.isNull(0));
      assertEquals(str, new String(vector.get(1)));

      buf.close();
    }
  }

  @Test
  public void testSetNullableVarBinaryHolderSafe() {
    try (VarBinaryVector vector = new VarBinaryVector("", allocator)) {
      vector.allocateNew(5, 1);

      NullableVarBinaryHolder nullHolder = new NullableVarBinaryHolder();
      nullHolder.isSet = 0;

      NullableVarBinaryHolder binHolder = new NullableVarBinaryHolder();
      binHolder.isSet = 1;

      String str = "hello world";
      ArrowBuf buf = allocator.buffer(16);
      buf.setBytes(0, str.getBytes());

      binHolder.start = 0;
      binHolder.end = str.length();
      binHolder.buffer = buf;

      vector.setSafe(0, binHolder);
      vector.setSafe(1, nullHolder);

      // verify results
      assertEquals(str, new String(vector.get(0)));
      assertTrue(vector.isNull(1));

      buf.close();
    }
  }

  @Test
  public void testGetPointerFixedWidth() {
    final int vectorLength = 100;
    try (IntVector vec1 = new IntVector("vec1", allocator);
         IntVector vec2 = new IntVector("vec2", allocator)) {
      vec1.allocateNew(vectorLength);
      vec2.allocateNew(vectorLength);

      for (int i = 0; i < vectorLength; i++) {
        if (i % 10 == 0) {
          vec1.setNull(i);
          vec2.setNull(i);
        } else {
          vec1.set(i, i * 1234);
          vec2.set(i, i * 1234);
        }
      }

      ArrowBufPointer ptr1 = new ArrowBufPointer();
      ArrowBufPointer ptr2 = new ArrowBufPointer();

      for (int i = 0; i < vectorLength; i++) {
        vec1.getDataPointer(i, ptr1);
        vec2.getDataPointer(i, ptr2);

        if (i % 10 == 0) {
          assertNull(ptr1.getBuf());
          assertNull(ptr2.getBuf());
        }

        assertTrue(ptr1.equals(ptr2));
        assertTrue(ptr2.equals(ptr2));
      }
    }
  }

  @Test
  public void testGetPointerVariableWidth() {
    final String[] sampleData = new String[]{
      "abc", "123", "def", null, "hello", "aaaaa", "world", "2019", null, "0717"};

    try (VarCharVector vec1 = new VarCharVector("vec1", allocator);
         VarCharVector vec2 = new VarCharVector("vec2", allocator)) {
      vec1.allocateNew(sampleData.length * 10, sampleData.length);
      vec2.allocateNew(sampleData.length * 10, sampleData.length);

      for (int i = 0; i < sampleData.length; i++) {
        String str = sampleData[i];
        if (str != null) {
          vec1.set(i, sampleData[i].getBytes());
          vec2.set(i, sampleData[i].getBytes());
        } else {
          vec1.setNull(i);
          vec2.setNull(i);
        }
      }

      ArrowBufPointer ptr1 = new ArrowBufPointer();
      ArrowBufPointer ptr2 = new ArrowBufPointer();

      for (int i = 0; i < sampleData.length; i++) {
        vec1.getDataPointer(i, ptr1);
        vec2.getDataPointer(i, ptr2);

        assertTrue(ptr1.equals(ptr2));
        assertTrue(ptr2.equals(ptr2));
      }
    }
  }

  public void testGetNullFromVariableWidthVector() {
    try (VarCharVector varCharVector = new VarCharVector("varcharvec", allocator);
    VarBinaryVector varBinaryVector = new VarBinaryVector("varbinary", allocator)) {
      varCharVector.allocateNew(10, 1);
      varBinaryVector.allocateNew(10, 1);

      varCharVector.setNull(0);
      varBinaryVector.setNull(0);

      assertNull(varCharVector.get(0));
      assertNull(varBinaryVector.get(0));
    }
  }

  @Test
  public void testZeroVectorEquals() {
    try (final ZeroVector vector1 = new ZeroVector();
        final ZeroVector vector2 = new ZeroVector()) {

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testZeroVectorNotEquals() {
    try (final IntVector intVector = new IntVector("int", allocator);
        final ZeroVector zeroVector = new ZeroVector()) {

      VectorEqualsVisitor zeroVisitor = new VectorEqualsVisitor();
      assertFalse(zeroVisitor.vectorEquals(intVector, zeroVector));

      VectorEqualsVisitor intVisitor = new VectorEqualsVisitor();
      assertFalse(intVisitor.vectorEquals(zeroVector, intVector));
    }
  }

  @Test
  public void testIntVectorEqualsWithNull() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);

      vector2.setSafe(0, 1);
      VectorEqualsVisitor visitor = new VectorEqualsVisitor();

      assertFalse(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testIntVectorEquals() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(3);
      vector1.setValueCount(3);
      vector2.allocateNew(3);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);
      vector1.setSafe(2, 3);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();

      assertFalse(visitor.vectorEquals(vector1, vector2));

      vector2.setValueCount(3);
      vector2.setSafe(2, 2);
      assertFalse(vector1.equals(vector2));

      vector2.setSafe(2, 3);
      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testDecimalVectorEquals() {
    try (final DecimalVector vector1 = new DecimalVector("decimal", allocator, 3, 3);
        final DecimalVector vector2 = new DecimalVector("decimal", allocator, 3, 3);
        final DecimalVector vector3 = new DecimalVector("decimal", allocator, 3, 2)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);
      vector3.allocateNew(2);
      vector3.setValueCount(2);

      vector1.setSafe(0, 100);
      vector1.setSafe(1, 200);

      vector2.setSafe(0, 100);
      vector2.setSafe(1, 200);

      vector3.setSafe(0, 100);
      vector3.setSafe(1, 200);

      VectorEqualsVisitor visitor1 = new VectorEqualsVisitor();
      VectorEqualsVisitor visitor2 = new VectorEqualsVisitor();

      assertTrue(visitor1.vectorEquals(vector1, vector2));
      assertFalse(visitor2.vectorEquals(vector1, vector3));
    }
  }

  @Test
  public void testVarcharVectorEuqalsWithNull() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setValueCount(2);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testVarcharVectorEquals() {
    try (final VarCharVector vector1 = new VarCharVector("varchar", allocator);
        final VarCharVector vector2 = new VarCharVector("varchar", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setSafe(2, STR3, 0, STR3.length);
      vector1.setValueCount(3);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setSafe(1, STR2, 0, STR2.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));

      vector2.setSafe(2, STR3, 0, STR3.length);
      vector2.setValueCount(3);
      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testVarBinaryVectorEquals() {
    try (final VarBinaryVector vector1 = new VarBinaryVector("binary", allocator);
        final VarBinaryVector vector2 = new VarBinaryVector("binary", allocator)) {

      vector1.allocateNew();
      vector2.allocateNew();

      // set some values
      vector1.setSafe(0, STR1, 0, STR1.length);
      vector1.setSafe(1, STR2, 0, STR2.length);
      vector1.setSafe(2, STR3, 0, STR3.length);
      vector1.setValueCount(3);

      vector2.setSafe(0, STR1, 0, STR1.length);
      vector2.setSafe(1, STR2, 0, STR2.length);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));

      vector2.setSafe(2, STR3, 0, STR3.length);
      vector2.setValueCount(3);
      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testListVectorEqualsWithNull() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {1, 2});
      writeListVector(writer2, new int[] {3, 4});
      writer2.setValueCount(3);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();

      assertFalse(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testListVectorEquals() {
    try (final ListVector vector1 = ListVector.empty("list", allocator);
        final ListVector vector2 = ListVector.empty("list", allocator);) {

      UnionListWriter writer1 = vector1.getWriter();
      writer1.allocate();

      //set some values
      writeListVector(writer1, new int[] {1, 2});
      writeListVector(writer1, new int[] {3, 4});
      writeListVector(writer1, new int[] {5, 6});
      writer1.setValueCount(3);

      UnionListWriter writer2 = vector2.getWriter();
      writer2.allocate();

      //set some values
      writeListVector(writer2, new int[] {1, 2});
      writeListVector(writer2, new int[] {3, 4});
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));

      writeListVector(writer2, new int[] {5, 6});
      writer2.setValueCount(3);

      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testStructVectorEqualsWithNull() {

    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writeStructVector(writer1, 3, 30L);
      writer1.setValueCount(3);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 3, 30L);
      writer2.setValueCount(3);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testStructVectorEquals() {
    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writeStructVector(writer1, 3, 30L);
      writer1.setValueCount(3);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 2, 20L);
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));

      writeStructVector(writer2, 3, 30L);
      writer2.setValueCount(3);

      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testStructVectorEqualsWithDiffChild() {
    try (final StructVector vector1 = StructVector.empty("struct", allocator);
        final StructVector vector2 = StructVector.empty("struct", allocator);) {
      vector1.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector1.addOrGet("f1", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);
      vector2.addOrGet("f0", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      vector2.addOrGet("f10", FieldType.nullable(new ArrowType.Int(64, true)), BigIntVector.class);

      NullableStructWriter writer1 = vector1.getWriter();
      writer1.allocate();

      writeStructVector(writer1, 1, 10L);
      writeStructVector(writer1, 2, 20L);
      writer1.setValueCount(2);

      NullableStructWriter writer2 = vector2.getWriter();
      writer2.allocate();

      writeStructVector(writer2, 1, 10L);
      writeStructVector(writer2, 2, 20L);
      writer2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertFalse(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test
  public void testUnionVectorEquals() {
    try (final UnionVector vector1 = new UnionVector("union", allocator, null);
        final UnionVector vector2 = new UnionVector("union", allocator, null);) {

      final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
      uInt4Holder.value = 10;
      uInt4Holder.isSet = 1;

      final NullableIntHolder intHolder = new NullableIntHolder();
      uInt4Holder.value = 20;
      uInt4Holder.isSet = 1;

      vector1.setType(0, Types.MinorType.UINT4);
      vector1.setSafe(0, uInt4Holder);

      vector1.setType(1, Types.MinorType.INT);
      vector1.setSafe(1, intHolder);
      vector1.setValueCount(2);

      vector2.setType(0, Types.MinorType.UINT4);
      vector2.setSafe(0, uInt4Holder);

      vector2.setType(1, Types.MinorType.INT);
      vector2.setSafe(1, intHolder);
      vector2.setValueCount(2);

      VectorEqualsVisitor visitor = new VectorEqualsVisitor();
      assertTrue(visitor.vectorEquals(vector1, vector2));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEqualsWithIndexOutOfRange() {
    try (final IntVector vector1 = new IntVector("int", allocator);
        final IntVector vector2 = new IntVector("int", allocator)) {

      vector1.allocateNew(2);
      vector1.setValueCount(2);
      vector2.allocateNew(2);
      vector2.setValueCount(2);

      vector1.setSafe(0, 1);
      vector1.setSafe(1, 2);

      vector2.setSafe(0, 1);
      vector2.setSafe(1, 2);

      assertTrue(new RangeEqualsVisitor(vector1, vector2).rangeEquals(new Range(2, 3, 1)));
    }
  }

  private void writeStructVector(NullableStructWriter writer, int value1, long value2) {
    writer.start();
    writer.integer("f0").writeInt(value1);
    writer.bigInt("f1").writeBigInt(value2);
    writer.end();
  }

  private void writeListVector(UnionListWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }
}
