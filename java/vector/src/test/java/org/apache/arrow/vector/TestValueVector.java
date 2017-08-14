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

import static org.apache.arrow.vector.TestUtils.newNullableVarCharVector;
import static org.apache.arrow.vector.TestUtils.newVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.nio.charset.Charset;
import java.util.List;
import java.util.ArrayList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.TypeLayout;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Assert;
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
   *  -- NullableUInt4Vector
   *  -- NullableIntVector
   *  -- NullableFloat4Vector
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
      final UInt4Vector.Mutator mutator = vector.getMutator();
      final UInt4Vector.Accessor accessor = vector.getAccessor();

      vector.allocateNew(1024);
      initialCapacity = vector.getValueCapacity();
      assertEquals(1024, initialCapacity);

      // Put and set a few values
      mutator.setSafe(0, 100);
      mutator.setSafe(1, 101);
      mutator.setSafe(100, 102);
      mutator.setSafe(1022, 103);
      mutator.setSafe(1023, 104);

      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      try {
        mutator.set(1024, 10000);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      try {
        accessor.get(1024);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      mutator.setSafe(1024, 10000);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* check vector data after realloc */
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));
      assertEquals(10000, accessor.get(1024));

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, 0, accessor.get(i));
      }
    }
  }

  @Test /* IntVector */
  public void testFixedType2() {
    try (final IntVector intVector = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {
      final IntVector.Mutator mutator = intVector.getMutator();
      final IntVector.Accessor accessor = intVector.getAccessor();
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
        mutator.set(i, j);
        j++;
      }

      try {
        mutator.set(16, 9);
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
        assertEquals("unexpected value at index: " + i, j, accessor.get(i));
        j++;
      }

      try {
        accessor.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      mutator.setSafe(16, 9);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, intVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      j = 1;
      for(int i = 0; i <= 16; i += 2) {
        assertEquals("unexpected value at index: " + i, j, accessor.get(i));
        j++;
      }

      /* reset the vector */
      intVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, intVector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, 0, accessor.get(i));
      }
    }
  }

  @Test /* Float4Vector */
  public void testFixedType3() {
    try (final Float4Vector floatVector = new Float4Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final Float4Vector.Mutator mutator = floatVector.getMutator();
      final Float4Vector.Accessor accessor = floatVector.getAccessor();
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

      /* populate the vector */
      mutator.set(0, 1.5f);
      mutator.set(2, 2.5f);
      mutator.set(4, 3.3f);
      mutator.set(6, 4.8f);
      mutator.set(8, 5.6f);
      mutator.set(10, 6.6f);
      mutator.set(12, 7.8f);
      mutator.set(14, 8.5f);

      try {
        mutator.set(16, 9.5f);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      assertEquals(1.5f, accessor.get(0), 0);
      assertEquals(2.5f, accessor.get(2), 0);
      assertEquals(3.3f, accessor.get(4), 0);
      assertEquals(4.8f, accessor.get(6), 0);
      assertEquals(5.6f, accessor.get(8), 0);
      assertEquals(6.6f, accessor.get(10), 0);
      assertEquals(7.8f, accessor.get(12), 0);
      assertEquals(8.5f, accessor.get(14), 0);

      try {
        accessor.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      mutator.setSafe(16, 9.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(1.5f, accessor.get(0), 0);
      assertEquals(2.5f, accessor.get(2), 0);
      assertEquals(3.3f, accessor.get(4), 0);
      assertEquals(4.8f, accessor.get(6), 0);
      assertEquals(5.6f, accessor.get(8), 0);
      assertEquals(6.6f, accessor.get(10), 0);
      assertEquals(7.8f, accessor.get(12), 0);
      assertEquals(8.5f, accessor.get(14), 0);
      assertEquals(9.5f, accessor.get(16), 0);

      /* reset the vector */
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, 0, accessor.get(i), 0);
      }
    }
  }

  @Test /* Float8Vector */
  public void testFixedType4() {
    try (final Float8Vector floatVector = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final Float8Vector.Mutator mutator = floatVector.getMutator();
      final Float8Vector.Accessor accessor = floatVector.getAccessor();
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
      mutator.set(0, 1.55);
      mutator.set(2, 2.53);
      mutator.set(4, 3.36);
      mutator.set(6, 4.82);
      mutator.set(8, 5.67);
      mutator.set(10, 6.67);
      mutator.set(12, 7.87);
      mutator.set(14, 8.56);

      try {
        mutator.set(16, 9.53);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      assertEquals(1.55, accessor.get(0), 0);
      assertEquals(2.53, accessor.get(2), 0);
      assertEquals(3.36, accessor.get(4), 0);
      assertEquals(4.82, accessor.get(6), 0);
      assertEquals(5.67, accessor.get(8), 0);
      assertEquals(6.67, accessor.get(10), 0);
      assertEquals(7.87, accessor.get(12), 0);
      assertEquals(8.56, accessor.get(14), 0);

      try {
        accessor.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      mutator.setSafe(16, 9.53);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(1.55, accessor.get(0), 0);
      assertEquals(2.53, accessor.get(2), 0);
      assertEquals(3.36, accessor.get(4), 0);
      assertEquals(4.82, accessor.get(6), 0);
      assertEquals(5.67, accessor.get(8), 0);
      assertEquals(6.67, accessor.get(10), 0);
      assertEquals(7.87, accessor.get(12), 0);
      assertEquals(8.56, accessor.get(14), 0);
      assertEquals(9.53, accessor.get(16), 0);

      /* reset the vector */
      floatVector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, floatVector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertEquals("non-zero data not expected at index: " + i, 0, accessor.get(i), 0);
      }
    }
  }

  @Test /* NullableUInt4Vector */
  public void testNullableFixedType1() {

    // Create a new value vector for 1024 integers.
    try (final NullableUInt4Vector vector = newVector(NullableUInt4Vector.class, EMPTY_SCHEMA_PATH, new ArrowType.Int(32, false), allocator);) {
      final NullableUInt4Vector.Mutator mutator = vector.getMutator();
      final NullableUInt4Vector.Accessor accessor = vector.getAccessor();
      boolean error = false;
      int initialCapacity = 1024;

      vector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      assertEquals(initialCapacity, vector.getValueCapacity());

      // Put and set a few values
      mutator.set(0, 100);
      mutator.set(1, 101);
      mutator.set(100, 102);
      mutator.set(1022, 103);
      mutator.set(1023, 104);

      /* check vector contents */
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      int val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i <= 99 || j <= 1021; i++, j++) {
        if (i <= 99) {
          assertTrue(accessor.isNull(i));
        }
        if(j <= 1021) {
          assertTrue(accessor.isNull(j));
        }
      }

      try {
        mutator.set(1024, 10000);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      try {
        accessor.get(1024);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* should trigger a realloc of the underlying bitvector and valuevector */
      mutator.setSafe(1024, 10000);

      /* check new capacity */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector contents should still be intact after realloc */
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));
      assertEquals(10000, accessor.get(1024));

      val = 0;

      /* check unset bits/null values */
      for (int i = 2, j = 101; i < 99 || j < 1021; i++, j++) {
        if (i <= 99) {
          assertTrue(accessor.isNull(i));
        }
        if(j <= 1021) {
          assertTrue(accessor.isNull(j));
        }
      }

      /* reset the vector */
      vector.reset();

       /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
      }
    }
  }

  @Test /* NullableFloat4Vector */
  public void testNullableFixedType2() {
    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = newVector(NullableFloat4Vector.class, EMPTY_SCHEMA_PATH, MinorType.FLOAT4, allocator);) {
      final NullableFloat4Vector.Mutator mutator = vector.getMutator();
      final NullableFloat4Vector.Accessor accessor = vector.getAccessor();
      boolean error = false;
      int initialCapacity = 16;

      vector.setInitialCapacity(initialCapacity);
      /* no memory allocation has happened yet */
      assertEquals(0, vector.getValueCapacity());

      vector.allocateNew();
      assertEquals(initialCapacity, vector.getValueCapacity());

      /* populate the vector */
      mutator.set(0, 100.5f);
      mutator.set(2, 201.5f);
      mutator.set(4, 300.3f);
      mutator.set(6, 423.8f);
      mutator.set(8, 555.6f);
      mutator.set(10, 66.6f);
      mutator.set(12, 78.8f);
      mutator.set(14, 89.5f);

      try {
        mutator.set(16, 90.5f);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* check vector contents */
      assertEquals(100.5f, accessor.get(0), 0);
      assertTrue(accessor.isNull(1));
      assertEquals(201.5f, accessor.get(2), 0);
      assertTrue(accessor.isNull(3));
      assertEquals(300.3f, accessor.get(4), 0);
      assertTrue(accessor.isNull(5));
      assertEquals(423.8f, accessor.get(6), 0);
      assertTrue(accessor.isNull(7));
      assertEquals(555.6f, accessor.get(8), 0);
      assertTrue(accessor.isNull(9));
      assertEquals(66.6f, accessor.get(10), 0);
      assertTrue(accessor.isNull(11));
      assertEquals(78.8f, accessor.get(12), 0);
      assertTrue(accessor.isNull(13));
      assertEquals(89.5f, accessor.get(14), 0);
      assertTrue(accessor.isNull(15));

      try {
        accessor.get(16);
      }
      catch (IndexOutOfBoundsException ie) {
        error = true;
      }
      finally {
        assertTrue(error);
        error = false;
      }

      /* this should trigger a realloc() */
      mutator.setSafe(16, 90.5f);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should still be intact after realloc */
      assertEquals(100.5f, accessor.get(0), 0);
      assertTrue(accessor.isNull(1));
      assertEquals(201.5f, accessor.get(2), 0);
      assertTrue(accessor.isNull(3));
      assertEquals(300.3f, accessor.get(4), 0);
      assertTrue(accessor.isNull(5));
      assertEquals(423.8f, accessor.get(6), 0);
      assertTrue(accessor.isNull(7));
      assertEquals(555.6f, accessor.get(8), 0);
      assertTrue(accessor.isNull(9));
      assertEquals(66.6f, accessor.get(10), 0);
      assertTrue(accessor.isNull(11));
      assertEquals(78.8f, accessor.get(12), 0);
      assertTrue(accessor.isNull(13));
      assertEquals(89.5f, accessor.get(14), 0);
      assertTrue(accessor.isNull(15));
      assertEquals(90.5f, accessor.get(16), 0);

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should be zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
      }
    }
  }

  @Test /* NullableIntVector */
  public void testNullableFixedType3() {
    // Create a new value vector for 1024 integers
    try (final NullableIntVector vector = newVector(NullableIntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, allocator)) {
      final NullableIntVector.Mutator mutator = vector.getMutator();
      final NullableIntVector.Accessor accessor = vector.getAccessor();
      boolean error = false;
      int initialCapacity = 1024;

      /* no memory allocation has happened yet so capacity of underlying buffer should be 0 */
      assertEquals(0, vector.getValueCapacity());
      /* allocate space for 4KB data (1024 * 4) */
      vector.allocateNew(initialCapacity);
      /* underlying buffer should be able to store 16 values */
      assertEquals(initialCapacity, vector.getValueCapacity());

      mutator.set(0, 1);
      mutator.set(1, 2);
      mutator.set(100, 3);
      mutator.set(1022, 4);
      mutator.set(1023, 5);

      /* check vector contents */
      int j = 1;
      for(int i = 0; i <= 1023; i++) {
        if((i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
        }
        else {
          assertFalse("null data not expected at index: " + i, accessor.isNull(i));
          assertEquals("unexpected value at index: " + i, j, accessor.get(i));
          j++;
        }
      }

      mutator.setValueCount(1024);
      Field field = vector.getField();
      TypeLayout typeLayout = field.getTypeLayout();

      List<ArrowBuf> buffers = vector.getFieldBuffers();

      assertEquals(2, typeLayout.getVectors().size());
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
      mutator.setSafe(1024, 6);

      /* underlying buffer should now be able to store double the number of values */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should still be intact after realloc */
      j = 1;
      for(int i = 0; i < (initialCapacity * 2); i++) {
        if((i > 1024) || (i >= 2 && i <= 99) || (i >= 101 && i <= 1021)) {
          assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
        }
        else {
          assertFalse("null data not expected at index: " + i, accessor.isNull(i));
          assertEquals("unexpected value at index: " + i, j, accessor.get(i));
          j++;
        }
      }

      /* reset the vector */
      vector.reset();

      /* capacity shouldn't change after reset */
      assertEquals(initialCapacity * 2, vector.getValueCapacity());

      /* vector data should have been zeroed out */
      for(int i = 0; i < (initialCapacity * 2); i++) {
        assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
      }

      vector.allocateNew(4096);
      // vector has been erased
      for(int i = 0; i < 4096; i++) {
        assertTrue("non-null data not expected at index: " + i, accessor.isNull(i));
      }
    }
  }

  /*
   * Tests for Variable Width Vectors
   *
   * Covered types as of now
   *
   *  -- NullableVarCharVector
   *
   * TODO:
   *
   *  -- VarCharVector
   *  -- VarBinaryVector
   *  -- NullableVarBinaryVector
   */

  @Test /* NullableVarCharVector */
  public void testNullableVarType1() {

    // Create a new value vector for 1024 integers.
    try (final NullableVarCharVector vector = newNullableVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024 * 10, 1024);

      m.set(0, STR1);
      m.set(1, STR2);
      m.set(2, STR3);

      // Check the sample strings.
      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));

      // Ensure null value throws.
      boolean b = false;
      try {
        vector.getAccessor().get(3);
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
   *  realloc related tests (edge cases) for more vector types.
   */

  @Test
  public void testReAllocNullableFixedWidthVector() {
    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = newVector(NullableFloat4Vector.class, EMPTY_SCHEMA_PATH, MinorType.FLOAT4, allocator)) {
      final NullableFloat4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      assertEquals(1024, vector.getValueCapacity());

      // Put values in indexes that fall within the initial allocation
      m.setSafe(0, 100.1f);
      m.setSafe(100, 102.3f);
      m.setSafe(1023, 104.5f);

      // Now try to put values in space that falls beyond the initial allocation
      m.setSafe(2000, 105.5f);

      // Check valueCapacity is more than initial allocation
      assertEquals(1024 * 2, vector.getValueCapacity());

      final NullableFloat4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100.1f, accessor.get(0), 0);
      assertEquals(102.3f, accessor.get(100), 0);
      assertEquals(104.5f, accessor.get(1023), 0);
      assertEquals(105.5f, accessor.get(2000), 0);

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when all values are inserted into the
      // vector
      m.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testReAllocNullableVariableWidthVector() {
    // Create a new value vector for 1024 integers
    try (final NullableVarCharVector vector = newVector(NullableVarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      final NullableVarCharVector.Mutator m = vector.getMutator();
      vector.allocateNew();

      int initialCapacity = vector.getValueCapacity();

      // Put values in indexes that fall within the initial allocation
      m.setSafe(0, STR1, 0, STR1.length);
      m.setSafe(initialCapacity - 1, STR2, 0, STR2.length);

      // Now try to put values in space that falls beyond the initial allocation
      m.setSafe(initialCapacity + 200, STR3, 0, STR3.length);

      // Check valueCapacity is more than initial allocation
      assertEquals((initialCapacity + 1) * 2 - 1, vector.getValueCapacity());

      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(initialCapacity - 1));
      assertArrayEquals(STR3, accessor.get(initialCapacity + 200));

      // Set the valueCount to be more than valueCapacity of current allocation. This is possible for NullableValueVectors
      // as we don't call setSafe for null values, but we do call setValueCount when the current batch is processed.
      m.setValueCount(vector.getValueCapacity() + 200);
    }
  }

  @Test
  public void testFillEmptiesNotOverfill() {
    try (final NullableVarCharVector vector = newVector(NullableVarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();

      vector.getMutator().setSafe(4094, "hello".getBytes(), 0, 5);
      vector.getMutator().setValueCount(4095);

      assertEquals(4096 * 4, vector.getFieldBuffers().get(1).capacity());
    }
  }

  @Test
  public void testCopyFromWithNulls() {
    try (final NullableVarCharVector vector = newVector(NullableVarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final NullableVarCharVector vector2 = newVector(NullableVarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.getMutator().setSafe(i, b, 0, b.length);
      }

      vector.getMutator().setValueCount(4095);

      vector2.allocateNew();

      for (int i = 0; i < 4095; i++) {
        vector2.copyFromSafe(i, i, vector);
      }

      vector2.getMutator().setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector2.getAccessor().getObject(i));
        } else {
          assertEquals(Integer.toString(i), vector2.getAccessor().getObject(i).toString());
        }
      }
    }
  }

  @Test
  public void testSetLastSetUsage() {
    try (final NullableVarCharVector vector = new NullableVarCharVector("myvector", allocator)) {

      final NullableVarCharVector.Mutator mutator = vector.getMutator();

      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(Integer.toString(-1), Integer.toString(mutator.getLastSet()));

      /* Check the vector output */
      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));
      assertArrayEquals(STR4, accessor.get(3));
      assertArrayEquals(STR5, accessor.get(4));
      assertArrayEquals(STR6, accessor.get(5));

      /*
       * If we don't do setLastSe(5) before setValueCount(), then the latter will corrupt
       * the value vector by filling in all positions [0,valuecount-1] will empty byte arrays.
       * Run the test by commenting out next line and we should see incorrect vector output.
       */
      mutator.setLastSet(5);
      mutator.setValueCount(20);

      /* Check the vector output again */
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));
      assertArrayEquals(STR4, accessor.get(3));
      assertArrayEquals(STR5, accessor.get(4));
      assertArrayEquals(STR6, accessor.get(5));
    }
  }

  @Test
  public void testVectorLoadUnload() {

    try (final NullableVarCharVector vector1 = new NullableVarCharVector("myvector", allocator)) {

      final NullableVarCharVector.Mutator mutator1 = vector1.getMutator();

      vector1.allocateNew(1024 * 10, 1024);

      mutator1.set(0, STR1);
      mutator1.set(1, STR2);
      mutator1.set(2, STR3);
      mutator1.set(3, STR4);
      mutator1.set(4, STR5);
      mutator1.set(5, STR6);
      assertEquals(Integer.toString(5), Integer.toString(mutator1.getLastSet()));
      mutator1.setValueCount(15);
      assertEquals(Integer.toString(14), Integer.toString(mutator1.getLastSet()));

      /* Check the vector output */
      final NullableVarCharVector.Accessor accessor1 = vector1.getAccessor();
      assertArrayEquals(STR1, accessor1.get(0));
      assertArrayEquals(STR2, accessor1.get(1));
      assertArrayEquals(STR3, accessor1.get(2));
      assertArrayEquals(STR4, accessor1.get(3));
      assertArrayEquals(STR5, accessor1.get(4));
      assertArrayEquals(STR6, accessor1.get(5));

      Field field = vector1.getField();
      String fieldName = field.getName();

      List<Field> fields = new ArrayList<Field>();
      List<FieldVector> fieldVectors = new ArrayList<FieldVector>();

      fields.add(field);
      fieldVectors.add(vector1);

      Schema schema = new Schema(fields);

      VectorSchemaRoot schemaRoot1 = new VectorSchemaRoot(schema, fieldVectors, accessor1.getValueCount());
      VectorUnloader vectorUnloader = new VectorUnloader(schemaRoot1);

      try (
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          BufferAllocator finalVectorsAllocator = allocator.newChildAllocator("new vector", 0, Long.MAX_VALUE);
          VectorSchemaRoot schemaRoot2 = VectorSchemaRoot.create(schema, finalVectorsAllocator);
      ) {

        VectorLoader vectorLoader = new VectorLoader(schemaRoot2);
        vectorLoader.load(recordBatch);

        NullableVarCharVector vector2 = (NullableVarCharVector) schemaRoot2.getVector(fieldName);
        NullableVarCharVector.Mutator mutator2 = vector2.getMutator();

        /*
         * lastSet would have internally been set by VectorLoader.load() when it invokes
         * loadFieldBuffers.
         */
        assertEquals(Integer.toString(14), Integer.toString(mutator2.getLastSet()));
        mutator2.setValueCount(25);
        assertEquals(Integer.toString(24), Integer.toString(mutator2.getLastSet()));

        /* Check the vector output */
        final NullableVarCharVector.Accessor accessor2 = vector2.getAccessor();
        assertArrayEquals(STR1, accessor2.get(0));
        assertArrayEquals(STR2, accessor2.get(1));
        assertArrayEquals(STR3, accessor2.get(2));
        assertArrayEquals(STR4, accessor2.get(3));
        assertArrayEquals(STR5, accessor2.get(4));
        assertArrayEquals(STR6, accessor2.get(5));
      }
    }
  }

  @Test
  public void testFillEmptiesUsage() {
    try (final NullableVarCharVector vector = new NullableVarCharVector("myvector", allocator)) {

      final NullableVarCharVector.Mutator mutator = vector.getMutator();

      vector.allocateNew(1024 * 10, 1024);

      setBytes(0, STR1, vector);
      setBytes(1, STR2, vector);
      setBytes(2, STR3, vector);
      setBytes(3, STR4, vector);
      setBytes(4, STR5, vector);
      setBytes(5, STR6, vector);

      /* Check current lastSet */
      assertEquals(Integer.toString(-1), Integer.toString(mutator.getLastSet()));

      /* Check the vector output */
      final NullableVarCharVector.Accessor accessor = vector.getAccessor();
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));
      assertArrayEquals(STR4, accessor.get(3));
      assertArrayEquals(STR5, accessor.get(4));
      assertArrayEquals(STR6, accessor.get(5));

      mutator.setLastSet(5);
      /* fill empty byte arrays from index [6, 9] */
      mutator.fillEmpties(10);

      /* Check current lastSet */
      assertEquals(Integer.toString(9), Integer.toString(mutator.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));
      assertArrayEquals(STR4, accessor.get(3));
      assertArrayEquals(STR5, accessor.get(4));
      assertArrayEquals(STR6, accessor.get(5));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(6)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(7)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(8)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(9)));

      setBytes(10, STR1, vector);
      setBytes(11, STR2, vector);

      mutator.setLastSet(11);
      /* fill empty byte arrays from index [12, 14] */
      mutator.setValueCount(15);

      /* Check current lastSet */
      assertEquals(Integer.toString(14), Integer.toString(mutator.getLastSet()));

      /* Check the vector output */
      assertArrayEquals(STR1, accessor.get(0));
      assertArrayEquals(STR2, accessor.get(1));
      assertArrayEquals(STR3, accessor.get(2));
      assertArrayEquals(STR4, accessor.get(3));
      assertArrayEquals(STR5, accessor.get(4));
      assertArrayEquals(STR6, accessor.get(5));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(6)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(7)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(8)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(9)));
      assertArrayEquals(STR1, accessor.get(10));
      assertArrayEquals(STR2, accessor.get(11));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(12)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(13)));
      assertEquals(Integer.toString(0), Integer.toString(accessor.getValueLength(14)));

      /* Check offsets */
      final UInt4Vector.Accessor offsetAccessor = vector.values.offsetVector.getAccessor();
      assertEquals(Integer.toString(0), Integer.toString(offsetAccessor.get(0)));
      assertEquals(Integer.toString(6), Integer.toString(offsetAccessor.get(1)));
      assertEquals(Integer.toString(16), Integer.toString(offsetAccessor.get(2)));
      assertEquals(Integer.toString(21), Integer.toString(offsetAccessor.get(3)));
      assertEquals(Integer.toString(30), Integer.toString(offsetAccessor.get(4)));
      assertEquals(Integer.toString(34), Integer.toString(offsetAccessor.get(5)));

      assertEquals(Integer.toString(40), Integer.toString(offsetAccessor.get(6)));
      assertEquals(Integer.toString(40), Integer.toString(offsetAccessor.get(7)));
      assertEquals(Integer.toString(40), Integer.toString(offsetAccessor.get(8)));
      assertEquals(Integer.toString(40), Integer.toString(offsetAccessor.get(9)));
      assertEquals(Integer.toString(40), Integer.toString(offsetAccessor.get(10)));

      assertEquals(Integer.toString(46), Integer.toString(offsetAccessor.get(11)));
      assertEquals(Integer.toString(56), Integer.toString(offsetAccessor.get(12)));

      assertEquals(Integer.toString(56), Integer.toString(offsetAccessor.get(13)));
      assertEquals(Integer.toString(56), Integer.toString(offsetAccessor.get(14)));
      assertEquals(Integer.toString(56), Integer.toString(offsetAccessor.get(15)));
    }
  }

  @Test
  public void testMultipleClose() {
    BufferAllocator vectorAllocator = allocator.newChildAllocator("vector_allocator", 0, Long.MAX_VALUE);
    NullableIntVector vector = newVector(NullableIntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, vectorAllocator);
    vector.close();
    vectorAllocator.close();
    vector.close();
    vectorAllocator.close();
  }

  public static void setBytes(int index, byte[] bytes, NullableVarCharVector vector) {
    final int currentOffset = vector.values.offsetVector.getAccessor().get(index);

    vector.bits.getMutator().setToOne(index);
    vector.values.offsetVector.getMutator().set(index + 1, currentOffset + bytes.length);
    vector.values.data.setBytes(currentOffset, bytes, 0, bytes.length);
  }
}
