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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.*;


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

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testFixedType() {

    // Create a new value vector for 1024 integers.
    try (final UInt4Vector vector = new UInt4Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final UInt4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.setSafe(0, 100);
      m.setSafe(1, 101);
      m.setSafe(100, 102);
      m.setSafe(1022, 103);
      m.setSafe(1023, 104);

      final UInt4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));
    }
  }

  @Test
  public void testNullableVarLen2() {

    // Create a new value vector for 1024 integers.
    try (final NullableVarCharVector vector = new NullableVarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
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

  @Test
  public void testNullableFixedType() {

    // Create a new value vector for 1024 integers.
    try (final NullableUInt4Vector vector = new NullableUInt4Vector(EMPTY_SCHEMA_PATH, allocator)) {
      final NullableUInt4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.set(0, 100);
      m.set(1, 101);
      m.set(100, 102);
      m.set(1022, 103);
      m.set(1023, 104);

      final NullableUInt4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      // Ensure null values throw
      {
        boolean b = false;
        try {
          accessor.get(3);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      vector.allocateNew(2048);
      {
        boolean b = false;
        try {
          accessor.get(0);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      m.set(0, 100);
      m.set(1, 101);
      m.set(100, 102);
      m.set(1022, 103);
      m.set(1023, 104);
      assertEquals(100, accessor.get(0));
      assertEquals(101, accessor.get(1));
      assertEquals(102, accessor.get(100));
      assertEquals(103, accessor.get(1022));
      assertEquals(104, accessor.get(1023));

      // Ensure null values throw.
      {
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
  }

  @Test
  public void testNullableFloat() {
    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = (NullableFloat4Vector) MinorType.FLOAT4.getNewVector(EMPTY_SCHEMA_PATH, allocator, null)) {
      final NullableFloat4Vector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values.
      m.set(0, 100.1f);
      m.set(1, 101.2f);
      m.set(100, 102.3f);
      m.set(1022, 103.4f);
      m.set(1023, 104.5f);

      final NullableFloat4Vector.Accessor accessor = vector.getAccessor();
      assertEquals(100.1f, accessor.get(0), 0);
      assertEquals(101.2f, accessor.get(1), 0);
      assertEquals(102.3f, accessor.get(100), 0);
      assertEquals(103.4f, accessor.get(1022), 0);
      assertEquals(104.5f, accessor.get(1023), 0);

      // Ensure null values throw.
      {
        boolean b = false;
        try {
          vector.getAccessor().get(3);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }

      vector.allocateNew(2048);
      {
        boolean b = false;
        try {
          accessor.get(0);
        } catch (IllegalStateException e) {
          b = true;
        } finally {
          assertTrue(b);
        }
      }
    }
  }

  @Test
  public void testBitVector() {
    // Create a new value vector for 1024 integers
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      final BitVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values
      m.set(0, 1);
      m.set(1, 0);
      m.set(100, 0);
      m.set(1022, 1);

      final BitVector.Accessor accessor = vector.getAccessor();
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));
      assertEquals(0, accessor.get(100));
      assertEquals(1, accessor.get(1022));

      // test setting the same value twice
      m.set(0, 1);
      m.set(0, 1);
      m.set(1, 0);
      m.set(1, 0);
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));

      // test toggling the values
      m.set(0, 0);
      m.set(1, 1);
      assertEquals(0, accessor.get(0));
      assertEquals(1, accessor.get(1));

      // Ensure unallocated space returns 0
      assertEquals(0, accessor.get(3));
    }
  }

  @Test
  public void testReAllocNullableFixedWidthVector() {
    // Create a new value vector for 1024 integers
    try (final NullableFloat4Vector vector = (NullableFloat4Vector) MinorType.FLOAT4.getNewVector(EMPTY_SCHEMA_PATH, allocator, null)) {
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
    try (final NullableVarCharVector vector = (NullableVarCharVector) MinorType.VARCHAR.getNewVector(EMPTY_SCHEMA_PATH, allocator, null)) {
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

}
