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

import static org.apache.arrow.vector.TestUtils.newNullableVarCharVector;
import static org.apache.arrow.vector.TestUtils.newVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

  @Test
  public void testNullableFixedType() {

    // Create a new value vector for 1024 integers.
    try (final NullableUInt4Vector vector = newVector(NullableUInt4Vector.class, EMPTY_SCHEMA_PATH, new ArrowType.Int(32, false), allocator);) {
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
    try (final NullableFloat4Vector vector = newVector(NullableFloat4Vector.class, EMPTY_SCHEMA_PATH, MinorType.FLOAT4, allocator);) {
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
  public void testNullableInt() {
    // Create a new value vector for 1024 integers
    try (final NullableIntVector vector = newVector(NullableIntVector.class, EMPTY_SCHEMA_PATH, MinorType.INT, allocator)) {
      final NullableIntVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);

      // Put and set a few values.
      m.set(0, 1);
      m.set(1, 2);
      m.set(100, 3);
      m.set(1022, 4);
      m.set(1023, 5);

      m.setValueCount(1024);

      final NullableIntVector.Accessor accessor = vector.getAccessor();
      assertEquals(1, accessor.get(0));
      assertEquals(2, accessor.get(1));
      assertEquals(3, accessor.get(100));
      assertEquals(4, accessor.get(1022));
      assertEquals(5, accessor.get(1023));

      // Ensure null values.
      assertTrue(vector.getAccessor().isNull(3));

      Field field = vector.getField();
      TypeLayout typeLayout = field.getTypeLayout();

      List<ArrowBuf> buffers = vector.getFieldBuffers();

      assertEquals(2, typeLayout.getVectors().size());
      assertEquals(2, buffers.size());

      ArrowBuf validityVectorBuf = buffers.get(0);
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

      vector.allocateNew(2048);
      // vector has been erased
      assertTrue(vector.getAccessor().isNull(0));
    }
  }


  @Test
  public void testBitVector() {
    // Create a new value vector for 1024 integers
    try (final BitVector vector = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {
      final BitVector.Mutator m = vector.getMutator();
      vector.allocateNew(1024);
      m.setValueCount(1024);

      // Put and set a few values
      m.set(0, 1);
      m.set(1, 0);
      m.set(100, 0);
      m.set(1022, 1);

      m.setValueCount(1024);

      final BitVector.Accessor accessor = vector.getAccessor();
      assertEquals(1, accessor.get(0));
      assertEquals(0, accessor.get(1));
      assertEquals(0, accessor.get(100));
      assertEquals(1, accessor.get(1022));

      assertEquals(1022, accessor.getNullCount());

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

      // should not change
      assertEquals(1022, accessor.getNullCount());

      // Ensure unallocated space returns 0
      assertEquals(0, accessor.get(3));

      // unset the previously set bits
      m.set(1, 0);
      m.set(1022, 0);
      // this should set all the array to 0
      assertEquals(1024, accessor.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1024; ++i) {
        assertEquals(1024 - i, accessor.getNullCount());
        m.set(i, 1);
      }

      assertEquals(0, accessor.getNullCount());

      vector.allocateNew(1015);
      m.setValueCount(1015);

      // ensure it has been zeroed
      assertEquals(1015, accessor.getNullCount());

      m.set(0, 1);
      m.set(1014, 1); // ensure that the last item of the last byte is allocated

      assertEquals(1013, accessor.getNullCount());

      vector.zeroVector();
      assertEquals(1015, accessor.getNullCount());

      // set all the array to 1
      for (int i = 0; i < 1015; ++i) {
        assertEquals(1015 - i, accessor.getNullCount());
        m.set(i, 1);
      }

      assertEquals(0, accessor.getNullCount());
    }
  }

  @Test
  public void testBitVectorRangeSetAllOnes() {
    validateRange(1000, 0, 1000);
    validateRange(1000, 0, 1);
    validateRange(1000, 1, 2);
    validateRange(1000, 5, 6);
    validateRange(1000, 5, 10);
    validateRange(1000, 5, 150);
    validateRange(1000, 5, 27);
    for (int i = 0; i < 8; i++) {
      for (int j = 0; j < 8; j++) {
        validateRange(1000, 10 + i, 27 + j);
        validateRange(1000, i, j);
      }
    }
  }

  private void validateRange(int length, int start, int count) {
    String desc = "[" + start + ", " + (start + count) + ") ";
    try (BitVector bitVector = new BitVector("bits", allocator)) {
      bitVector.reset();
      bitVector.allocateNew(length);
      bitVector.getMutator().setRangeToOne(start, count);
      for (int i = 0; i < start; i++) {
        Assert.assertEquals(desc + i, 0, bitVector.getAccessor().get(i));
      }
      for (int i = start; i < start + count; i++) {
        Assert.assertEquals(desc + i, 1, bitVector.getAccessor().get(i));
      }
      for (int i = start + count; i < length; i++) {
        Assert.assertEquals(desc + i, 0, bitVector.getAccessor().get(i));
      }
    }
  }

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
