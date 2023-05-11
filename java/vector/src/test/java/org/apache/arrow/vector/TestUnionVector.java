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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.VectorWithOrdinal;
import org.apache.arrow.vector.complex.impl.UnionWriter;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestUnionVector {
  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testUnionVector() throws Exception {

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (UnionVector unionVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      unionVector.allocateNew();

      // write some data
      unionVector.setType(0, MinorType.UINT4);
      unionVector.setSafe(0, uInt4Holder);
      unionVector.setType(2, MinorType.UINT4);
      unionVector.setSafe(2, uInt4Holder);
      unionVector.setValueCount(4);

      // check that what we wrote is correct
      assertEquals(4, unionVector.getValueCount());

      assertEquals(false, unionVector.isNull(0));
      assertEquals(100, unionVector.getObject(0));

      assertNull(unionVector.getObject(1));

      assertEquals(false, unionVector.isNull(2));
      assertEquals(100, unionVector.getObject(2));

      assertNull(unionVector.getObject(3));
    }
  }

  @Test
  public void testUnionVectorMapValue() throws Exception {
    try (UnionVector unionVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      unionVector.allocateNew();

      UnionWriter writer = (UnionWriter) unionVector.getWriter();

      // populate map vector with the following two records
      // [
      //    null,
      //    [[1: 2], [3: 4], [5: null]]
      // ]

      writer.setPosition(0);
      writer.writeNull();

      writer.setPosition(1);
      writer.startMap();

      writer.startEntry();
      writer.key().integer().writeInt(1);
      writer.value().integer().writeInt(2);
      writer.endEntry();

      writer.startEntry();
      writer.key().integer().writeInt(3);
      writer.value().integer().writeInt(4);
      writer.endEntry();

      writer.startEntry();
      writer.key().integer().writeInt(5);
      writer.endEntry();

      writer.endMap();

      unionVector.setValueCount(2);

      // check that what we wrote is correct
      assertEquals(2, unionVector.getValueCount());

      // first entry
      assertNull(unionVector.getObject(0));

      // second entry
      List<Map<String, Integer>> resultList = (List<Map<String, Integer>>) unionVector.getObject(1);
      assertEquals(3, resultList.size());

      Map<String, Integer> resultMap = resultList.get(0);
      assertEquals(1, (int) resultMap.get(MapVector.KEY_NAME));
      assertEquals(2, (int) resultMap.get(MapVector.VALUE_NAME));

      resultMap = resultList.get(1);
      assertEquals(3, (int) resultMap.get(MapVector.KEY_NAME));
      assertEquals(4, (int) resultMap.get(MapVector.VALUE_NAME));

      resultMap = resultList.get(2);
      assertEquals(5, (int) resultMap.get(MapVector.KEY_NAME));
      assertNull(resultMap.get(MapVector.VALUE_NAME));
    }
  }

  @Test
  public void testTransfer() throws Exception {
    try (UnionVector srcVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      srcVector.allocateNew();

      // write some data
      srcVector.setType(0, MinorType.INT);
      srcVector.setSafe(0, newIntHolder(5));
      srcVector.setType(1, MinorType.BIT);
      srcVector.setSafe(1, newBitHolder(false));
      srcVector.setType(3, MinorType.INT);
      srcVector.setSafe(3, newIntHolder(10));
      srcVector.setType(5, MinorType.BIT);
      srcVector.setSafe(5, newBitHolder(false));
      srcVector.setValueCount(6);

      try (UnionVector destVector =
               new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
        TransferPair pair = srcVector.makeTransferPair(destVector);

        // Creating the transfer should transfer the type of the field at least.
        assertEquals(srcVector.getField(), destVector.getField());

        // transfer
        pair.transfer();

        assertEquals(srcVector.getField(), destVector.getField());

        // now check the values are transferred
        assertEquals(6, destVector.getValueCount());

        assertFalse(destVector.isNull(0));
        assertEquals(5, destVector.getObject(0));

        assertFalse(destVector.isNull(1));
        assertEquals(false, destVector.getObject(1));

        assertNull(destVector.getObject(2));

        assertFalse(destVector.isNull(3));
        assertEquals(10, destVector.getObject(3));

        assertNull(destVector.getObject(4));

        assertFalse(destVector.isNull(5));
        assertEquals(false, destVector.getObject(5));
      }
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (UnionVector sourceVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceVector.setType(0, MinorType.INT);
      sourceVector.setSafe(0, newIntHolder(5));
      sourceVector.setType(1, MinorType.INT);
      sourceVector.setSafe(1, newIntHolder(10));
      sourceVector.setType(2, MinorType.INT);
      sourceVector.setSafe(2, newIntHolder(15));
      sourceVector.setType(3, MinorType.INT);
      sourceVector.setSafe(3, newIntHolder(20));
      sourceVector.setType(4, MinorType.INT);
      sourceVector.setSafe(4, newIntHolder(25));
      sourceVector.setType(5, MinorType.INT);
      sourceVector.setSafe(5, newIntHolder(30));
      sourceVector.setType(6, MinorType.INT);
      sourceVector.setSafe(6, newIntHolder(35));
      sourceVector.setType(7, MinorType.INT);
      sourceVector.setSafe(7, newIntHolder(40));
      sourceVector.setType(8, MinorType.INT);
      sourceVector.setSafe(8, newIntHolder(45));
      sourceVector.setType(9, MinorType.INT);
      sourceVector.setSafe(9, newIntHolder(50));
      sourceVector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceVector.getValueCount());
      assertEquals(false, sourceVector.isNull(0));
      assertEquals(5, sourceVector.getObject(0));
      assertEquals(false, sourceVector.isNull(1));
      assertEquals(10, sourceVector.getObject(1));
      assertEquals(false, sourceVector.isNull(2));
      assertEquals(15, sourceVector.getObject(2));
      assertEquals(false, sourceVector.isNull(3));
      assertEquals(20, sourceVector.getObject(3));
      assertEquals(false, sourceVector.isNull(4));
      assertEquals(25, sourceVector.getObject(4));
      assertEquals(false, sourceVector.isNull(5));
      assertEquals(30, sourceVector.getObject(5));
      assertEquals(false, sourceVector.isNull(6));
      assertEquals(35, sourceVector.getObject(6));
      assertEquals(false, sourceVector.isNull(7));
      assertEquals(40, sourceVector.getObject(7));
      assertEquals(false, sourceVector.isNull(8));
      assertEquals(45, sourceVector.getObject(8));
      assertEquals(false, sourceVector.isNull(9));
      assertEquals(50, sourceVector.getObject(9));

      try (UnionVector toVector =
               new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {{0, 3},
            {3, 1},
            {4, 2},
            {6, 1},
            {7, 1},
            {8, 2}
        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different data at indexes: " + (start + i) + "and " + i, sourceVector.getObject(start + i),
                toVector.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testSplitAndTransferWithMixedVectors() throws Exception {
    try (UnionVector sourceVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      sourceVector.setType(0, MinorType.INT);
      sourceVector.setSafe(0, newIntHolder(5));

      sourceVector.setType(1, MinorType.FLOAT4);
      sourceVector.setSafe(1, newFloat4Holder(5.5f));

      sourceVector.setType(2, MinorType.INT);
      sourceVector.setSafe(2, newIntHolder(10));

      sourceVector.setType(3, MinorType.FLOAT4);
      sourceVector.setSafe(3, newFloat4Holder(10.5f));

      sourceVector.setType(4, MinorType.INT);
      sourceVector.setSafe(4, newIntHolder(15));

      sourceVector.setType(5, MinorType.FLOAT4);
      sourceVector.setSafe(5, newFloat4Holder(15.5f));

      sourceVector.setType(6, MinorType.INT);
      sourceVector.setSafe(6, newIntHolder(20));

      sourceVector.setType(7, MinorType.FLOAT4);
      sourceVector.setSafe(7, newFloat4Holder(20.5f));

      sourceVector.setType(8, MinorType.INT);
      sourceVector.setSafe(8, newIntHolder(30));

      sourceVector.setType(9, MinorType.FLOAT4);
      sourceVector.setSafe(9, newFloat4Holder(30.5f));
      sourceVector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, sourceVector.getValueCount());
      assertEquals(false, sourceVector.isNull(0));
      assertEquals(5, sourceVector.getObject(0));
      assertEquals(false, sourceVector.isNull(1));
      assertEquals(5.5f, sourceVector.getObject(1));
      assertEquals(false, sourceVector.isNull(2));
      assertEquals(10, sourceVector.getObject(2));
      assertEquals(false, sourceVector.isNull(3));
      assertEquals(10.5f, sourceVector.getObject(3));
      assertEquals(false, sourceVector.isNull(4));
      assertEquals(15, sourceVector.getObject(4));
      assertEquals(false, sourceVector.isNull(5));
      assertEquals(15.5f, sourceVector.getObject(5));
      assertEquals(false, sourceVector.isNull(6));
      assertEquals(20, sourceVector.getObject(6));
      assertEquals(false, sourceVector.isNull(7));
      assertEquals(20.5f, sourceVector.getObject(7));
      assertEquals(false, sourceVector.isNull(8));
      assertEquals(30, sourceVector.getObject(8));
      assertEquals(false, sourceVector.isNull(9));
      assertEquals(30.5f, sourceVector.getObject(9));

      try (UnionVector toVector =
               new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {

        final TransferPair transferPair = sourceVector.makeTransferPair(toVector);

        final int[][] transferLengths = {{0, 2},
            {2, 1},
            {3, 2},
            {5, 3},
            {8, 2}
        };

        for (final int[] transferLength : transferLengths) {
          final int start = transferLength[0];
          final int length = transferLength[1];

          transferPair.splitAndTransfer(start, length);

          /* check the toVector output after doing the splitAndTransfer */
          for (int i = 0; i < length; i++) {
            assertEquals("Different values at index: " + i, sourceVector.getObject(start + i), toVector.getObject(i));
          }
        }
      }
    }
  }

  @Test
  public void testGetFieldTypeInfo() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");

    int[] typeIds = new int[2];
    typeIds[0] = MinorType.INT.ordinal();
    typeIds[1] = MinorType.VARCHAR.ordinal();

    List<Field> children = new ArrayList<>();
    children.add(new Field("int", FieldType.nullable(MinorType.INT.getType()), null));
    children.add(new Field("varchar", FieldType.nullable(MinorType.VARCHAR.getType()), null));

    final FieldType fieldType = new FieldType(false, new ArrowType.Union(UnionMode.Sparse, typeIds),
        /*dictionary=*/null, metadata);
    final Field field = new Field("union", fieldType, children);

    MinorType minorType = MinorType.UNION;
    UnionVector vector = (UnionVector) minorType.getNewVector(field, allocator, null);
    vector.initializeChildrenFromFields(children);

    assertTrue(vector.getField().equals(field));

    // Union has 2 child vectors
    assertEquals(vector.size(), 2);

    // Check child field 0
    VectorWithOrdinal intChild = vector.getChildVectorWithOrdinal("int");
    assertEquals(intChild.ordinal, 0);
    assertEquals(intChild.vector.getField(), children.get(0));

    // Check child field 1
    VectorWithOrdinal varcharChild = vector.getChildVectorWithOrdinal("varchar");
    assertEquals(varcharChild.ordinal, 1);
    assertEquals(varcharChild.vector.getField(), children.get(1));
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (UnionVector vector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      boolean error = false;

      vector.allocateNew();

      /* populate the UnionVector */
      vector.setType(0, MinorType.INT);
      vector.setSafe(0, newIntHolder(5));

      vector.setType(1, MinorType.FLOAT4);
      vector.setSafe(1, newFloat4Holder(5.5f));

      vector.setType(2, MinorType.INT);
      vector.setSafe(2, newIntHolder(10));

      vector.setType(3, MinorType.FLOAT4);
      vector.setSafe(3, newFloat4Holder(10.5f));

      vector.setValueCount(10);

      /* check the vector output */
      assertEquals(10, vector.getValueCount());
      assertEquals(false, vector.isNull(0));
      assertEquals(5, vector.getObject(0));
      assertEquals(false, vector.isNull(1));
      assertEquals(5.5f, vector.getObject(1));
      assertEquals(false, vector.isNull(2));
      assertEquals(10, vector.getObject(2));
      assertEquals(false, vector.isNull(3));
      assertEquals(10.5f, vector.getObject(3));

      List<ArrowBuf> buffers = vector.getFieldBuffers();


      try {
        long offsetAddress = vector.getOffsetBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
        error = false;
      }

      try {
        long dataAddress = vector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(1, buffers.size());
    }
  }

  @Test
  public void testSetGetNull() {
    try (UnionVector srcVector =
             new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      srcVector.allocateNew();

      final NullableIntHolder holder = new NullableIntHolder();
      holder.isSet = 1;
      holder.value = 5;

      // write some data
      srcVector.setType(0, MinorType.INT);
      srcVector.setSafe(0, holder);

      assertFalse(srcVector.isNull(0));

      holder.isSet = 0;
      srcVector.setSafe(0, holder);

      assertNull(srcVector.getObject(0));
    }
  }

  @Test
  public void testCreateNewVectorWithoutTypeExceptionThrown() {
    try (UnionVector vector =
        new UnionVector(EMPTY_SCHEMA_PATH, allocator, /* field type */ null, /* call-back */ null)) {
      IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class,
          () -> vector.getTimeStampMilliTZVector());
      assertEquals("No TimeStampMilliTZ present. Provide ArrowType argument to create a new vector", e1.getMessage());

      IllegalArgumentException e2 = assertThrows(IllegalArgumentException.class,
          () -> vector.getDurationVector());
      assertEquals("No Duration present. Provide ArrowType argument to create a new vector", e2.getMessage());

      IllegalArgumentException e3 = assertThrows(IllegalArgumentException.class,
          () -> vector.getFixedSizeBinaryVector());
      assertEquals("No FixedSizeBinary present. Provide ArrowType argument to create a new vector", e3.getMessage());

      IllegalArgumentException e4 = assertThrows(IllegalArgumentException.class,
          () -> vector.getDecimalVector());
      assertEquals("No Decimal present. Provide ArrowType argument to create a new vector", e4.getMessage());
    }
  }

  private static NullableIntHolder newIntHolder(int value) {
    final NullableIntHolder holder = new NullableIntHolder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }

  private static NullableBitHolder newBitHolder(boolean value) {
    final NullableBitHolder holder = new NullableBitHolder();
    holder.isSet = 1;
    holder.value = value ? 1 : 0;
    return holder;
  }

  private static NullableFloat4Holder newFloat4Holder(float value) {
    final NullableFloat4Holder holder = new NullableFloat4Holder();
    holder.isSet = 1;
    holder.value = value;
    return holder;
  }
}
