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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.VectorWithOrdinal;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDenseUnionVector {
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
  public void testDenseUnionVector() throws Exception {

    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    try (DenseUnionVector unionVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      unionVector.allocateNew();

      // write some data
      byte uint4TypeId = unionVector.registerNewTypeId(Field.nullable("", MinorType.UINT4.getType()));
      unionVector.setTypeId(0, uint4TypeId);
      unionVector.setSafe(0, uInt4Holder);
      unionVector.setTypeId(2, uint4TypeId);
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
  public void testTransfer() throws Exception {
    try (DenseUnionVector srcVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      srcVector.allocateNew();

      // write some data
      byte intTypeId = srcVector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));
      srcVector.setTypeId(0, intTypeId);
      srcVector.setSafe(0, newIntHolder(5));
      byte bitTypeId = srcVector.registerNewTypeId(Field.nullable("", MinorType.BIT.getType()));
      srcVector.setTypeId(1, bitTypeId);
      srcVector.setSafe(1, newBitHolder(false));
      srcVector.setTypeId(3, intTypeId);
      srcVector.setSafe(3, newIntHolder(10));
      srcVector.setTypeId(5, bitTypeId);
      srcVector.setSafe(5, newBitHolder(false));
      srcVector.setValueCount(6);

      try (DenseUnionVector destVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
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
    try (DenseUnionVector sourceVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      byte intTypeId = sourceVector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));
      sourceVector.setTypeId(0, intTypeId);
      sourceVector.setSafe(0, newIntHolder(5));
      sourceVector.setTypeId(1, intTypeId);
      sourceVector.setSafe(1, newIntHolder(10));
      sourceVector.setTypeId(2, intTypeId);
      sourceVector.setSafe(2, newIntHolder(15));
      sourceVector.setTypeId(3, intTypeId);
      sourceVector.setSafe(3, newIntHolder(20));
      sourceVector.setTypeId(4, intTypeId);
      sourceVector.setSafe(4, newIntHolder(25));
      sourceVector.setTypeId(5, intTypeId);
      sourceVector.setSafe(5, newIntHolder(30));
      sourceVector.setTypeId(6, intTypeId);
      sourceVector.setSafe(6, newIntHolder(35));
      sourceVector.setTypeId(7, intTypeId);
      sourceVector.setSafe(7, newIntHolder(40));
      sourceVector.setTypeId(8, intTypeId);
      sourceVector.setSafe(8, newIntHolder(45));
      sourceVector.setTypeId(9, intTypeId);
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

      try (DenseUnionVector toVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
        toVector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));

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
    try (DenseUnionVector sourceVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {

      sourceVector.allocateNew();

      /* populate the UnionVector */
      byte intTypeId = sourceVector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));

      sourceVector.setTypeId(0, intTypeId);
      sourceVector.setSafe(0, newIntHolder(5));

      byte float4TypeId = sourceVector.registerNewTypeId(Field.nullable("", MinorType.FLOAT4.getType()));

      sourceVector.setTypeId(1, float4TypeId);
      sourceVector.setSafe(1, newFloat4Holder(5.5f));

      sourceVector.setTypeId(2, intTypeId);
      sourceVector.setSafe(2, newIntHolder(10));

      sourceVector.setTypeId(3, float4TypeId);
      sourceVector.setSafe(3, newFloat4Holder(10.5f));

      sourceVector.setTypeId(4, intTypeId);
      sourceVector.setSafe(4, newIntHolder(15));

      sourceVector.setTypeId(5, float4TypeId);
      sourceVector.setSafe(5, newFloat4Holder(15.5f));

      sourceVector.setTypeId(6, intTypeId);
      sourceVector.setSafe(6, newIntHolder(20));

      sourceVector.setTypeId(7, float4TypeId);
      sourceVector.setSafe(7, newFloat4Holder(20.5f));

      sourceVector.setTypeId(8, intTypeId);
      sourceVector.setSafe(8, newIntHolder(30));

      sourceVector.setTypeId(9, float4TypeId);
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

      try (DenseUnionVector toVector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
        toVector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));
        toVector.registerNewTypeId(Field.nullable("", MinorType.FLOAT4.getType()));

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
    typeIds[0] = 0;
    typeIds[1] = 1;

    List<Field> children = new ArrayList<>();
    children.add(new Field("int", FieldType.nullable(MinorType.INT.getType()), null));
    children.add(new Field("varchar", FieldType.nullable(MinorType.VARCHAR.getType()), null));

    final FieldType fieldType = new FieldType(false, new ArrowType.Union(UnionMode.Dense, typeIds),
            /*dictionary=*/null, metadata);
    final Field field = new Field("union", fieldType, children);

    MinorType minorType = MinorType.DENSEUNION;
    DenseUnionVector vector = (DenseUnionVector) minorType.getNewVector(field, allocator, null);
    vector.initializeChildrenFromFields(children);

    assertEquals(vector.getField(), field);

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
    try (DenseUnionVector vector = new DenseUnionVector(EMPTY_SCHEMA_PATH, allocator, null, null)) {
      boolean error = false;

      vector.allocateNew();

      /* populate the UnionVector */
      byte intTypeId = vector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));
      vector.setTypeId(0, intTypeId);
      vector.setSafe(0, newIntHolder(5));

      byte float4TypeId = vector.registerNewTypeId(Field.nullable("", MinorType.INT.getType()));
      vector.setTypeId(1, float4TypeId);
      vector.setSafe(1, newFloat4Holder(5.5f));

      vector.setTypeId(2, intTypeId);
      vector.setSafe(2, newIntHolder(10));

      vector.setTypeId(3, float4TypeId);
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

      long offsetAddress = vector.getOffsetBufferAddress();

      try {
        vector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(2, buffers.size());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
    }
  }

  /**
   * Test adding two struct vectors to the dense union vector.
   */
  @Test
  public void testMultipleStructs() {
    FieldType type = new FieldType(true, ArrowType.Struct.INSTANCE, null, null);
    try (StructVector structVector1 = new StructVector("struct1", allocator, type, null);
         StructVector structVector2 = new StructVector("struct2", allocator, type, null);
         DenseUnionVector unionVector = DenseUnionVector.empty("union", allocator)) {

      // prepare sub vectors

      // first struct vector: (int, int)
      IntVector subVector11 = structVector1
              .addOrGet("sub11", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      subVector11.allocateNew();
      ValueVectorDataPopulator.setVector(subVector11, 0, 1);

      IntVector subVector12 = structVector1
              .addOrGet("sub12", FieldType.nullable(MinorType.INT.getType()), IntVector.class);
      subVector12.allocateNew();
      ValueVectorDataPopulator.setVector(subVector12, 0, 10);

      structVector1.setIndexDefined(0);
      structVector1.setIndexDefined(1);
      structVector1.setValueCount(2);

      // second struct vector: (string, string)
      VarCharVector subVector21 = structVector2
              .addOrGet("sub21", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);
      subVector21.allocateNew();
      ValueVectorDataPopulator.setVector(subVector21, "a0");

      VarCharVector subVector22 = structVector2
              .addOrGet("sub22", FieldType.nullable(MinorType.VARCHAR.getType()), VarCharVector.class);
      subVector22.allocateNew();
      ValueVectorDataPopulator.setVector(subVector22, "b0");

      structVector2.setIndexDefined(0);
      structVector2.setValueCount(1);

      // register relative types
      byte typeId1 = unionVector.registerNewTypeId(structVector1.getField());
      byte typeId2 = unionVector.registerNewTypeId(structVector2.getField());
      assertEquals(typeId1, 0);
      assertEquals(typeId2, 1);

      // add two struct vectors to union vector
      unionVector.addVector(typeId1, structVector1);
      unionVector.addVector(typeId2, structVector2);

      while (unionVector.getValueCapacity() < 3) {
        unionVector.reAlloc();
      }

      ArrowBuf offsetBuf = unionVector.getOffsetBuffer();

      unionVector.setTypeId(0, typeId1);
      offsetBuf.setInt(0, 0);

      unionVector.setTypeId(1, typeId2);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH, 0);

      unionVector.setTypeId(2, typeId1);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH * 2, 1);

      unionVector.setValueCount(3);

      Map<String, Integer> value0 = new JsonStringHashMap<>();
      value0.put("sub11", 0);
      value0.put("sub12", 0);

      assertEquals(value0, unionVector.getObject(0));

      Map<String, Text> value1 = new JsonStringHashMap<>();
      value1.put("sub21", new Text("a0"));
      value1.put("sub22", new Text("b0"));

      assertEquals(value1, unionVector.getObject(1));

      Map<String, Integer> value2 = new JsonStringHashMap<>();
      value2.put("sub11", 1);
      value2.put("sub12", 10);

      assertEquals(value2, unionVector.getObject(2));
    }
  }

  /**
   * Test adding two varchar vectors to the dense union vector.
   */
  @Test
  public void testMultipleVarChars() {
    try (VarCharVector childVector1 = new VarCharVector("child1", allocator);
         VarCharVector childVector2 = new VarCharVector("child2", allocator);
         DenseUnionVector unionVector = DenseUnionVector.empty("union", allocator)) {

      // prepare sub vectors
      ValueVectorDataPopulator.setVector(childVector1, "a0", "a4");
      ValueVectorDataPopulator.setVector(childVector2, "b1", "b2");

      // register relative types
      byte typeId1 = unionVector.registerNewTypeId(childVector1.getField());
      byte typeId2 = unionVector.registerNewTypeId(childVector2.getField());

      assertEquals(typeId1, 0);
      assertEquals(typeId2, 1);

      while (unionVector.getValueCapacity() < 5) {
        unionVector.reAlloc();
      }

      // add two struct vectors to union vector
      unionVector.addVector(typeId1, childVector1);
      unionVector.addVector(typeId2, childVector2);

      ArrowBuf offsetBuf = unionVector.getOffsetBuffer();

      // slot 0 points to child1
      unionVector.setTypeId(0, typeId1);
      offsetBuf.setInt(0, 0);

      // slot 1 points to child2
      unionVector.setTypeId(1, typeId2);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH, 0);

      // slot 2 points to child2
      unionVector.setTypeId(2, typeId2);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH * 2, 1);


      // slot 4 points to child1
      unionVector.setTypeId(4, typeId1);
      offsetBuf.setInt(DenseUnionVector.OFFSET_WIDTH * 4, 1);

      unionVector.setValueCount(5);

      assertEquals(new Text("a0"), unionVector.getObject(0));
      assertEquals(new Text("b1"), unionVector.getObject(1));
      assertEquals(new Text("b2"), unionVector.getObject(2));
      assertNull(unionVector.getObject(3));
      assertEquals(new Text("a4"), unionVector.getObject(4));
    }
  }

  @Test
  public void testChildVectorValueCounts() {
    final NullableIntHolder intHolder = new NullableIntHolder();
    intHolder.isSet = 1;

    final NullableBigIntHolder longHolder = new NullableBigIntHolder();
    longHolder.isSet = 1;

    final NullableFloat4Holder floatHolder = new NullableFloat4Holder();
    floatHolder.isSet = 1;

    try (DenseUnionVector vector = new DenseUnionVector("vector", allocator, null, null)) {
      vector.allocateNew();

      // populate the delta vector with values {7, null, 8L, 9.0f, 10, 12L}
      while (vector.getValueCapacity() < 6) {
        vector.reAlloc();
      }
      byte intTypeId = vector.registerNewTypeId(Field.nullable("", Types.MinorType.INT.getType()));
      vector.setTypeId(0, intTypeId);
      intHolder.value = 7;
      vector.setSafe(0, intHolder);
      byte longTypeId = vector.registerNewTypeId(Field.nullable("", Types.MinorType.BIGINT.getType()));
      vector.setTypeId(2, longTypeId);
      longHolder.value = 8L;
      vector.setSafe(2, longHolder);
      byte floatTypeId = vector.registerNewTypeId(Field.nullable("", Types.MinorType.FLOAT4.getType()));
      vector.setTypeId(3, floatTypeId);
      floatHolder.value = 9.0f;
      vector.setSafe(3, floatHolder);

      vector.setTypeId(4, intTypeId);
      intHolder.value = 10;
      vector.setSafe(4, intHolder);
      vector.setTypeId(5, longTypeId);
      longHolder.value = 12L;
      vector.setSafe(5, longHolder);

      vector.setValueCount(6);

      // verify results
      IntVector intVector = (IntVector) vector.getVectorByType(intTypeId);
      assertEquals(2, intVector.getValueCount());
      assertEquals(7, intVector.get(0));
      assertEquals(10, intVector.get(1));

      BigIntVector longVector = (BigIntVector) vector.getVectorByType(longTypeId);
      assertEquals(2, longVector.getValueCount());
      assertEquals(8L, longVector.get(0));
      assertEquals(12L, longVector.get(1));

      Float4Vector floagVector = (Float4Vector) vector.getVectorByType(floatTypeId);
      assertEquals(1, floagVector.getValueCount());
      assertEquals(9.0f, floagVector.get(0), 0);
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
