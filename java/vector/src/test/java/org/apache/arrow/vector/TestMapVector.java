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
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestMapVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  public <T> T getResultKey(Map<?, T> resultStruct) {
    assertTrue(resultStruct.containsKey(MapVector.KEY_NAME));
    return resultStruct.get(MapVector.KEY_NAME);
  }

  public <T> T getResultValue(Map<?, T> resultStruct) {
    assertTrue(resultStruct.containsKey(MapVector.VALUE_NAME));
    return resultStruct.get(MapVector.VALUE_NAME);
  }

  @Test
  public void testBasicOperation() {
    int count = 5;
    try (MapVector mapVector = MapVector.empty("map", allocator, false)) {
      mapVector.allocateNew();
      UnionMapWriter mapWriter = mapVector.getWriter();
      for (int i = 0; i < count; i++) {
        mapWriter.startMap();
        for (int j = 0; j < i + 1; j++) {
          mapWriter.startEntry();
          mapWriter.key().bigInt().writeBigInt(j);
          mapWriter.value().integer().writeInt(j);
          mapWriter.endEntry();
        }
        mapWriter.endMap();
      }
      mapWriter.setValueCount(count);
      UnionMapReader mapReader = mapVector.getReader();
      for (int i = 0; i < count; i++) {
        mapReader.setPosition(i);
        for (int j = 0; j < i + 1; j++) {
          mapReader.next();
          assertEquals("record: " + i, j, mapReader.key().readLong().longValue());
          assertEquals(j, mapReader.value().readInteger().intValue());
        }
      }
    }
  }

  @Test
  public void testBasicOperationNulls() {
    int count = 6;
    try (MapVector mapVector = MapVector.empty("map", allocator, false)) {
      mapVector.allocateNew();
      UnionMapWriter mapWriter = mapVector.getWriter();
      for (int i = 0; i < count; i++) {
        // i == 1 is a NULL
        if (i != 1) {
          mapWriter.setPosition(i);
          mapWriter.startMap();
          // i == 3 is an empty map
          if (i != 3) {
            for (int j = 0; j < i + 1; j++) {
              mapWriter.startEntry();
              mapWriter.key().bigInt().writeBigInt(j);
              // i == 5 maps to a NULL value
              if (i != 5) {
                mapWriter.value().integer().writeInt(j);
              }
              mapWriter.endEntry();
            }
          }
          mapWriter.endMap();
        }
      }
      mapWriter.setValueCount(count);
      UnionMapReader mapReader = mapVector.getReader();
      for (int i = 0; i < count; i++) {
        mapReader.setPosition(i);
        if (i == 1) {
          assertFalse(mapReader.isSet());
        } else {
          if (i == 3) {
            JsonStringArrayList<?> result = (JsonStringArrayList<?>) mapReader.readObject();
            assertTrue(result.isEmpty());
          } else {
            for (int j = 0; j < i + 1; j++) {
              mapReader.next();
              assertEquals("record: " + i, j, mapReader.key().readLong().longValue());
              if (i == 5) {
                assertFalse(mapReader.value().isSet());
              } else {
                assertEquals(j, mapReader.value().readInteger().intValue());
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void testCopyFrom() throws Exception {
    try (MapVector inVector = MapVector.empty("input", allocator, false);
         MapVector outVector = MapVector.empty("output", allocator, false)) {
      UnionMapWriter writer = inVector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // {1 -> 11, 2 -> 22, 3 -> 33}
      // null
      // {2 -> null}
      writer.setPosition(0); // optional
      writer.startMap();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(1);
      writer.value().bigInt().writeBigInt(11);
      writer.endEntry();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(2);
      writer.value().bigInt().writeBigInt(22);
      writer.endEntry();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(3);
      writer.value().bigInt().writeBigInt(33);
      writer.endEntry();
      writer.endMap();

      writer.setPosition(2);
      writer.startMap();
      writer.startEntry();
      writer.key().bigInt().writeBigInt(2);
      writer.endEntry();
      writer.endMap();

      writer.setValueCount(3);

      // copy values from input to output
      outVector.allocateNew();
      for (int i = 0; i < 3; i++) {
        outVector.copyFrom(i, i, inVector);
      }
      outVector.setValueCount(3);

      // assert the output vector is correct
      FieldReader reader = outVector.getReader();
      assertTrue("shouldn't be null", reader.isSet());
      reader.setPosition(1);
      assertFalse("should be null", reader.isSet());
      reader.setPosition(2);
      assertTrue("shouldn't be null", reader.isSet());


      /* index 0 */
      Object result = outVector.getObject(0);
      ArrayList<?> resultSet = (ArrayList<?>) result;
      assertEquals(3, resultSet.size());
      Map<?, ?> resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(1L, getResultKey(resultStruct));
      assertEquals(11L, getResultValue(resultStruct));
      resultStruct = (Map<?, ?>) resultSet.get(1);
      assertEquals(2L, getResultKey(resultStruct));
      assertEquals(22L, getResultValue(resultStruct));
      resultStruct = (Map<?, ?>) resultSet.get(2);
      assertEquals(3L, getResultKey(resultStruct));
      assertEquals(33L, getResultValue(resultStruct));

      /* index 1 */
      result = outVector.getObject(1);
      assertNull(result);

      /* index 2 */
      result = outVector.getObject(2);
      resultSet = (ArrayList<?>) result;
      assertEquals(1, resultSet.size());
      resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(2L, getResultKey(resultStruct));
      assertFalse(resultStruct.containsKey(MapVector.VALUE_NAME));
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (MapVector mapVector = MapVector.empty("sourceVector", allocator, false)) {

      /* Explicitly add the map child vectors */
      FieldType type = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
      AddOrGetResult<StructVector> addResult = mapVector.addOrGetVector(type);
      FieldType keyType = new FieldType(false, MinorType.BIGINT.getType(), null, null);
      FieldType valueType = FieldType.nullable(MinorType.FLOAT8.getType());
      addResult.getVector().addOrGet(MapVector.KEY_NAME, keyType, BigIntVector.class);
      addResult.getVector().addOrGet(MapVector.VALUE_NAME, valueType, Float8Vector.class);

      UnionMapWriter mapWriter = mapVector.getWriter();

      /* allocate memory */
      mapWriter.allocate();

      /* populate data */
      mapWriter.setPosition(0);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(10);
      mapWriter.value().float8().writeFloat8(1.0);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(11);
      mapWriter.value().float8().writeFloat8(1.1);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(12);
      mapWriter.value().float8().writeFloat8(1.2);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setPosition(1);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(13);
      mapWriter.value().float8().writeFloat8(1.3);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(14);
      mapWriter.value().float8().writeFloat8(1.4);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setPosition(2);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(15);
      mapWriter.value().float8().writeFloat8(1.5);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(16);
      mapWriter.value().float8().writeFloat8(1.6);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(17);
      mapWriter.value().float8().writeFloat8(1.7);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(18);
      mapWriter.value().float8().writeFloat8(1.8);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setPosition(3);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(19);
      mapWriter.value().float8().writeFloat8(1.9);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setPosition(4);
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(20);
      mapWriter.value().float8().writeFloat8(2.0);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(21);
      mapWriter.value().float8().writeFloat8(2.1);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(22);
      mapWriter.value().float8().writeFloat8(2.2);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(23);
      mapWriter.value().float8().writeFloat8(2.3);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapVector.setValueCount(5);

      assertEquals(5, mapVector.getLastSet());

      /* get offset buffer */
      final ArrowBuf offsetBuffer = mapVector.getOffsetBuffer();

      /* get dataVector */
      StructVector dataVector = (StructVector) mapVector.getDataVector();

      /* check the vector output */
      int index = 0;
      int offset = 0;
      Map<?, ?> result = null;

      /* index 0 */
      assertFalse(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(10L, getResultKey(result));
      assertEquals(1.0, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(11L, getResultKey(result));
      assertEquals(1.1, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(12L, getResultKey(result));
      assertEquals(1.2, getResultValue(result));

      /* index 1 */
      index++;
      assertFalse(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(13L, getResultKey(result));
      assertEquals(1.3, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(14L, getResultKey(result));
      assertEquals(1.4, getResultValue(result));

      /* index 2 */
      index++;
      assertFalse(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(15L, getResultKey(result));
      assertEquals(1.5, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(16L, getResultKey(result));
      assertEquals(1.6, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(17L, getResultKey(result));
      assertEquals(1.7, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(18L, getResultKey(result));
      assertEquals(1.8, getResultValue(result));

      /* index 3 */
      index++;
      assertFalse(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(9), Integer.toString(offset));

      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(19L, getResultKey(result));
      assertEquals(1.9, getResultValue(result));

      /* index 4 */
      index++;
      assertFalse(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(10), Integer.toString(offset));

      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(20L, getResultKey(result));
      assertEquals(2.0, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(21L, getResultKey(result));
      assertEquals(2.1, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(22L, getResultKey(result));
      assertEquals(2.2, getResultValue(result));
      offset++;
      result = (Map<?, ?>) dataVector.getObject(offset);
      assertEquals(23L, getResultKey(result));
      assertEquals(2.3, getResultValue(result));

      /* index 5 */
      index++;
      assertTrue(mapVector.isNull(index));
      offset = offsetBuffer.getInt(index * MapVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(14), Integer.toString(offset));

      /* do split and transfer */
      try (MapVector toVector = MapVector.empty("toVector", allocator, false)) {

        TransferPair transferPair = mapVector.makeTransferPair(toVector);

        int[][] transferLengths = {{0, 2}, {3, 1}, {4, 1}};

        for (final int[] transferLength : transferLengths) {
          int start = transferLength[0];
          int splitLength = transferLength[1];

          int dataLength1 = 0;
          int dataLength2 = 0;

          int offset1 = 0;
          int offset2 = 0;

          transferPair.splitAndTransfer(start, splitLength);

          /* get offsetBuffer of toVector */
          final ArrowBuf toOffsetBuffer = toVector.getOffsetBuffer();

          /* get dataVector of toVector */
          StructVector dataVector1 = (StructVector) toVector.getDataVector();

          for (int i = 0; i < splitLength; i++) {
            dataLength1 = offsetBuffer.getInt((start + i + 1) * MapVector.OFFSET_WIDTH) -
                    offsetBuffer.getInt((start + i) * MapVector.OFFSET_WIDTH);
            dataLength2 = toOffsetBuffer.getInt((i + 1) * MapVector.OFFSET_WIDTH) -
                    toOffsetBuffer.getInt(i * MapVector.OFFSET_WIDTH);

            assertEquals("Different data lengths at index: " + i + " and start: " + start,
                    dataLength1, dataLength2);

            offset1 = offsetBuffer.getInt((start + i) * MapVector.OFFSET_WIDTH);
            offset2 = toOffsetBuffer.getInt(i * MapVector.OFFSET_WIDTH);

            for (int j = 0; j < dataLength1; j++) {
              assertEquals("Different data at indexes: " + offset1 + " and " + offset2,
                      dataVector.getObject(offset1), dataVector1.getObject(offset2));

              offset1++;
              offset2++;
            }
          }
        }
      }
    }
  }

  @Test
  public void testMapWithListValue() throws Exception {
    try (MapVector mapVector = MapVector.empty("sourceVector", allocator, false)) {

      UnionMapWriter mapWriter = mapVector.getWriter();
      ListWriter valueWriter;

      /* allocate memory */
      mapWriter.allocate();

      /* the dataVector that backs a listVector will also be a
       * listVector for this test.
       */

      /* write one or more maps index 0 */
      mapWriter.setPosition(0);
      mapWriter.startMap();

      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(1);
      valueWriter = mapWriter.value().list();
      valueWriter.startList();
      valueWriter.bigInt().writeBigInt(50);
      valueWriter.bigInt().writeBigInt(100);
      valueWriter.bigInt().writeBigInt(200);
      valueWriter.endList();
      mapWriter.endEntry();

      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(2);
      valueWriter = mapWriter.value().list();
      valueWriter.startList();
      valueWriter.bigInt().writeBigInt(75);
      valueWriter.bigInt().writeBigInt(125);
      valueWriter.bigInt().writeBigInt(150);
      valueWriter.bigInt().writeBigInt(175);
      valueWriter.endList();
      mapWriter.endEntry();

      mapWriter.endMap();

      /* write one or more maps at index 1 */
      mapWriter.setPosition(1);
      mapWriter.startMap();

      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(3);
      valueWriter = mapWriter.value().list();
      valueWriter.startList();
      valueWriter.bigInt().writeBigInt(10);
      valueWriter.endList();
      mapWriter.endEntry();

      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(4);
      valueWriter = mapWriter.value().list();
      valueWriter.startList();
      valueWriter.bigInt().writeBigInt(15);
      valueWriter.bigInt().writeBigInt(20);
      valueWriter.endList();
      mapWriter.endEntry();

      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(5);
      valueWriter = mapWriter.value().list();
      valueWriter.startList();
      valueWriter.bigInt().writeBigInt(25);
      valueWriter.bigInt().writeBigInt(30);
      valueWriter.bigInt().writeBigInt(35);
      valueWriter.endList();
      mapWriter.endEntry();

      mapWriter.endMap();

      assertEquals(2, mapVector.getLastSet());

      mapWriter.setValueCount(2);

      assertEquals(2, mapVector.getValueCount());

      // Get mapVector element at index 0
      Object result = mapVector.getObject(0);
      ArrayList<?> resultSet = (ArrayList<?>) result;

      // 2 map entries at index 0
      assertEquals(2, resultSet.size());

      // First Map entry
      Map<?, ?> resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(1L, getResultKey(resultStruct));
      ArrayList<Long> list = (ArrayList<Long>) getResultValue(resultStruct);
      assertEquals(3, list.size());  // value is a list with 3 elements
      assertEquals(new Long(50), list.get(0));
      assertEquals(new Long(100), list.get(1));
      assertEquals(new Long(200), list.get(2));

      // Second Map entry
      resultStruct = (Map<?, ?>) resultSet.get(1);
      list = (ArrayList<Long>) getResultValue(resultStruct);
      assertEquals(4, list.size());  // value is a list with 4 elements
      assertEquals(new Long(75), list.get(0));
      assertEquals(new Long(125), list.get(1));
      assertEquals(new Long(150), list.get(2));
      assertEquals(new Long(175), list.get(3));

      // Get mapVector element at index 1
      result = mapVector.getObject(1);
      resultSet = (ArrayList<?>) result;

      // First Map entry
      resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(3L, getResultKey(resultStruct));
      list = (ArrayList<Long>) getResultValue(resultStruct);
      assertEquals(1, list.size());  // value is a list with 1 element
      assertEquals(new Long(10), list.get(0));

      // Second Map entry
      resultStruct = (Map<?, ?>) resultSet.get(1);
      assertEquals(4L, getResultKey(resultStruct));
      list = (ArrayList<Long>) getResultValue(resultStruct);
      assertEquals(2, list.size());  // value is a list with 1 element
      assertEquals(new Long(15), list.get(0));
      assertEquals(new Long(20), list.get(1));

      // Third Map entry
      resultStruct = (Map<?, ?>) resultSet.get(2);
      assertEquals(5L, getResultKey(resultStruct));
      list = (ArrayList<Long>) getResultValue(resultStruct);
      assertEquals(3, list.size());  // value is a list with 1 element
      assertEquals(new Long(25), list.get(0));
      assertEquals(new Long(30), list.get(1));
      assertEquals(new Long(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(mapVector.isNull(0));
      assertFalse(mapVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = mapVector.getOffsetBuffer();

      /* mapVector has 2 entries at index 0 and 3 entries at index 1 */
      assertEquals(0, offsetBuffer.getInt(0 * MapVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getInt(1 * MapVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(2 * MapVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testClearAndReuse() {
    try (final MapVector vector = MapVector.empty("map", allocator, false)) {
      vector.allocateNew();
      UnionMapWriter mapWriter = vector.getWriter();

      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(1);
      mapWriter.value().integer().writeInt(11);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(2);
      mapWriter.value().integer().writeInt(22);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setValueCount(2);

      Object result = vector.getObject(0);
      ArrayList<?> resultSet = (ArrayList<?>) result;
      Map<?, ?> resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(1L, getResultKey(resultStruct));
      assertEquals(11, getResultValue(resultStruct));

      result = vector.getObject(1);
      resultSet = (ArrayList<?>) result;
      resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(2L, getResultKey(resultStruct));
      assertEquals(22, getResultValue(resultStruct));

      // Clear and release the buffers to trigger a realloc when adding next value
      vector.clear();
      mapWriter = new UnionMapWriter(vector);

      // The map vector should reuse a buffer when reallocating the offset buffer
      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(3);
      mapWriter.value().integer().writeInt(33);
      mapWriter.endEntry();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(4);
      mapWriter.value().integer().writeInt(44);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.startMap();
      mapWriter.startEntry();
      mapWriter.key().bigInt().writeBigInt(5);
      mapWriter.value().integer().writeInt(55);
      mapWriter.endEntry();
      mapWriter.endMap();

      mapWriter.setValueCount(2);

      result = vector.getObject(0);
      resultSet = (ArrayList<?>) result;
      resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(3L, getResultKey(resultStruct));
      assertEquals(33, getResultValue(resultStruct));
      resultStruct = (Map<?, ?>) resultSet.get(1);
      assertEquals(4L, getResultKey(resultStruct));
      assertEquals(44, getResultValue(resultStruct));

      result = vector.getObject(1);
      resultSet = (ArrayList<?>) result;
      resultStruct = (Map<?, ?>) resultSet.get(0);
      assertEquals(5L, getResultKey(resultStruct));
      assertEquals(55, getResultValue(resultStruct));
    }
  }
}
