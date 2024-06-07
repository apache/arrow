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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLargeListVector {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testCopyFrom() throws Exception {
    try (LargeListVector inVector = LargeListVector.empty("input", allocator);
         LargeListVector outVector = LargeListVector.empty("output", allocator)) {
      UnionLargeListWriter writer = inVector.getWriter();
      writer.allocate();

      // populate input vector with the following records
      // [1, 2, 3]
      // null
      // []
      writer.setPosition(0); // optional
      writer.startList();
      writer.bigInt().writeBigInt(1);
      writer.bigInt().writeBigInt(2);
      writer.bigInt().writeBigInt(3);
      writer.endList();

      writer.setPosition(2);
      writer.startList();
      writer.endList();

      writer.setValueCount(3);

      // copy values from input to output
      outVector.allocateNew();
      for (int i = 0; i < 3; i++) {
        outVector.copyFrom(i, i, inVector);
      }
      outVector.setValueCount(3);

      // assert the output vector is correct
      FieldReader reader = outVector.getReader();
      assertTrue(reader.isSet(), "shouldn't be null");
      reader.setPosition(1);
      assertFalse(reader.isSet(), "should be null");
      reader.setPosition(2);
      assertTrue(reader.isSet(), "shouldn't be null");


      /* index 0 */
      Object result = outVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(1), resultSet.get(0));
      assertEquals(Long.valueOf(2), resultSet.get(1));
      assertEquals(Long.valueOf(3), resultSet.get(2));

      /* index 1 */
      result = outVector.getObject(1);
      assertNull(result);

      /* index 2 */
      result = outVector.getObject(2);
      resultSet = (ArrayList<Long>) result;
      assertEquals(0, resultSet.size());

      /* 3+0+0/3 */
      assertEquals(1.0D, inVector.getDensity(), 0);
    }
  }

  @Test
  public void testSetLastSetUsage() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("input", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      listVector.addOrGetVector(FieldType.nullable(type.getType()));

      /* allocate memory */
      listVector.allocateNew();

      /* get inner buffers; validityBuffer and offsetBuffer */

      ArrowBuf validityBuffer = listVector.getValidityBuffer();
      ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* get the underlying data vector -- BigIntVector */
      BigIntVector dataVector = (BigIntVector) listVector.getDataVector();

      /* check current lastSet */
      assertEquals(-1L, listVector.getLastSet());

      int index = 0;
      int offset;

      /* write [10, 11, 12] to the list vector at index 0 */
      BitVectorHelper.setBit(validityBuffer, index);
      dataVector.setSafe(0, 1, 10);
      dataVector.setSafe(1, 1, 11);
      dataVector.setSafe(2, 1, 12);
      offsetBuffer.setLong((index + 1) * LargeListVector.OFFSET_WIDTH, 3);

      index += 1;

      /* write [13, 14] to the list vector at index 1 */
      BitVectorHelper.setBit(validityBuffer, index);
      dataVector.setSafe(3, 1, 13);
      dataVector.setSafe(4, 1, 14);
      offsetBuffer.setLong((index + 1) * LargeListVector.OFFSET_WIDTH, 5);

      index += 1;

      /* write [15, 16, 17] to the list vector at index 2 */
      BitVectorHelper.setBit(validityBuffer, index);
      dataVector.setSafe(5, 1, 15);
      dataVector.setSafe(6, 1, 16);
      dataVector.setSafe(7, 1, 17);
      offsetBuffer.setLong((index + 1) * LargeListVector.OFFSET_WIDTH, 8);

      /* check current lastSet */
      assertEquals(-1L, listVector.getLastSet());

      /* set lastset and arbitrary valuecount for list vector.
       *
       * NOTE: if we don't execute setLastSet() before setLastValueCount(), then
       * the latter will corrupt the offsetBuffer and thus the accessor will not
       * retrieve the correct values from underlying dataBuffer. Run the test
       * by commenting out next line and we should see failures from 5th assert
       * onwards. This is why doing setLastSet() is important before setValueCount()
       * once the vector has been loaded.
       *
       * Another important thing to remember is the value of lastSet itself.
       * Even though the listVector has elements till index 2 only, the lastSet should
       * be set as 3. This is because the offsetBuffer has valid offsets filled till index 3.
       * If we do setLastSet(2), the offsetBuffer at index 3 will contain incorrect value
       * after execution of setValueCount().
       *
       * correct state of the listVector
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 3, 5, 8, 8, 8.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       *
       * if we don't do setLastSet() before setValueCount --> incorrect state
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 0, 0, 0, 0, 0.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       *
       * if we do setLastSet(2) before setValueCount --> incorrect state
       * bitvector    {1, 1, 1, 0, 0.... }
       * offsetvector {0, 3, 5, 5, 5, 5.....}
       * datavector   { [10, 11, 12],
       *                [13, 14],
       *                [15, 16, 17]
       *              }
       */
      listVector.setLastSet(2);
      listVector.setValueCount(10);

      /* (3+2+3)/10 */
      assertEquals(0.8D, listVector.getDensity(), 0);

      index = 0;
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      Long actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(10), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(11), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(12), actual);

      index++;
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(13), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(14), actual);

      index++;
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(15), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(16), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(17), actual);

      index++;
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(8), Integer.toString(offset));
      actual = dataVector.getObject(offset);
      assertNull(actual);
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("sourceVector", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      listVector.addOrGetVector(FieldType.nullable(type.getType()));

      UnionLargeListWriter listWriter = listVector.getWriter();

      /* allocate memory */
      listWriter.allocate();

      /* populate data */
      listWriter.setPosition(0);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(10);
      listWriter.bigInt().writeBigInt(11);
      listWriter.bigInt().writeBigInt(12);
      listWriter.endList();

      listWriter.setPosition(1);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(13);
      listWriter.bigInt().writeBigInt(14);
      listWriter.endList();

      listWriter.setPosition(2);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(15);
      listWriter.bigInt().writeBigInt(16);
      listWriter.bigInt().writeBigInt(17);
      listWriter.bigInt().writeBigInt(18);
      listWriter.endList();

      listWriter.setPosition(3);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(19);
      listWriter.endList();

      listWriter.setPosition(4);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(20);
      listWriter.bigInt().writeBigInt(21);
      listWriter.bigInt().writeBigInt(22);
      listWriter.bigInt().writeBigInt(23);
      listWriter.endList();

      listVector.setValueCount(5);

      assertEquals(4, listVector.getLastSet());

      /* get offset buffer */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* get dataVector */
      BigIntVector dataVector = (BigIntVector) listVector.getDataVector();

      /* check the vector output */

      int index = 0;
      int offset;
      Long actual;

      /* index 0 */
      assertFalse(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(10), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(11), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(12), actual);

      /* index 1 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(13), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(14), actual);

      /* index 2 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(15), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(16), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(17), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(18), actual);

      /* index 3 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(9), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(19), actual);

      /* index 4 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(10), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(20), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(21), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(22), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(23), actual);

      /* index 5 */
      index++;
      assertTrue(listVector.isNull(index));
      offset = (int) offsetBuffer.getLong(index * LargeListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(14), Integer.toString(offset));

      /* do split and transfer */
      try (LargeListVector toVector = LargeListVector.empty("toVector", allocator)) {

        TransferPair transferPair = listVector.makeTransferPair(toVector);

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
          BigIntVector dataVector1 = (BigIntVector) toVector.getDataVector();

          for (int i = 0; i < splitLength; i++) {
            dataLength1 = (int) offsetBuffer.getLong((start + i + 1) * LargeListVector.OFFSET_WIDTH) -
                    (int) offsetBuffer.getLong((start + i) * LargeListVector.OFFSET_WIDTH);
            dataLength2 = (int) toOffsetBuffer.getLong((i + 1) * LargeListVector.OFFSET_WIDTH) -
                    (int) toOffsetBuffer.getLong(i * LargeListVector.OFFSET_WIDTH);

            assertEquals(dataLength1, dataLength2,
                "Different data lengths at index: " + i + " and start: " + start);

            offset1 = (int) offsetBuffer.getLong((start + i) * LargeListVector.OFFSET_WIDTH);
            offset2 = (int) toOffsetBuffer.getLong(i * LargeListVector.OFFSET_WIDTH);

            for (int j = 0; j < dataLength1; j++) {
              assertEquals(dataVector.getObject(offset1), dataVector1.getObject(offset2),
                  "Different data at indexes: " + offset1 + " and " + offset2);

              offset1++;
              offset2++;
            }
          }
        }
      }
    }
  }

  @Test
  public void testNestedLargeListVector() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("sourceVector", allocator)) {

      UnionLargeListWriter listWriter = listVector.getWriter();

      /* allocate memory */
      listWriter.allocate();

      /* the dataVector that backs a listVector will also be a
       * listVector for this test.
       */

      /* write one or more inner lists at index 0 */
      listWriter.setPosition(0);
      listWriter.startList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(50);
      listWriter.list().bigInt().writeBigInt(100);
      listWriter.list().bigInt().writeBigInt(200);
      listWriter.list().endList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(75);
      listWriter.list().bigInt().writeBigInt(125);
      listWriter.list().bigInt().writeBigInt(150);
      listWriter.list().bigInt().writeBigInt(175);
      listWriter.list().endList();

      listWriter.endList();

      /* write one or more inner lists at index 1 */
      listWriter.setPosition(1);
      listWriter.startList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(10);
      listWriter.list().endList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(15);
      listWriter.list().bigInt().writeBigInt(20);
      listWriter.list().endList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(25);
      listWriter.list().bigInt().writeBigInt(30);
      listWriter.list().bigInt().writeBigInt(35);
      listWriter.list().endList();

      listWriter.endList();

      assertEquals(1, listVector.getLastSet());

      listVector.setValueCount(2);

      assertEquals(2, listVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = listVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size()); /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size()); /* size of first inner list */
      assertEquals(4, resultSet.get(1).size()); /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(50), list.get(0));
      assertEquals(Long.valueOf(100), list.get(1));
      assertEquals(Long.valueOf(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(75), list.get(0));
      assertEquals(Long.valueOf(125), list.get(1));
      assertEquals(Long.valueOf(150), list.get(2));
      assertEquals(Long.valueOf(175), list.get(3));

      /* get listVector value at index 1 -- the value itself is a listvector */
      result = listVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(3, resultSet.size()); /* 3 inner lists at index 1 */
      assertEquals(1, resultSet.get(0).size()); /* size of first inner list */
      assertEquals(2, resultSet.get(1).size()); /* size of second inner list */
      assertEquals(3, resultSet.get(2).size()); /* size of third inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(10), list.get(0));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(15), list.get(0));
      assertEquals(Long.valueOf(20), list.get(1));

      list = resultSet.get(2);
      assertEquals(Long.valueOf(25), list.get(0));
      assertEquals(Long.valueOf(30), list.get(1));
      assertEquals(Long.valueOf(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(listVector.isNull(0));
      assertFalse(listVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getLong(0 * LargeListVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getLong(1 * LargeListVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getLong(2 * LargeListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testNestedLargeListVector1() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("sourceVector", allocator)) {

      MinorType listType = MinorType.LIST;
      MinorType scalarType = MinorType.BIGINT;

      listVector.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList1 = (ListVector) listVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList2 = (ListVector) innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList3 = (ListVector) innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList4 = (ListVector) innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList5 = (ListVector) innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList6 = (ListVector) innerList5.getDataVector();
      innerList6.addOrGetVector(FieldType.nullable(scalarType.getType()));

      listVector.setInitialCapacity(128);
    }
  }

  @Test
  public void testNestedLargeListVector2() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("sourceVector", allocator)) {
      listVector.setInitialCapacity(1);
      UnionLargeListWriter listWriter = listVector.getWriter();
      /* allocate memory */
      listWriter.allocate();

      /* write one or more inner lists at index 0 */
      listWriter.setPosition(0);
      listWriter.startList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(50);
      listWriter.list().bigInt().writeBigInt(100);
      listWriter.list().bigInt().writeBigInt(200);
      listWriter.list().endList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(75);
      listWriter.list().bigInt().writeBigInt(125);
      listWriter.list().endList();

      listWriter.endList();

      /* write one or more inner lists at index 1 */
      listWriter.setPosition(1);
      listWriter.startList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(15);
      listWriter.list().bigInt().writeBigInt(20);
      listWriter.list().endList();

      listWriter.list().startList();
      listWriter.list().bigInt().writeBigInt(25);
      listWriter.list().bigInt().writeBigInt(30);
      listWriter.list().bigInt().writeBigInt(35);
      listWriter.list().endList();

      listWriter.endList();

      assertEquals(1, listVector.getLastSet());

      listVector.setValueCount(2);

      assertEquals(2, listVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = listVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size()); /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size()); /* size of first inner list */
      assertEquals(2, resultSet.get(1).size()); /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(50), list.get(0));
      assertEquals(Long.valueOf(100), list.get(1));
      assertEquals(Long.valueOf(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(75), list.get(0));
      assertEquals(Long.valueOf(125), list.get(1));

      /* get listVector value at index 1 -- the value itself is a listvector */
      result = listVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(2, resultSet.size()); /* 3 inner lists at index 1 */
      assertEquals(2, resultSet.get(0).size()); /* size of first inner list */
      assertEquals(3, resultSet.get(1).size()); /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(15), list.get(0));
      assertEquals(Long.valueOf(20), list.get(1));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(25), list.get(0));
      assertEquals(Long.valueOf(30), list.get(1));
      assertEquals(Long.valueOf(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(listVector.isNull(0));
      assertFalse(listVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getLong(0 * LargeListVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getLong(1 * LargeListVector.OFFSET_WIDTH));
      assertEquals(4, offsetBuffer.getLong(2 * LargeListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("vector", allocator)) {

      UnionLargeListWriter listWriter = listVector.getWriter();
      boolean error = false;

      listWriter.allocate();

      listWriter.setPosition(0);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(50);
      listWriter.bigInt().writeBigInt(100);
      listWriter.bigInt().writeBigInt(200);
      listWriter.endList();

      listWriter.setPosition(1);
      listWriter.startList();
      listWriter.bigInt().writeBigInt(250);
      listWriter.bigInt().writeBigInt(300);
      listWriter.endList();

      listVector.setValueCount(2);

      /* check listVector contents */
      Object result = listVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(100), resultSet.get(1));
      assertEquals(Long.valueOf(200), resultSet.get(2));

      result = listVector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(250), resultSet.get(0));
      assertEquals(Long.valueOf(300), resultSet.get(1));

      List<ArrowBuf> buffers = listVector.getFieldBuffers();

      long bitAddress = listVector.getValidityBufferAddress();
      long offsetAddress = listVector.getOffsetBufferAddress();

      try {
        listVector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(2, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());

      /* (3+2)/2 */
      assertEquals(2.5, listVector.getDensity(), 0);
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (LargeListVector listVector = LargeListVector.empty("sourceVector", allocator)) {
      String emptyListStr = listVector.getField().toString();
      assertTrue(emptyListStr.contains(LargeListVector.DATA_VECTOR_NAME));

      listVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listVector.getField().toString();
      assertTrue(emptyVectorStr.contains(LargeListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testSetInitialCapacity() {
    try (final LargeListVector vector = LargeListVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      /**
       * use the default multiplier of 5,
       * 512 * 5 => 2560 * 4 => 10240 bytes => 16KB => 4096 value capacity.
       */
      vector.setInitialCapacity(512);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512 * 5);

      /* use density as 4 */
      vector.setInitialCapacity(512, 4);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512 * 4);

      /**
       * inner value capacity we pass to data vector is 512 * 0.1 => 51
       * For an int vector this is 204 bytes of memory for data buffer
       * and 7 bytes for validity buffer.
       * and with power of 2 allocation, we allocate 256 bytes and 8 bytes
       * for the data buffer and validity buffer of the inner vector. Thus
       * value capacity of inner vector is 64
       */
      vector.setInitialCapacity(512, 0.1);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 51);

      /**
       * inner value capacity we pass to data vector is 512 * 0.01 => 5
       * For an int vector this is 20 bytes of memory for data buffer
       * and 1 byte for validity buffer.
       * and with power of 2 allocation, we allocate 32 bytes and 1 bytes
       * for the data buffer and validity buffer of the inner vector. Thus
       * value capacity of inner vector is 8
       */
      vector.setInitialCapacity(512, 0.01);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 5);

      /**
       * inner value capacity we pass to data vector is 5 * 0.1 => 0
       * which is then rounded off to 1. So we pass value count as 1
       * to the inner int vector.
       * the offset buffer of the list vector is allocated for 6 values
       * which is 24 bytes and then rounded off to 32 bytes (8 values)
       * the validity buffer of the list vector is allocated for 5
       * values which is 1 byte. This is why value capacity of the list
       * vector is 7 as we take the min of validity buffer value capacity
       * and offset buffer value capacity.
       */
      vector.setInitialCapacity(5, 0.1);
      vector.allocateNew();
      assertEquals(7, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 1);
    }
  }

  @Test
  public void testClearAndReuse() {
    try (final LargeListVector vector = LargeListVector.empty("list", allocator)) {
      BigIntVector bigIntVector =
          (BigIntVector) vector.addOrGetVector(FieldType.nullable(MinorType.BIGINT.getType())).getVector();
      vector.setInitialCapacity(10);
      vector.allocateNew();

      vector.startNewValue(0);
      bigIntVector.setSafe(0, 7);
      vector.endValue(0, 1);
      vector.startNewValue(1);
      bigIntVector.setSafe(1, 8);
      vector.endValue(1, 1);
      vector.setValueCount(2);

      Object result = vector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(8), resultSet.get(0));

      // Clear and release the buffers to trigger a realloc when adding next value
      vector.clear();

      // The list vector should reuse a buffer when reallocating the offset buffer
      vector.startNewValue(0);
      bigIntVector.setSafe(0, 7);
      vector.endValue(0, 1);
      vector.startNewValue(1);
      bigIntVector.setSafe(1, 8);
      vector.endValue(1, 1);
      vector.setValueCount(2);

      result = vector.getObject(0);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(Long.valueOf(8), resultSet.get(0));
    }
  }

  @Test
  public void testWriterGetField() {
    try (final LargeListVector vector = LargeListVector.empty("list", allocator)) {

      UnionLargeListWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endList();
      vector.setValueCount(2);

      Field expectedDataField = new Field(BaseRepeatedValueVector.DATA_VECTOR_NAME,
          FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field expectedField = new Field(vector.getName(), FieldType.nullable(ArrowType.LargeList.INSTANCE),
          Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());
    }
  }

  @Test
  public void testClose() throws Exception {
    try (final LargeListVector vector = LargeListVector.empty("list", allocator)) {

      UnionLargeListWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endList();
      vector.setValueCount(2);

      assertTrue(vector.getBufferSize() > 0);
      assertTrue(vector.getDataVector().getBufferSize() > 0);

      writer.close();
      assertEquals(0, vector.getBufferSize());
      assertEquals(0, vector.getDataVector().getBufferSize());
    }
  }

  @Test
  public void testGetBufferSizeFor() {
    try (final LargeListVector vector = LargeListVector.empty("list", allocator)) {

      UnionLargeListWriter writer = vector.getWriter();
      writer.allocate();

      //set some values
      writeIntValues(writer, new int[] {1, 2});
      writeIntValues(writer, new int[] {3, 4});
      writeIntValues(writer, new int[] {5, 6});
      writeIntValues(writer, new int[] {7, 8, 9, 10});
      writeIntValues(writer, new int[] {11, 12, 13, 14});
      writer.setValueCount(5);

      IntVector dataVector = (IntVector) vector.getDataVector();
      int[] indices = new int[] {0, 2, 4, 6, 10, 14};

      for (int valueCount = 1; valueCount <= 5; valueCount++) {
        int validityBufferSize = BitVectorHelper.getValidityBufferSize(valueCount);
        int offsetBufferSize = (valueCount + 1) * LargeListVector.OFFSET_WIDTH;

        int expectedSize = validityBufferSize + offsetBufferSize + dataVector.getBufferSizeFor(indices[valueCount]);
        assertEquals(expectedSize, vector.getBufferSizeFor(valueCount));
      }
    }
  }

  @Test
  public void testIsEmpty() {
    try (final LargeListVector vector = LargeListVector.empty("list", allocator)) {
      UnionLargeListWriter writer = vector.getWriter();
      writer.allocate();

      // set values [1,2], null, [], [5,6]
      writeIntValues(writer, new int[] {1, 2});
      writer.setPosition(2);
      writeIntValues(writer, new int[] {});
      writeIntValues(writer, new int[] {5, 6});
      writer.setValueCount(4);

      assertFalse(vector.isEmpty(0));
      assertTrue(vector.isNull(1));
      assertTrue(vector.isEmpty(1));
      assertFalse(vector.isNull(2));
      assertTrue(vector.isEmpty(2));
      assertFalse(vector.isEmpty(3));
    }
  }

  @Test
  public void testTotalCapacity() {
    final FieldType type = FieldType.nullable(MinorType.INT.getType());
    try (final LargeListVector vector = new LargeListVector("list", allocator, type, null)) {
      // Force the child vector to be allocated based on the type
      // (this is a bad API: we have to track and repeat the type twice)
      vector.addOrGetVector(type);

      // Specify the allocation size but do not actually allocate
      vector.setInitialTotalCapacity(10, 100);

      // Finally actually do the allocation
      vector.allocateNewSafe();

      // Note: allocator rounds up and can be greater than the requested allocation.
      assertTrue(vector.getValueCapacity() >= 10);
      assertTrue(vector.getDataVector().getValueCapacity() >= 100);
    }
  }

  @Test
  public void testGetTransferPairWithField() throws Exception {
    try (final LargeListVector fromVector = LargeListVector.empty("list", allocator)) {

      UnionLargeListWriter writer = fromVector.getWriter();
      writer.allocate();

      //set some values
      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endList();
      fromVector.setValueCount(2);

      final TransferPair transferPair = fromVector.getTransferPair(fromVector.getField(),
          allocator);
      final LargeListVector toVector = (LargeListVector) transferPair.getTo();
      // Field inside a new vector created by reusing a field should be the same in memory as the original field.
      assertSame(toVector.getField(), fromVector.getField());
    }
  }

  private void writeIntValues(UnionLargeListWriter writer, int[] values) {
    writer.startList();
    for (int v: values) {
      writer.integer().writeInt(v);
    }
    writer.endList();
  }
}
