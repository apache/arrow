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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestListVector {

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
  public void testCopyFrom() throws Exception {
    try (ListVector inVector = ListVector.empty("input", allocator);
         ListVector outVector = ListVector.empty("output", allocator)) {
      UnionListWriter writer = inVector.getWriter();
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
      Assert.assertTrue("shouldn't be null", reader.isSet());
      reader.setPosition(1);
      Assert.assertFalse("should be null", reader.isSet());
      reader.setPosition(2);
      Assert.assertTrue("shouldn't be null", reader.isSet());


      /* index 0 */
      Object result = outVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(new Long(1), (Long) resultSet.get(0));
      assertEquals(new Long(2), (Long) resultSet.get(1));
      assertEquals(new Long(3), (Long) resultSet.get(2));

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
    try (ListVector listVector = ListVector.empty("input", allocator)) {

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
      assertEquals(Integer.toString(0), Integer.toString(listVector.getLastSet()));

      int index = 0;
      int offset = 0;

      /* write [10, 11, 12] to the list vector at index 0 */
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      dataVector.setSafe(0, 1, 10);
      dataVector.setSafe(1, 1, 11);
      dataVector.setSafe(2, 1, 12);
      offsetBuffer.setInt((index + 1) * ListVector.OFFSET_WIDTH, 3);

      index += 1;

      /* write [13, 14] to the list vector at index 1 */
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      dataVector.setSafe(3, 1, 13);
      dataVector.setSafe(4, 1, 14);
      offsetBuffer.setInt((index + 1) * ListVector.OFFSET_WIDTH, 5);

      index += 1;

      /* write [15, 16, 17] to the list vector at index 2 */
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      dataVector.setSafe(5, 1, 15);
      dataVector.setSafe(6, 1, 16);
      dataVector.setSafe(7, 1, 17);
      offsetBuffer.setInt((index + 1) * ListVector.OFFSET_WIDTH, 8);

      /* check current lastSet */
      assertEquals(Integer.toString(0), Integer.toString(listVector.getLastSet()));

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
      listVector.setLastSet(3);
      listVector.setValueCount(10);

      /* (3+2+3)/10 */
      assertEquals(0.8D, listVector.getDensity(), 0);

      index = 0;
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      Object actual = dataVector.getObject(offset);
      assertEquals(new Long(10), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(11), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(12), (Long) actual);

      index++;
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(13), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(14), (Long) actual);

      index++;
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(15), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(16), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(17), (Long) actual);

      index++;
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(8), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertNull(actual);
    }
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (ListVector listVector = ListVector.empty("sourceVector", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      listVector.addOrGetVector(FieldType.nullable(type.getType()));

      UnionListWriter listWriter = listVector.getWriter();

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

      assertEquals(5, listVector.getLastSet());

      /* get offset buffer */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* get dataVector */
      BigIntVector dataVector = (BigIntVector) listVector.getDataVector();

      /* check the vector output */

      int index = 0;
      int offset = 0;
      Object actual = null;

      /* index 0 */
      assertFalse(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(10), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(11), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(12), (Long) actual);

      /* index 1 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(13), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(14), (Long) actual);

      /* index 2 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(15), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(16), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(17), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(18), (Long) actual);

      /* index 3 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(9), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(19), (Long) actual);

      /* index 4 */
      index++;
      assertFalse(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(10), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(new Long(20), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(21), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(22), (Long) actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(new Long(23), (Long) actual);

      /* index 5 */
      index++;
      assertTrue(listVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(14), Integer.toString(offset));

      /* do split and transfer */
      try (ListVector toVector = ListVector.empty("toVector", allocator)) {

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
            dataLength1 = offsetBuffer.getInt((start + i + 1) * ListVector.OFFSET_WIDTH) -
                    offsetBuffer.getInt((start + i) * ListVector.OFFSET_WIDTH);
            dataLength2 = toOffsetBuffer.getInt((i + 1) * ListVector.OFFSET_WIDTH) -
                    toOffsetBuffer.getInt(i * ListVector.OFFSET_WIDTH);

            assertEquals("Different data lengths at index: " + i + " and start: " + start,
                    dataLength1, dataLength2);

            offset1 = offsetBuffer.getInt((start + i) * ListVector.OFFSET_WIDTH);
            offset2 = toOffsetBuffer.getInt(i * ListVector.OFFSET_WIDTH);

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
  public void testNestedListVector() throws Exception {
    try (ListVector listVector = ListVector.empty("sourceVector", allocator)) {

      UnionListWriter listWriter = listVector.getWriter();

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

      assertEquals(2, listVector.getLastSet());

      listVector.setValueCount(2);

      assertEquals(2, listVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = listVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size());              /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size());       /* size of first inner list */
      assertEquals(4, resultSet.get(1).size());       /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(new Long(50), list.get(0));
      assertEquals(new Long(100), list.get(1));
      assertEquals(new Long(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(new Long(75), list.get(0));
      assertEquals(new Long(125), list.get(1));
      assertEquals(new Long(150), list.get(2));
      assertEquals(new Long(175), list.get(3));

      /* get listVector value at index 1 -- the value itself is a listvector */
      result = listVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(3, resultSet.size());              /* 3 inner lists at index 1 */
      assertEquals(1, resultSet.get(0).size());       /* size of first inner list */
      assertEquals(2, resultSet.get(1).size());       /* size of second inner list */
      assertEquals(3, resultSet.get(2).size());       /* size of third inner list */

      list = resultSet.get(0);
      assertEquals(new Long(10), list.get(0));

      list = resultSet.get(1);
      assertEquals(new Long(15), list.get(0));
      assertEquals(new Long(20), list.get(1));

      list = resultSet.get(2);
      assertEquals(new Long(25), list.get(0));
      assertEquals(new Long(30), list.get(1));
      assertEquals(new Long(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(listVector.isNull(0));
      assertFalse(listVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getInt(0 * ListVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getInt(1 * ListVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(2 * ListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testNestedListVector1() throws Exception {
    try (ListVector listVector = ListVector.empty("sourceVector", allocator)) {

      MinorType listType = MinorType.LIST;
      MinorType scalarType = MinorType.BIGINT;

      listVector.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList1 = (ListVector)listVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList2 = (ListVector)innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList3 = (ListVector)innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList4 = (ListVector)innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList5 = (ListVector)innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listType.getType()));

      ListVector innerList6 = (ListVector)innerList5.getDataVector();
      innerList6.addOrGetVector(FieldType.nullable(scalarType.getType()));

      listVector.setInitialCapacity(128);
    }
  }

  @Test
  public void testNestedListVector2() throws Exception {
    try (ListVector listVector = ListVector.empty("sourceVector", allocator)) {
      listVector.setInitialCapacity(1);
      UnionListWriter listWriter = listVector.getWriter();
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

      assertEquals(2, listVector.getLastSet());

      listVector.setValueCount(2);

      assertEquals(2, listVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = listVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size());              /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size());       /* size of first inner list */
      assertEquals(2, resultSet.get(1).size());       /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(new Long(50), list.get(0));
      assertEquals(new Long(100), list.get(1));
      assertEquals(new Long(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(new Long(75), list.get(0));
      assertEquals(new Long(125), list.get(1));

      /* get listVector value at index 1 -- the value itself is a listvector */
      result = listVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(2, resultSet.size());              /* 3 inner lists at index 1 */
      assertEquals(2, resultSet.get(0).size());       /* size of first inner list */
      assertEquals(3, resultSet.get(1).size());       /* size of second inner list */

      list = resultSet.get(0);
      assertEquals(new Long(15), list.get(0));
      assertEquals(new Long(20), list.get(1));

      list = resultSet.get(1);
      assertEquals(new Long(25), list.get(0));
      assertEquals(new Long(30), list.get(1));
      assertEquals(new Long(35), list.get(2));

      /* check underlying bitVector */
      assertFalse(listVector.isNull(0));
      assertFalse(listVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = listVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getInt(0 * ListVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getInt(1 * ListVector.OFFSET_WIDTH));
      assertEquals(4, offsetBuffer.getInt(2 * ListVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (ListVector listVector = ListVector.empty("vector", allocator)) {

      UnionListWriter listWriter = listVector.getWriter();
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
      assertEquals(new Long(50), resultSet.get(0));
      assertEquals(new Long(100), resultSet.get(1));
      assertEquals(new Long(200), resultSet.get(2));

      result = listVector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(new Long(250), resultSet.get(0));
      assertEquals(new Long(300), resultSet.get(1));

      List<ArrowBuf> buffers = listVector.getFieldBuffers();

      long bitAddress = listVector.getValidityBufferAddress();
      long offsetAddress = listVector.getOffsetBufferAddress();

      try {
        long dataAddress = listVector.getDataBufferAddress();
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
    try (ListVector listVector = ListVector.empty("sourceVector", allocator)) {
      String emptyListStr = listVector.getField().toString();
      assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listVector.getField().toString();
      assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testSetInitialCapacity() {
    try (final ListVector vector = ListVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      /**
       * use the default multiplier of 5,
       * 512 * 5 => 2560 * 4 => 10240 bytes => 16KB => 4096 value capacity.
       */
      vector.setInitialCapacity(512);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertEquals(4096, vector.getDataVector().getValueCapacity());

      /* use density as 4 */
      vector.setInitialCapacity(512, 4);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertEquals(512 * 4, vector.getDataVector().getValueCapacity());

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
      assertEquals(64, vector.getDataVector().getValueCapacity());

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
      assertEquals(8, vector.getDataVector().getValueCapacity());

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
      assertEquals(1, vector.getDataVector().getValueCapacity());
    }
  }

  @Test
  public void testClearAndReuse() {
    try (final ListVector vector = ListVector.empty("list", allocator)) {
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
      assertEquals(new Long(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(new Long(8), resultSet.get(0));

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
      assertEquals(new Long(7), resultSet.get(0));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(new Long(8), resultSet.get(0));
    }
  }
}
