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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseLargeRepeatedValueViewVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListViewWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestLargeListViewVector {

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
  public void testBasicLargeListViewVector() {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      UnionLargeListViewWriter largeListViewWriter = largeListViewVector.getWriter();

      /* allocate memory */
      largeListViewWriter.allocate();

      /* write the first list at index 0 */
      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(12);
      largeListViewWriter.bigInt().writeBigInt(-7);
      largeListViewWriter.bigInt().writeBigInt(25);
      largeListViewWriter.endListView();

      /* the second list at index 1 is null (we are not setting any)*/

      /* write the third list at index 2 */
      largeListViewWriter.setPosition(2);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(0);
      largeListViewWriter.bigInt().writeBigInt(-127);
      largeListViewWriter.bigInt().writeBigInt(127);
      largeListViewWriter.bigInt().writeBigInt(50);
      largeListViewWriter.endListView();

      /* write the fourth list at index 3 (empty list) */
      largeListViewWriter.setPosition(3);
      largeListViewWriter.startListView();
      largeListViewWriter.endListView();

      /* write the fifth list at index 4 */
      largeListViewWriter.setPosition(4);
      largeListViewWriter.startListView();
      largeListViewWriter.bigInt().writeBigInt(1);
      largeListViewWriter.bigInt().writeBigInt(2);
      largeListViewWriter.bigInt().writeBigInt(3);
      largeListViewWriter.bigInt().writeBigInt(4);
      largeListViewWriter.endListView();

      largeListViewWriter.setValueCount(5);
      // check value count
      assertEquals(5, largeListViewVector.getValueCount());

      /* get vector at index 0 -- the value is a BigIntVector*/
      final ArrowBuf offSetBuffer = largeListViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = largeListViewVector.getSizeBuffer();
      final FieldVector dataVec = largeListViewVector.getDataVector();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // check data vector
      assertEquals(12, ((BigIntVector) dataVec).get(0));
      assertEquals(-7, ((BigIntVector) dataVec).get(1));
      assertEquals(25, ((BigIntVector) dataVec).get(2));
      assertEquals(0, ((BigIntVector) dataVec).get(3));
      assertEquals(-127, ((BigIntVector) dataVec).get(4));
      assertEquals(127, ((BigIntVector) dataVec).get(5));
      assertEquals(50, ((BigIntVector) dataVec).get(6));
      assertEquals(1, ((BigIntVector) dataVec).get(7));
      assertEquals(2, ((BigIntVector) dataVec).get(8));
      assertEquals(3, ((BigIntVector) dataVec).get(9));
      assertEquals(4, ((BigIntVector) dataVec).get(10));

      largeListViewVector.validate();
    }
  }

  @Test
  public void testImplicitNullVectors() {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      UnionLargeListViewWriter largeListViewWriter = largeListViewVector.getWriter();
      /* allocate memory */
      largeListViewWriter.allocate();

      final ArrowBuf offSetBuffer = largeListViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = largeListViewVector.getSizeBuffer();

      /* write the first list at index 0 */
      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(12);
      largeListViewWriter.bigInt().writeBigInt(-7);
      largeListViewWriter.bigInt().writeBigInt(25);
      largeListViewWriter.endListView();

      int offSet0 = offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH);
      int size0 = sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH);

      // after the first list is written,
      // the initial offset must be 0,
      // the size must be 3 (as there are 3 elements in the array),
      // the lastSet must be 0 since, the first list is written at index 0.

      assertEquals(0, offSet0);
      assertEquals(3, size0);

      largeListViewWriter.setPosition(5);
      largeListViewWriter.startListView();

      // writing the 6th list at index 5,
      // and the list items from index 1 through 4 are not populated.
      // but since there is a gap between the 0th and 5th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.

      for (int i = 1; i < 5; i++) {
        int offSet = offSetBuffer.getInt(i * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseLargeRepeatedValueViewVector.SIZE_WIDTH);
        // Since the list is not written, the offset and size must equal to child vector's size
        // i.e., 3, and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 5.
        assertEquals(0, offSet);
        assertEquals(0, size);
      }

      largeListViewWriter.bigInt().writeBigInt(12);
      largeListViewWriter.bigInt().writeBigInt(25);
      largeListViewWriter.endListView();

      int offSet5 = offSetBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH);
      int size5 = sizeBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH);

      assertEquals(3, offSet5);
      assertEquals(2, size5);

      largeListViewWriter.setPosition(10);
      largeListViewWriter.startListView();

      // writing the 11th list at index 10,
      // and the list items from index 6 through 10 are not populated.
      // but since there is a gap between the 5th and 11th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.
      for (int i = 6; i < 10; i++) {
        int offSet = offSetBuffer.getInt(i * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseLargeRepeatedValueViewVector.SIZE_WIDTH);
        // Since the list is not written, the offset and size must equal to 0
        // and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 10.
        assertEquals(0, offSet);
        assertEquals(0, size);
      }

      largeListViewWriter.bigInt().writeBigInt(12);
      largeListViewWriter.endListView();

      int offSet11 = offSetBuffer.getInt(10 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH);
      int size11 = sizeBuffer.getInt(10 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH);

      assertEquals(5, offSet11);
      assertEquals(1, size11);

      largeListViewVector.setValueCount(11);

      largeListViewVector.validate();
    }
  }

  @Test
  public void testNestedLargeListViewVector() throws Exception {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {

      UnionLargeListViewWriter largeListViewWriter = largeListViewVector.getWriter();

      /* allocate memory */
      largeListViewWriter.allocate();

      /* the dataVector that backs a largeListViewVector will also be a
       * largeListViewVector for this test.
       */

      /* write one or more inner lists at index 0 */
      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(50);
      largeListViewWriter.listView().bigInt().writeBigInt(100);
      largeListViewWriter.listView().bigInt().writeBigInt(200);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(75);
      largeListViewWriter.listView().bigInt().writeBigInt(125);
      largeListViewWriter.listView().bigInt().writeBigInt(150);
      largeListViewWriter.listView().bigInt().writeBigInt(175);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.endListView();

      /* write one or more inner lists at index 1 */
      largeListViewWriter.setPosition(1);
      largeListViewWriter.startListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(10);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(15);
      largeListViewWriter.listView().bigInt().writeBigInt(20);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(25);
      largeListViewWriter.listView().bigInt().writeBigInt(30);
      largeListViewWriter.listView().bigInt().writeBigInt(35);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.endList();

      largeListViewVector.setValueCount(2);

      assertEquals(2, largeListViewVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = largeListViewVector.getObject(0);
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
      result = largeListViewVector.getObject(1);
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
      assertFalse(largeListViewVector.isNull(0));
      assertFalse(largeListViewVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = largeListViewVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getLong(0 * LargeListViewVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getLong(1 * LargeListViewVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testNestedLargeListViewVector1() {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {

      MinorType listViewType = MinorType.LISTVIEW;
      MinorType scalarType = MinorType.BIGINT;

      largeListViewVector.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList1 = (ListViewVector) largeListViewVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList2 = (ListViewVector) innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList3 = (ListViewVector) innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList4 = (ListViewVector) innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList5 = (ListViewVector) innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listViewType.getType()));

      ListViewVector innerList6 = (ListViewVector) innerList5.getDataVector();
      innerList6.addOrGetVector(FieldType.nullable(scalarType.getType()));

      largeListViewVector.setInitialCapacity(128);
    }
  }

  @Test
  public void testNestedLargeListViewVector2() throws Exception {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      largeListViewVector.setInitialCapacity(1);
      UnionLargeListViewWriter largeListViewWriter = largeListViewVector.getWriter();
      /* allocate memory */
      largeListViewWriter.allocate();

      /* write one or more inner lists at index 0 */
      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(50);
      largeListViewWriter.listView().bigInt().writeBigInt(100);
      largeListViewWriter.listView().bigInt().writeBigInt(200);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(75);
      largeListViewWriter.listView().bigInt().writeBigInt(125);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.endListView();

      /* write one or more inner lists at index 1 */
      largeListViewWriter.setPosition(1);
      largeListViewWriter.startListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(15);
      largeListViewWriter.listView().bigInt().writeBigInt(20);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.listView().startListView();
      largeListViewWriter.listView().bigInt().writeBigInt(25);
      largeListViewWriter.listView().bigInt().writeBigInt(30);
      largeListViewWriter.listView().bigInt().writeBigInt(35);
      largeListViewWriter.listView().endListView();

      largeListViewWriter.endListView();

      largeListViewVector.setValueCount(2);

      assertEquals(2, largeListViewVector.getValueCount());

      /* get listVector value at index 0 -- the value itself is a listvector */
      Object result = largeListViewVector.getObject(0);
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
      result = largeListViewVector.getObject(1);
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
      assertFalse(largeListViewVector.isNull(0));
      assertFalse(largeListViewVector.isNull(1));

      /* check underlying offsets */
      final ArrowBuf offsetBuffer = largeListViewVector.getOffsetBuffer();

      /* listVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getLong(0 * LargeListViewVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getLong(1 * LargeListViewVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (LargeListViewVector largeListViewVector = LargeListViewVector.empty("vector", allocator)) {

      UnionLargeListViewWriter largeListViewWriter = largeListViewVector.getWriter();
      boolean error = false;

      largeListViewWriter.allocate();

      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();
      largeListViewWriter.bigInt().writeBigInt(50);
      largeListViewWriter.bigInt().writeBigInt(100);
      largeListViewWriter.bigInt().writeBigInt(200);
      largeListViewWriter.endListView();

      largeListViewWriter.setPosition(1);
      largeListViewWriter.startListView();
      largeListViewWriter.bigInt().writeBigInt(250);
      largeListViewWriter.bigInt().writeBigInt(300);
      largeListViewWriter.endListView();

      largeListViewVector.setValueCount(2);

      /* check listVector contents */
      Object result = largeListViewVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(100), resultSet.get(1));
      assertEquals(Long.valueOf(200), resultSet.get(2));

      result = largeListViewVector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(250), resultSet.get(0));
      assertEquals(Long.valueOf(300), resultSet.get(1));

      List<ArrowBuf> buffers = largeListViewVector.getFieldBuffers();

      long bitAddress = largeListViewVector.getValidityBufferAddress();
      long offsetAddress = largeListViewVector.getOffsetBufferAddress();
      long sizeAddress = largeListViewVector.getSizeBufferAddress();

      try {
        largeListViewVector.getDataBufferAddress();
      } catch (UnsupportedOperationException ue) {
        error = true;
      } finally {
        assertTrue(error);
      }

      assertEquals(3, buffers.size());
      assertEquals(bitAddress, buffers.get(0).memoryAddress());
      assertEquals(offsetAddress, buffers.get(1).memoryAddress());
      assertEquals(sizeAddress, buffers.get(2).memoryAddress());

      /* (3+2)/2 */
      assertEquals(2.5, largeListViewVector.getDensity(), 0);
    }
  }

  /*
   * Setting up the buffers directly needs to be validated with the base method used in
   * the ListVector class where we use the approach of startListView(),
   * write to the child vector and endListView().
   * <p>
   * To support this, we have to consider the following scenarios;
   * <p>
   * 1. Only using directly buffer-based inserts.
   * 2. Default list insertion followed by buffer-based inserts.
   * 3. Buffer-based inserts followed by default list insertion.
   */

  /* Setting up buffers directly would require the following steps to be taken
   * 0. Allocate buffers in listViewVector by calling `allocateNew` method.
   * 1. Initialize the child vector using `initializeChildrenFromFields` method.
   * 2. Set values in the child vector.
   * 3. Set validity, offset and size buffers using `setValidity`,
   * `setOffset` and `setSize` methods.
   * 4. Set value count using `setValueCount` method.
   */
  @Test
  public void testBasicLargeListViewSet() {

    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      // Allocate buffers in largeListViewVector by calling `allocateNew` method.
      largeListViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.
      FieldType fieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      largeListViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = largeListViewVector.getDataVector();
      fieldVector.clear();

      BigIntVector childVector = (BigIntVector) fieldVector;
      childVector.allocateNew(7);

      childVector.set(0, 12);
      childVector.set(1, -7);
      childVector.set(2, 25);
      childVector.set(3, 0);
      childVector.set(4, -127);
      childVector.set(5, 127);
      childVector.set(6, 50);

      childVector.setValueCount(7);

      // Set validity, offset and size buffers using `setValidity`,
      // `setOffset` and `setSize` methods.
      largeListViewVector.setOffset(0, 0);
      largeListViewVector.setOffset(1, 3);
      largeListViewVector.setOffset(2, 3);
      largeListViewVector.setOffset(3, 7);

      largeListViewVector.setSize(0, 3);
      largeListViewVector.setSize(1, 0);
      largeListViewVector.setSize(2, 4);
      largeListViewVector.setSize(3, 0);

      largeListViewVector.setValidity(0, 1);
      largeListViewVector.setValidity(1, 0);
      largeListViewVector.setValidity(2, 1);
      largeListViewVector.setValidity(3, 1);

      // Set value count using `setValueCount` method.
      largeListViewVector.setValueCount(4);

      final ArrowBuf offSetBuffer = largeListViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = largeListViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // check values
      assertEquals(12, ((BigIntVector) largeListViewVector.getDataVector()).get(0));
      assertEquals(-7, ((BigIntVector) largeListViewVector.getDataVector()).get(1));
      assertEquals(25, ((BigIntVector) largeListViewVector.getDataVector()).get(2));
      assertEquals(0, ((BigIntVector) largeListViewVector.getDataVector()).get(3));
      assertEquals(-127, ((BigIntVector) largeListViewVector.getDataVector()).get(4));
      assertEquals(127, ((BigIntVector) largeListViewVector.getDataVector()).get(5));
      assertEquals(50, ((BigIntVector) largeListViewVector.getDataVector()).get(6));

      largeListViewVector.validate();
    }
  }
}
