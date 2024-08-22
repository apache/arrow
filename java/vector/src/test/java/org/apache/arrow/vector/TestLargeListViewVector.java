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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseLargeRepeatedValueViewVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListViewWriter;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
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

      largeListViewWriter.endListView();

      largeListViewVector.setValueCount(2);

      assertEquals(2, largeListViewVector.getValueCount());

      /* get largeListViewVector value at index 0 -- the value itself is a largeListViewVector */
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

      /* get largeListViewVector value at index 1 -- the value itself is a largeListViewVector */
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

      /* largeListViewVector has 2 lists at index 0 and 3 lists at index 1 */
      assertEquals(0, offsetBuffer.getLong(0 * LargeListViewVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getLong(1 * LargeListViewVector.OFFSET_WIDTH));
    }
  }

  @Test
  public void testNestedLargeListViewVector1() {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {

      MinorType listViewType = MinorType.LARGELISTVIEW;
      MinorType scalarType = MinorType.BIGINT;

      largeListViewVector.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList1 = (LargeListViewVector) largeListViewVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList2 = (LargeListViewVector) innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList3 = (LargeListViewVector) innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList4 = (LargeListViewVector) innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList5 = (LargeListViewVector) innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listViewType.getType()));

      LargeListViewVector innerList6 = (LargeListViewVector) innerList5.getDataVector();
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

      /* get largeListViewVector value at index 0 -- the value itself is a largeListViewVector */
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

      /* get largeListViewVector value at index 1 -- the value itself is a largeListViewVector */
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

      /* largeListViewVector has 2 lists at index 0 and 3 lists at index 1 */
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

      /* check largeListViewVector contents */
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
   * the LargeListViewVector class where we use the approach of startListView(),
   * write to the child vector and endListView().
   * <p>
   * To support this, we have to consider the following scenarios;
   * <p>
   * 1. Only using directly buffer-based inserts.
   * 2. Default list insertion followed by buffer-based inserts.
   * 3. Buffer-based inserts followed by default list insertion.
   */

  /* Setting up buffers directly would require the following steps to be taken
   * 0. Allocate buffers in largeListViewVector by calling `allocateNew` method.
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

  @Test
  public void testBasicLargeListViewSetNested() {
    // Expected largeListViewVector
    // [[[50,100,200],[75,125,150,175]],[[10],[15,20],[25,30,35]]]

    // Setting child vector
    // [[50,100,200],[75,125,150,175],[10],[15,20],[25,30,35]]
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      // Allocate buffers in largeListViewVector by calling `allocateNew` method.
      largeListViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.
      FieldType fieldType = new FieldType(true, new ArrowType.LargeListView(), null, null);
      FieldType childFieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field childField = new Field("child-vector", childFieldType, null);
      List<Field> children = new ArrayList<>();
      children.add(childField);
      Field field = new Field("child-vector", fieldType, children);
      largeListViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = largeListViewVector.getDataVector();
      fieldVector.clear();

      LargeListViewVector childVector = (LargeListViewVector) fieldVector;
      UnionLargeListViewWriter largeListViewWriter = childVector.getWriter();
      largeListViewWriter.allocate();

      largeListViewWriter.setPosition(0);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(50);
      largeListViewWriter.bigInt().writeBigInt(100);
      largeListViewWriter.bigInt().writeBigInt(200);

      largeListViewWriter.endListView();

      largeListViewWriter.setPosition(1);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(75);
      largeListViewWriter.bigInt().writeBigInt(125);
      largeListViewWriter.bigInt().writeBigInt(150);
      largeListViewWriter.bigInt().writeBigInt(175);

      largeListViewWriter.endListView();

      largeListViewWriter.setPosition(2);
      largeListViewWriter.startListView();

      largeListViewWriter.bigInt().writeBigInt(10);

      largeListViewWriter.endListView();

      largeListViewWriter.startListView();
      largeListViewWriter.setPosition(3);

      largeListViewWriter.bigInt().writeBigInt(15);
      largeListViewWriter.bigInt().writeBigInt(20);

      largeListViewWriter.endListView();

      largeListViewWriter.startListView();
      largeListViewWriter.setPosition(4);

      largeListViewWriter.bigInt().writeBigInt(25);
      largeListViewWriter.bigInt().writeBigInt(30);
      largeListViewWriter.bigInt().writeBigInt(35);

      largeListViewWriter.endListView();

      childVector.setValueCount(5);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.

      largeListViewVector.setValidity(0, 1);
      largeListViewVector.setValidity(1, 1);

      largeListViewVector.setOffset(0, 0);
      largeListViewVector.setOffset(1, 2);

      largeListViewVector.setSize(0, 2);
      largeListViewVector.setSize(1, 3);

      // Set value count using `setValueCount` method.
      largeListViewVector.setValueCount(2);

      assertEquals(2, largeListViewVector.getValueCount());

      /* get largeListViewVector value at index 0 -- the value itself is a largeListViewVector */
      Object result = largeListViewVector.getObject(0);
      ArrayList<ArrayList<Long>> resultSet = (ArrayList<ArrayList<Long>>) result;
      ArrayList<Long> list;

      assertEquals(2, resultSet.size()); /* 2 inner lists at index 0 */
      assertEquals(3, resultSet.get(0).size()); /* size of the first inner list */
      assertEquals(4, resultSet.get(1).size()); /* size of the second inner list */

      list = resultSet.get(0);
      assertEquals(Long.valueOf(50), list.get(0));
      assertEquals(Long.valueOf(100), list.get(1));
      assertEquals(Long.valueOf(200), list.get(2));

      list = resultSet.get(1);
      assertEquals(Long.valueOf(75), list.get(0));
      assertEquals(Long.valueOf(125), list.get(1));
      assertEquals(Long.valueOf(150), list.get(2));
      assertEquals(Long.valueOf(175), list.get(3));

      /* get largeListViewVector value at index 1 -- the value itself is a largeListViewVector */
      result = largeListViewVector.getObject(1);
      resultSet = (ArrayList<ArrayList<Long>>) result;

      assertEquals(3, resultSet.size()); /* 3 inner lists at index 1 */
      assertEquals(1, resultSet.get(0).size()); /* size of the first inner list */
      assertEquals(2, resultSet.get(1).size()); /* size of the second inner list */
      assertEquals(3, resultSet.get(2).size()); /* size of the third inner list */

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

      final ArrowBuf offSetBuffer = largeListViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = largeListViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(2, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      largeListViewVector.validate();
    }
  }

  @Test
  public void testBasicLargeListViewSetWithListViewWriter() {
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
      //  `setOffset` and `setSize` methods.

      largeListViewVector.setValidity(0, 1);
      largeListViewVector.setValidity(1, 0);
      largeListViewVector.setValidity(2, 1);
      largeListViewVector.setValidity(3, 1);

      largeListViewVector.setOffset(0, 0);
      largeListViewVector.setOffset(1, 3);
      largeListViewVector.setOffset(2, 3);
      largeListViewVector.setOffset(3, 7);

      largeListViewVector.setSize(0, 3);
      largeListViewVector.setSize(1, 0);
      largeListViewVector.setSize(2, 4);
      largeListViewVector.setSize(3, 0);

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

      UnionLargeListViewWriter listViewWriter = largeListViewVector.getWriter();

      listViewWriter.setPosition(4);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(121);
      listViewWriter.bigInt().writeBigInt(-71);
      listViewWriter.bigInt().writeBigInt(251);
      listViewWriter.endListView();

      largeListViewVector.setValueCount(5);

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // check values
      assertEquals(12, ((BigIntVector) largeListViewVector.getDataVector()).get(0));
      assertEquals(-7, ((BigIntVector) largeListViewVector.getDataVector()).get(1));
      assertEquals(25, ((BigIntVector) largeListViewVector.getDataVector()).get(2));
      assertEquals(0, ((BigIntVector) largeListViewVector.getDataVector()).get(3));
      assertEquals(-127, ((BigIntVector) largeListViewVector.getDataVector()).get(4));
      assertEquals(127, ((BigIntVector) largeListViewVector.getDataVector()).get(5));
      assertEquals(50, ((BigIntVector) largeListViewVector.getDataVector()).get(6));
      assertEquals(121, ((BigIntVector) largeListViewVector.getDataVector()).get(7));
      assertEquals(-71, ((BigIntVector) largeListViewVector.getDataVector()).get(8));
      assertEquals(251, ((BigIntVector) largeListViewVector.getDataVector()).get(9));

      largeListViewVector.validate();
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("sourceVector", allocator)) {
      String emptyListStr = largeListViewVector.getField().toString();
      assertTrue(emptyListStr.contains(LargeListViewVector.DATA_VECTOR_NAME));

      largeListViewVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = largeListViewVector.getField().toString();
      assertTrue(emptyVectorStr.contains(LargeListViewVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testSetInitialCapacity() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("", allocator)) {
      vector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));

      vector.setInitialCapacity(512);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512);

      vector.setInitialCapacity(512, 4);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 512 * 4);

      vector.setInitialCapacity(512, 0.1);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 51);

      vector.setInitialCapacity(512, 0.01);
      vector.allocateNew();
      assertEquals(512, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 5);

      vector.setInitialCapacity(5, 0.1);
      vector.allocateNew();
      assertEquals(8, vector.getValueCapacity());
      assertTrue(vector.getDataVector().getValueCapacity() >= 1);

      vector.validate();
    }
  }

  @Test
  public void testClearAndReuse() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("list", allocator)) {
      BigIntVector bigIntVector =
          (BigIntVector)
              vector.addOrGetVector(FieldType.nullable(MinorType.BIGINT.getType())).getVector();
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
    try (final LargeListViewVector vector = LargeListViewVector.empty("list", allocator)) {

      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      // set some values
      writer.startListView();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endListView();
      vector.setValueCount(2);

      Field expectedDataField =
          new Field(
              BaseLargeRepeatedValueViewVector.DATA_VECTOR_NAME,
              FieldType.nullable(new ArrowType.Int(32, true)),
              null);
      Field expectedField =
          new Field(
              vector.getName(),
              FieldType.nullable(ArrowType.LargeListView.INSTANCE),
              Collections.singletonList(expectedDataField));

      assertEquals(expectedField, writer.getField());
    }
  }

  @Test
  public void testClose() throws Exception {
    try (final LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {

      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      // set some values
      writer.startListView();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endListView();
      vector.setValueCount(2);

      assertTrue(vector.getBufferSize() > 0);
      assertTrue(vector.getDataVector().getBufferSize() > 0);

      writer.close();
      assertEquals(0, vector.getBufferSize());
      assertEquals(0, vector.getDataVector().getBufferSize());

      vector.validate();
    }
  }

  @Test
  public void testGetBufferSizeFor() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {

      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      // set some values
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
        int offsetBufferSize = valueCount * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH;
        int sizeBufferSize = valueCount * BaseLargeRepeatedValueViewVector.SIZE_WIDTH;

        int expectedSize =
            validityBufferSize
                + offsetBufferSize
                + sizeBufferSize
                + dataVector.getBufferSizeFor(indices[valueCount]);
        assertEquals(expectedSize, vector.getBufferSizeFor(valueCount));
      }
      vector.validate();
    }
  }

  @Test
  public void testIsEmpty() {
    try (final LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      UnionLargeListViewWriter writer = vector.getWriter();
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

      vector.validate();
    }
  }

  @Test
  public void testTotalCapacity() {
    final FieldType type = FieldType.nullable(MinorType.INT.getType());
    try (final LargeListViewVector vector =
        new LargeListViewVector("largelistview", allocator, type, null)) {
      // Force the child vector to be allocated based on the type
      // (this is a bad API: we have to track and repeat the type twice)
      vector.addOrGetVector(type);

      // Specify the allocation size but do not actually allocate
      vector.setInitialTotalCapacity(10, 100);

      // Finally, actually do the allocation
      vector.allocateNewSafe();

      // Note: allocator rounds up and can be greater than the requested allocation.
      assertTrue(vector.getValueCapacity() >= 10);
      assertTrue(vector.getDataVector().getValueCapacity() >= 100);
    }
  }

  @Test
  public void testSetNull1() {
    try (LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(10);
      writer.bigInt().writeBigInt(20);
      writer.endListView();

      vector.setNull(1);

      writer.setPosition(2);
      writer.startListView();
      writer.bigInt().writeBigInt(30);
      writer.bigInt().writeBigInt(40);
      writer.endListView();

      vector.setNull(3);
      vector.setNull(4);

      writer.setPosition(5);
      writer.startListView();
      writer.bigInt().writeBigInt(50);
      writer.bigInt().writeBigInt(60);
      writer.endListView();

      vector.setValueCount(6);

      assertFalse(vector.isNull(0));
      assertTrue(vector.isNull(1));
      assertFalse(vector.isNull(2));
      assertTrue(vector.isNull(3));
      assertTrue(vector.isNull(4));
      assertFalse(vector.isNull(5));

      // validate buffers

      final ArrowBuf validityBuffer = vector.getValidityBuffer();
      final ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = vector.getSizeBuffer();

      assertEquals(1, BitVectorHelper.get(validityBuffer, 0));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 1));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 2));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 3));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 4));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 5));

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(4, offsetBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(2, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // validate values

      Object result = vector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(10), resultSet.get(0));
      assertEquals(Long.valueOf(20), resultSet.get(1));

      result = vector.getObject(2);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(30), resultSet.get(0));
      assertEquals(Long.valueOf(40), resultSet.get(1));

      result = vector.getObject(5);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(60), resultSet.get(1));

      vector.validate();
    }
  }

  @Test
  public void testSetNull2() {
    try (LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      // validate setting nulls first and then writing values
      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      vector.setNull(0);
      vector.setNull(2);
      vector.setNull(4);

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(10);
      writer.bigInt().writeBigInt(20);
      writer.bigInt().writeBigInt(30);
      writer.endListView();

      writer.setPosition(3);
      writer.startListView();
      writer.bigInt().writeBigInt(40);
      writer.bigInt().writeBigInt(50);
      writer.endListView();

      writer.setPosition(5);
      writer.startListView();
      writer.bigInt().writeBigInt(60);
      writer.bigInt().writeBigInt(70);
      writer.bigInt().writeBigInt(80);
      writer.endListView();

      vector.setValueCount(6);

      assertTrue(vector.isNull(0));
      assertFalse(vector.isNull(1));
      assertTrue(vector.isNull(2));
      assertFalse(vector.isNull(3));
      assertTrue(vector.isNull(4));
      assertFalse(vector.isNull(5));

      // validate buffers

      final ArrowBuf validityBuffer = vector.getValidityBuffer();
      final ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = vector.getSizeBuffer();

      assertEquals(0, BitVectorHelper.get(validityBuffer, 0));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 1));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 2));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 3));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 4));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 5));

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(0, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // validate values

      Object result = vector.getObject(1);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(10), resultSet.get(0));
      assertEquals(Long.valueOf(20), resultSet.get(1));
      assertEquals(Long.valueOf(30), resultSet.get(2));

      result = vector.getObject(3);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(40), resultSet.get(0));
      assertEquals(Long.valueOf(50), resultSet.get(1));

      result = vector.getObject(5);
      resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(60), resultSet.get(0));
      assertEquals(Long.valueOf(70), resultSet.get(1));
      assertEquals(Long.valueOf(80), resultSet.get(2));

      vector.validate();
    }
  }

  @Test
  public void testSetNull3() {
    try (LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      // validate setting values first and then writing nulls
      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(10);
      writer.bigInt().writeBigInt(20);
      writer.bigInt().writeBigInt(30);
      writer.endListView();

      writer.setPosition(3);
      writer.startListView();
      writer.bigInt().writeBigInt(40);
      writer.bigInt().writeBigInt(50);
      writer.endListView();

      writer.setPosition(5);
      writer.startListView();
      writer.bigInt().writeBigInt(60);
      writer.bigInt().writeBigInt(70);
      writer.bigInt().writeBigInt(80);
      writer.endListView();

      vector.setNull(0);
      vector.setNull(2);
      vector.setNull(4);

      vector.setValueCount(6);

      assertTrue(vector.isNull(0));
      assertFalse(vector.isNull(1));
      assertTrue(vector.isNull(2));
      assertFalse(vector.isNull(3));
      assertTrue(vector.isNull(4));
      assertFalse(vector.isNull(5));

      // validate buffers

      final ArrowBuf validityBuffer = vector.getValidityBuffer();
      final ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = vector.getSizeBuffer();

      assertEquals(0, BitVectorHelper.get(validityBuffer, 0));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 1));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 2));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 3));
      assertEquals(0, BitVectorHelper.get(validityBuffer, 4));
      assertEquals(1, BitVectorHelper.get(validityBuffer, 5));

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(0, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(5 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // validate values

      Object result = vector.getObject(1);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(10), resultSet.get(0));
      assertEquals(Long.valueOf(20), resultSet.get(1));
      assertEquals(Long.valueOf(30), resultSet.get(2));

      result = vector.getObject(3);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(40), resultSet.get(0));
      assertEquals(Long.valueOf(50), resultSet.get(1));

      result = vector.getObject(5);
      resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(60), resultSet.get(0));
      assertEquals(Long.valueOf(70), resultSet.get(1));
      assertEquals(Long.valueOf(80), resultSet.get(2));

      vector.validate();
    }
  }

  @Test
  public void testOverWrite1() {
    try (LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(10);
      writer.bigInt().writeBigInt(20);
      writer.bigInt().writeBigInt(30);
      writer.endListView();

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(40);
      writer.bigInt().writeBigInt(50);
      writer.endListView();

      vector.setValueCount(2);

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(60);
      writer.bigInt().writeBigInt(70);
      writer.endListView();

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(80);
      writer.bigInt().writeBigInt(90);
      writer.endListView();

      vector.setValueCount(2);

      Object result = vector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(60), resultSet.get(0));
      assertEquals(Long.valueOf(70), resultSet.get(1));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(80), resultSet.get(0));
      assertEquals(Long.valueOf(90), resultSet.get(1));

      vector.validate();
    }
  }

  @Test
  public void testOverwriteWithNull() {
    try (LargeListViewVector vector = LargeListViewVector.empty("largelistview", allocator)) {
      UnionLargeListViewWriter writer = vector.getWriter();
      writer.allocate();

      ArrowBuf offsetBuffer = vector.getOffsetBuffer();
      ArrowBuf sizeBuffer = vector.getSizeBuffer();

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(10);
      writer.bigInt().writeBigInt(20);
      writer.bigInt().writeBigInt(30);
      writer.endListView();

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(40);
      writer.bigInt().writeBigInt(50);
      writer.endListView();

      vector.setValueCount(2);

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      vector.setNull(0);

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      vector.setNull(1);

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      assertTrue(vector.isNull(0));
      assertTrue(vector.isNull(1));

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(60);
      writer.bigInt().writeBigInt(70);
      writer.endListView();

      assertEquals(0, offsetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(80);
      writer.bigInt().writeBigInt(90);
      writer.endListView();

      assertEquals(2, offsetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      vector.setValueCount(2);

      assertFalse(vector.isNull(0));
      assertFalse(vector.isNull(1));

      Object result = vector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(60), resultSet.get(0));
      assertEquals(Long.valueOf(70), resultSet.get(1));

      result = vector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(80), resultSet.get(0));
      assertEquals(Long.valueOf(90), resultSet.get(1));

      vector.validate();
    }
  }

  @Test
  public void testOutOfOrderOffset1() {
    // [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
    try (LargeListViewVector largeListViewVector =
        LargeListViewVector.empty("largelistview", allocator)) {
      // Allocate buffers in largeListViewVector by calling `allocateNew` method.
      largeListViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.

      FieldType fieldType = new FieldType(true, new ArrowType.Int(16, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      largeListViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = largeListViewVector.getDataVector();
      fieldVector.clear();

      SmallIntVector childVector = (SmallIntVector) fieldVector;

      childVector.allocateNew(7);

      childVector.set(0, 0);
      childVector.set(1, -127);
      childVector.set(2, 127);
      childVector.set(3, 50);
      childVector.set(4, 12);
      childVector.set(5, -7);
      childVector.set(6, 25);

      childVector.setValueCount(7);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.
      largeListViewVector.setValidity(0, 1);
      largeListViewVector.setValidity(1, 0);
      largeListViewVector.setValidity(2, 1);
      largeListViewVector.setValidity(3, 1);
      largeListViewVector.setValidity(4, 1);

      largeListViewVector.setOffset(0, 4);
      largeListViewVector.setOffset(1, 7);
      largeListViewVector.setOffset(2, 0);
      largeListViewVector.setOffset(3, 0);
      largeListViewVector.setOffset(4, 3);

      largeListViewVector.setSize(0, 3);
      largeListViewVector.setSize(1, 0);
      largeListViewVector.setSize(2, 4);
      largeListViewVector.setSize(3, 0);
      largeListViewVector.setSize(4, 2);

      // Set value count using `setValueCount` method.
      largeListViewVector.setValueCount(5);

      final ArrowBuf offSetBuffer = largeListViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = largeListViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(4, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // check child vector
      assertEquals(0, ((SmallIntVector) largeListViewVector.getDataVector()).get(0));
      assertEquals(-127, ((SmallIntVector) largeListViewVector.getDataVector()).get(1));
      assertEquals(127, ((SmallIntVector) largeListViewVector.getDataVector()).get(2));
      assertEquals(50, ((SmallIntVector) largeListViewVector.getDataVector()).get(3));
      assertEquals(12, ((SmallIntVector) largeListViewVector.getDataVector()).get(4));
      assertEquals(-7, ((SmallIntVector) largeListViewVector.getDataVector()).get(5));
      assertEquals(25, ((SmallIntVector) largeListViewVector.getDataVector()).get(6));

      // check values
      Object result = largeListViewVector.getObject(0);
      ArrayList<Integer> resultSet = (ArrayList<Integer>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Short.valueOf("12"), resultSet.get(0));
      assertEquals(Short.valueOf("-7"), resultSet.get(1));
      assertEquals(Short.valueOf("25"), resultSet.get(2));

      assertTrue(largeListViewVector.isNull(1));

      result = largeListViewVector.getObject(2);
      resultSet = (ArrayList<Integer>) result;
      assertEquals(4, resultSet.size());
      assertEquals(Short.valueOf("0"), resultSet.get(0));
      assertEquals(Short.valueOf("-127"), resultSet.get(1));
      assertEquals(Short.valueOf("127"), resultSet.get(2));
      assertEquals(Short.valueOf("50"), resultSet.get(3));

      assertTrue(largeListViewVector.isEmpty(3));

      result = largeListViewVector.getObject(4);
      resultSet = (ArrayList<Integer>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Short.valueOf("50"), resultSet.get(0));
      assertEquals(Short.valueOf("12"), resultSet.get(1));

      largeListViewVector.validate();
    }
  }

  private int validateSizeBufferAndCalculateMinOffset(
      int start,
      int splitLength,
      ArrowBuf fromOffsetBuffer,
      ArrowBuf fromSizeBuffer,
      ArrowBuf toSizeBuffer) {
    int minOffset = fromOffsetBuffer.getInt((long) start * LargeListViewVector.OFFSET_WIDTH);
    int fromDataLength;
    int toDataLength;

    for (int i = 0; i < splitLength; i++) {
      fromDataLength = fromSizeBuffer.getInt((long) (start + i) * LargeListViewVector.SIZE_WIDTH);
      toDataLength = toSizeBuffer.getInt((long) (i) * LargeListViewVector.SIZE_WIDTH);

      /* validate size */
      assertEquals(
          fromDataLength,
          toDataLength,
          "Different data lengths at index: " + i + " and start: " + start);

      /* calculate minimum offset */
      int currentOffset =
          fromOffsetBuffer.getInt((long) (start + i) * LargeListViewVector.OFFSET_WIDTH);
      if (currentOffset < minOffset) {
        minOffset = currentOffset;
      }
    }

    return minOffset;
  }

  private void validateOffsetBuffer(
      int start,
      int splitLength,
      ArrowBuf fromOffsetBuffer,
      ArrowBuf toOffsetBuffer,
      int minOffset) {
    int offset1;
    int offset2;

    for (int i = 0; i < splitLength; i++) {
      offset1 = fromOffsetBuffer.getInt((long) (start + i) * LargeListViewVector.OFFSET_WIDTH);
      offset2 = toOffsetBuffer.getInt((long) (i) * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(
          offset1 - minOffset,
          offset2,
          "Different offset values at index: " + i + " and start: " + start);
    }
  }

  private void validateDataBuffer(
      int start,
      int splitLength,
      ArrowBuf fromOffsetBuffer,
      ArrowBuf fromSizeBuffer,
      BigIntVector fromDataVector,
      ArrowBuf toOffsetBuffer,
      BigIntVector toDataVector) {
    int dataLength;
    Long fromValue;
    for (int i = 0; i < splitLength; i++) {
      dataLength = fromSizeBuffer.getInt((long) (start + i) * LargeListViewVector.SIZE_WIDTH);
      for (int j = 0; j < dataLength; j++) {
        fromValue =
            fromDataVector.getObject(
                (fromOffsetBuffer.getInt((long) (start + i) * LargeListViewVector.OFFSET_WIDTH)
                    + j));
        Long toValue =
            toDataVector.getObject(
                (toOffsetBuffer.getInt((long) i * LargeListViewVector.OFFSET_WIDTH) + j));
        assertEquals(
            fromValue, toValue, "Different data values at index: " + i + " and start: " + start);
      }
    }
  }

  /**
   * Validate split and transfer of data from fromVector to toVector. Note that this method assumes
   * that the child vector is BigIntVector.
   *
   * @param start start index
   * @param splitLength length of data to split and transfer
   * @param fromVector fromVector
   * @param toVector toVector
   */
  private void validateSplitAndTransfer(
      TransferPair transferPair,
      int start,
      int splitLength,
      LargeListViewVector fromVector,
      LargeListViewVector toVector) {

    transferPair.splitAndTransfer(start, splitLength);

    /* get offsetBuffer of toVector */
    final ArrowBuf toOffsetBuffer = toVector.getOffsetBuffer();

    /* get sizeBuffer of toVector */
    final ArrowBuf toSizeBuffer = toVector.getSizeBuffer();

    /* get dataVector of toVector */
    BigIntVector toDataVector = (BigIntVector) toVector.getDataVector();

    /* get offsetBuffer of toVector */
    final ArrowBuf fromOffsetBuffer = fromVector.getOffsetBuffer();

    /* get sizeBuffer of toVector */
    final ArrowBuf fromSizeBuffer = fromVector.getSizeBuffer();

    /* get dataVector of toVector */
    BigIntVector fromDataVector = (BigIntVector) fromVector.getDataVector();

    /* validate size buffers */
    int minOffset =
        validateSizeBufferAndCalculateMinOffset(
            start, splitLength, fromOffsetBuffer, fromSizeBuffer, toSizeBuffer);
    /* validate offset buffers */
    validateOffsetBuffer(start, splitLength, fromOffsetBuffer, toOffsetBuffer, minOffset);
    /* validate data */
    validateDataBuffer(
        start,
        splitLength,
        fromOffsetBuffer,
        fromSizeBuffer,
        fromDataVector,
        toOffsetBuffer,
        toDataVector);
  }

  @Test
  public void testSplitAndTransfer() throws Exception {
    try (LargeListViewVector fromVector = LargeListViewVector.empty("sourceVector", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      fromVector.addOrGetVector(FieldType.nullable(type.getType()));

      UnionLargeListViewWriter listViewWriter = fromVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* populate data */
      listViewWriter.setPosition(0);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(10);
      listViewWriter.bigInt().writeBigInt(11);
      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.endListView();

      listViewWriter.setPosition(1);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(13);
      listViewWriter.bigInt().writeBigInt(14);
      listViewWriter.endListView();

      listViewWriter.setPosition(2);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(15);
      listViewWriter.bigInt().writeBigInt(16);
      listViewWriter.bigInt().writeBigInt(17);
      listViewWriter.bigInt().writeBigInt(18);
      listViewWriter.endListView();

      listViewWriter.setPosition(3);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(19);
      listViewWriter.endListView();

      listViewWriter.setPosition(4);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(20);
      listViewWriter.bigInt().writeBigInt(21);
      listViewWriter.bigInt().writeBigInt(22);
      listViewWriter.bigInt().writeBigInt(23);
      listViewWriter.endListView();

      fromVector.setValueCount(5);

      /* get offset buffer */
      final ArrowBuf offsetBuffer = fromVector.getOffsetBuffer();

      /* get size buffer */
      final ArrowBuf sizeBuffer = fromVector.getSizeBuffer();

      /* get dataVector */
      BigIntVector dataVector = (BigIntVector) fromVector.getDataVector();

      /* check the vector output */

      int index = 0;
      int offset;
      int size = 0;
      Long actual;

      /* index 0 */
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(0), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(10), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(11), actual);
      offset++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(12), actual);
      assertEquals(
          Integer.toString(3),
          Integer.toString(sizeBuffer.getInt(index * LargeListViewVector.SIZE_WIDTH)));

      /* index 1 */
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(3), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(13), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(14), actual);
      size++;
      assertEquals(
          Integer.toString(size),
          Integer.toString(sizeBuffer.getInt(index * LargeListViewVector.SIZE_WIDTH)));

      /* index 2 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(5), Integer.toString(offset));
      size++;

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(15), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(16), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(17), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(18), actual);
      assertEquals(
          Integer.toString(size),
          Integer.toString(sizeBuffer.getInt(index * LargeListViewVector.SIZE_WIDTH)));

      /* index 3 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(9), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(19), actual);
      size++;
      assertEquals(
          Integer.toString(size),
          Integer.toString(sizeBuffer.getInt(index * LargeListViewVector.SIZE_WIDTH)));

      /* index 4 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * LargeListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(10), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(20), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(21), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(22), actual);
      offset++;
      size++;
      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(23), actual);
      size++;
      assertEquals(
          Integer.toString(size),
          Integer.toString(sizeBuffer.getInt(index * LargeListViewVector.SIZE_WIDTH)));

      /* do split and transfer */
      try (LargeListViewVector toVector = LargeListViewVector.empty("toVector", allocator)) {
        int[][] transferLengths = {{0, 2}, {3, 1}, {4, 1}};
        TransferPair transferPair = fromVector.makeTransferPair(toVector);

        for (final int[] transferLength : transferLengths) {
          int start = transferLength[0];
          int splitLength = transferLength[1];
          validateSplitAndTransfer(transferPair, start, splitLength, fromVector, toVector);
        }
      }
    }
  }

  @Test
  public void testGetTransferPairWithField() throws Exception {
    try (final LargeListViewVector fromVector = LargeListViewVector.empty("listview", allocator)) {

      UnionLargeListViewWriter writer = fromVector.getWriter();
      writer.allocate();

      // set some values
      writer.startListView();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endListView();
      fromVector.setValueCount(2);

      final TransferPair transferPair =
          fromVector.getTransferPair(fromVector.getField(), allocator);
      final LargeListViewVector toVector = (LargeListViewVector) transferPair.getTo();
      // Field inside a new vector created by reusing a field should be the same in memory as the
      // original field.
      assertSame(toVector.getField(), fromVector.getField());
    }
  }

  @Test
  public void testOutOfOrderOffsetSplitAndTransfer() {
    // [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
    try (LargeListViewVector fromVector = LargeListViewVector.empty("fromVector", allocator)) {
      // Allocate buffers in LargeListViewVector by calling `allocateNew` method.
      fromVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.

      FieldType fieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      fromVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = fromVector.getDataVector();
      fieldVector.clear();

      BigIntVector childVector = (BigIntVector) fieldVector;

      childVector.allocateNew(7);

      childVector.set(0, 0);
      childVector.set(1, -127);
      childVector.set(2, 127);
      childVector.set(3, 50);
      childVector.set(4, 12);
      childVector.set(5, -7);
      childVector.set(6, 25);

      childVector.setValueCount(7);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.
      fromVector.setValidity(0, 1);
      fromVector.setValidity(1, 0);
      fromVector.setValidity(2, 1);
      fromVector.setValidity(3, 1);
      fromVector.setValidity(4, 1);

      fromVector.setOffset(0, 4);
      fromVector.setOffset(1, 7);
      fromVector.setOffset(2, 0);
      fromVector.setOffset(3, 0);
      fromVector.setOffset(4, 3);

      fromVector.setSize(0, 3);
      fromVector.setSize(1, 0);
      fromVector.setSize(2, 4);
      fromVector.setSize(3, 0);
      fromVector.setSize(4, 2);

      // Set value count using `setValueCount` method.
      fromVector.setValueCount(5);

      final ArrowBuf offSetBuffer = fromVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = fromVector.getSizeBuffer();

      // check offset buffer
      assertEquals(4, offSetBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(4 * BaseLargeRepeatedValueViewVector.SIZE_WIDTH));

      // check child vector
      assertEquals(0, ((BigIntVector) fromVector.getDataVector()).get(0));
      assertEquals(-127, ((BigIntVector) fromVector.getDataVector()).get(1));
      assertEquals(127, ((BigIntVector) fromVector.getDataVector()).get(2));
      assertEquals(50, ((BigIntVector) fromVector.getDataVector()).get(3));
      assertEquals(12, ((BigIntVector) fromVector.getDataVector()).get(4));
      assertEquals(-7, ((BigIntVector) fromVector.getDataVector()).get(5));
      assertEquals(25, ((BigIntVector) fromVector.getDataVector()).get(6));

      // check values
      Object result = fromVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(12), resultSet.get(0));
      assertEquals(Long.valueOf(-7), resultSet.get(1));
      assertEquals(Long.valueOf(25), resultSet.get(2));

      assertTrue(fromVector.isNull(1));

      result = fromVector.getObject(2);
      resultSet = (ArrayList<Long>) result;
      assertEquals(4, resultSet.size());
      assertEquals(Long.valueOf(0), resultSet.get(0));
      assertEquals(Long.valueOf(-127), resultSet.get(1));
      assertEquals(Long.valueOf(127), resultSet.get(2));
      assertEquals(Long.valueOf(50), resultSet.get(3));

      assertTrue(fromVector.isEmpty(3));

      result = fromVector.getObject(4);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(12), resultSet.get(1));

      fromVector.validate();

      /* do split and transfer */
      try (LargeListViewVector toVector = LargeListViewVector.empty("toVector", allocator)) {
        int[][] transferLengths = {{2, 3}, {0, 1}, {0, 3}};
        TransferPair transferPair = fromVector.makeTransferPair(toVector);

        for (final int[] transferLength : transferLengths) {
          int start = transferLength[0];
          int splitLength = transferLength[1];
          validateSplitAndTransfer(transferPair, start, splitLength, fromVector, toVector);
        }
      }
    }
  }

  private void writeIntValues(UnionLargeListViewWriter writer, int[] values) {
    writer.startListView();
    for (int v : values) {
      writer.integer().writeInt(v);
    }
    writer.endListView();
  }
}
