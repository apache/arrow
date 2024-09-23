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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueViewVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.holders.DurationHolder;
import org.apache.arrow.vector.holders.TimeStampMilliTZHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestListViewVector {

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
  public void testBasicListViewVector() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* write the first list at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(-7);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endListView();

      /* the second list at index 1 is null (we are not setting any)*/

      /* write the third list at index 2 */
      listViewWriter.setPosition(2);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(0);
      listViewWriter.bigInt().writeBigInt(-127);
      listViewWriter.bigInt().writeBigInt(127);
      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.endListView();

      /* write the fourth list at index 3 (empty list) */
      listViewWriter.setPosition(3);
      listViewWriter.startListView();
      listViewWriter.endListView();

      /* write the fifth list at index 4 */
      listViewWriter.setPosition(4);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(1);
      listViewWriter.bigInt().writeBigInt(2);
      listViewWriter.bigInt().writeBigInt(3);
      listViewWriter.bigInt().writeBigInt(4);
      listViewWriter.endListView();

      listViewWriter.setValueCount(5);
      // check value count
      assertEquals(5, listViewVector.getValueCount());

      /* get vector at index 0 -- the value is a BigIntVector*/
      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();
      final FieldVector dataVec = listViewVector.getDataVector();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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

      listViewVector.validate();
    }
  }

  @Test
  public void testImplicitNullVectors() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();
      /* allocate memory */
      listViewWriter.allocate();

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      /* write the first list at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(-7);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endListView();

      int offSet0 = offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size0 = sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH);

      // after the first list is written,
      // the initial offset must be 0,
      // the size must be 3 (as there are 3 elements in the array),
      // the lastSet must be 0 since, the first list is written at index 0.

      assertEquals(0, offSet0);
      assertEquals(3, size0);

      listViewWriter.setPosition(5);
      listViewWriter.startListView();

      // writing the 6th list at index 5,
      // and the list items from index 1 through 4 are not populated.
      // but since there is a gap between the 0th and 5th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.

      for (int i = 1; i < 5; i++) {
        int offSet = offSetBuffer.getInt(i * BaseRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseRepeatedValueViewVector.SIZE_WIDTH);
        // Since the list is not written, the offset and size must equal to child vector's size
        // i.e., 3, and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 5.
        assertEquals(0, offSet);
        assertEquals(0, size);
      }

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endListView();

      int offSet5 = offSetBuffer.getInt(5 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size5 = sizeBuffer.getInt(5 * BaseRepeatedValueViewVector.SIZE_WIDTH);

      assertEquals(3, offSet5);
      assertEquals(2, size5);

      listViewWriter.setPosition(10);
      listViewWriter.startListView();

      // writing the 11th list at index 10,
      // and the list items from index 6 through 10 are not populated.
      // but since there is a gap between the 5th and 11th list, in terms
      // of buffer allocation, the offset and size buffers must be updated
      // to reflect the implicit null vectors.
      for (int i = 6; i < 10; i++) {
        int offSet = offSetBuffer.getInt(i * BaseRepeatedValueViewVector.OFFSET_WIDTH);
        int size = sizeBuffer.getInt(i * BaseRepeatedValueViewVector.SIZE_WIDTH);
        // Since the list is not written, the offset and size must equal to 0
        // and size should be 0 as the list is not written.
        // And the last set value is the value currently being written, which is 10.
        assertEquals(0, offSet);
        assertEquals(0, size);
      }

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.endListView();

      int offSet11 = offSetBuffer.getInt(10 * BaseRepeatedValueViewVector.OFFSET_WIDTH);
      int size11 = sizeBuffer.getInt(10 * BaseRepeatedValueViewVector.SIZE_WIDTH);

      assertEquals(5, offSet11);
      assertEquals(1, size11);

      listViewVector.setValueCount(11);

      listViewVector.validate();
    }
  }

  @Test
  public void testNestedListViewVector() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* the dataVector that backs a listVector will also be a
       * listVector for this test.
       */

      /* write one or more inner lists at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startListView();

      listViewWriter.listView().startListView();
      listViewWriter.listView().bigInt().writeBigInt(50);
      listViewWriter.listView().bigInt().writeBigInt(100);
      listViewWriter.listView().bigInt().writeBigInt(200);
      listViewWriter.listView().endListView();

      listViewWriter.listView().startListView();
      listViewWriter.listView().bigInt().writeBigInt(75);
      listViewWriter.listView().bigInt().writeBigInt(125);
      listViewWriter.listView().bigInt().writeBigInt(150);
      listViewWriter.listView().bigInt().writeBigInt(175);
      listViewWriter.listView().endListView();

      listViewWriter.endListView();

      /* write one or more inner lists at index 1 */
      listViewWriter.setPosition(1);
      listViewWriter.startListView();

      listViewWriter.listView().startListView();
      listViewWriter.listView().bigInt().writeBigInt(10);
      listViewWriter.listView().endListView();

      listViewWriter.listView().startListView();
      listViewWriter.listView().bigInt().writeBigInt(15);
      listViewWriter.listView().bigInt().writeBigInt(20);
      listViewWriter.listView().endListView();

      listViewWriter.listView().startListView();
      listViewWriter.listView().bigInt().writeBigInt(25);
      listViewWriter.listView().bigInt().writeBigInt(30);
      listViewWriter.listView().bigInt().writeBigInt(35);
      listViewWriter.listView().endListView();

      listViewWriter.endListView();

      listViewVector.setValueCount(2);

      // [[[50,100,200],[75,125,150,175]], [[10],[15,20],[25,30,35]]]

      assertEquals(2, listViewVector.getValueCount());

      /* get listViewVector value at index 0 -- the value itself is a listViewVector */
      Object result = listViewVector.getObject(0);
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

      /* get listViewVector value at index 1 -- the value itself is a listViewVector */
      result = listViewVector.getObject(1);
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
      assertFalse(listViewVector.isNull(0));
      assertFalse(listViewVector.isNull(1));

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      listViewVector.validate();
    }
  }

  @Test
  public void testNestedListVector() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {

      MinorType listType = MinorType.LISTVIEW;
      MinorType scalarType = MinorType.BIGINT;

      listViewVector.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList1 = (ListViewVector) listViewVector.getDataVector();
      innerList1.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList2 = (ListViewVector) innerList1.getDataVector();
      innerList2.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList3 = (ListViewVector) innerList2.getDataVector();
      innerList3.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList4 = (ListViewVector) innerList3.getDataVector();
      innerList4.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList5 = (ListViewVector) innerList4.getDataVector();
      innerList5.addOrGetVector(FieldType.nullable(listType.getType()));

      ListViewVector innerList6 = (ListViewVector) innerList5.getDataVector();
      innerList6.addOrGetVector(FieldType.nullable(scalarType.getType()));

      listViewVector.setInitialCapacity(128);

      listViewVector.validate();
    }
  }

  private void setValuesInBuffer(int[] bufValues, ArrowBuf buffer, long bufWidth) {
    for (int i = 0; i < bufValues.length; i++) {
      buffer.setInt(i * bufWidth, bufValues[i]);
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
  public void testBasicListViewSet() {

    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.
      FieldType fieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
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
      listViewVector.setOffset(0, 0);
      listViewVector.setOffset(1, 3);
      listViewVector.setOffset(2, 3);
      listViewVector.setOffset(3, 7);

      listViewVector.setSize(0, 3);
      listViewVector.setSize(1, 0);
      listViewVector.setSize(2, 4);
      listViewVector.setSize(3, 0);

      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 0);
      listViewVector.setValidity(2, 1);
      listViewVector.setValidity(3, 1);

      // Set value count using `setValueCount` method.
      listViewVector.setValueCount(4);

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      // check values
      assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
      assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
      assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
      assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
      assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
      assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
      assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));

      listViewVector.validate();
    }
  }

  @Test
  public void testBasicListViewSetNested() {
    // Expected listview
    // [[[50,100,200],[75,125,150,175]],[[10],[15,20],[25,30,35]]]

    // Setting child vector
    // [[50,100,200],[75,125,150,175],[10],[15,20],[25,30,35]]
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.
      FieldType fieldType = new FieldType(true, new ArrowType.ListView(), null, null);
      FieldType childFieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field childField = new Field("child-vector", childFieldType, null);
      List<Field> children = new ArrayList<>();
      children.add(childField);
      Field field = new Field("child-vector", fieldType, children);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
      fieldVector.clear();

      ListViewVector childVector = (ListViewVector) fieldVector;
      UnionListViewWriter listViewWriter = childVector.getWriter();
      listViewWriter.allocate();

      listViewWriter.setPosition(0);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.bigInt().writeBigInt(100);
      listViewWriter.bigInt().writeBigInt(200);

      listViewWriter.endListView();

      listViewWriter.setPosition(1);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(75);
      listViewWriter.bigInt().writeBigInt(125);
      listViewWriter.bigInt().writeBigInt(150);
      listViewWriter.bigInt().writeBigInt(175);

      listViewWriter.endListView();

      listViewWriter.setPosition(2);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(10);

      listViewWriter.endListView();

      listViewWriter.startListView();
      listViewWriter.setPosition(3);

      listViewWriter.bigInt().writeBigInt(15);
      listViewWriter.bigInt().writeBigInt(20);

      listViewWriter.endListView();

      listViewWriter.startListView();
      listViewWriter.setPosition(4);

      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.bigInt().writeBigInt(30);
      listViewWriter.bigInt().writeBigInt(35);

      listViewWriter.endListView();

      childVector.setValueCount(5);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.

      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 1);

      listViewVector.setOffset(0, 0);
      listViewVector.setOffset(1, 2);

      listViewVector.setSize(0, 2);
      listViewVector.setSize(1, 3);

      // Set value count using `setValueCount` method.
      listViewVector.setValueCount(2);

      assertEquals(2, listViewVector.getValueCount());

      /* get listViewVector value at index 0 -- the value itself is a listViewVector */
      Object result = listViewVector.getObject(0);
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

      /* get listViewVector value at index 1 -- the value itself is a listViewVector */
      result = listViewVector.getObject(1);
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
      assertFalse(listViewVector.isNull(0));
      assertFalse(listViewVector.isNull(1));

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      listViewVector.validate();
    }
  }

  @Test
  public void testBasicListViewSetWithListViewWriter() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.
      FieldType fieldType = new FieldType(true, new ArrowType.Int(64, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
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

      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 0);
      listViewVector.setValidity(2, 1);
      listViewVector.setValidity(3, 1);

      listViewVector.setOffset(0, 0);
      listViewVector.setOffset(1, 3);
      listViewVector.setOffset(2, 3);
      listViewVector.setOffset(3, 7);

      listViewVector.setSize(0, 3);
      listViewVector.setSize(1, 0);
      listViewVector.setSize(2, 4);
      listViewVector.setSize(3, 0);

      // Set value count using `setValueCount` method.
      listViewVector.setValueCount(4);

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      // check values
      assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
      assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
      assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
      assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
      assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
      assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
      assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));

      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      listViewWriter.setPosition(4);
      listViewWriter.startListView();

      listViewWriter.bigInt().writeBigInt(121);
      listViewWriter.bigInt().writeBigInt(-71);
      listViewWriter.bigInt().writeBigInt(251);
      listViewWriter.endListView();

      listViewVector.setValueCount(5);

      // check offset buffer
      assertEquals(0, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      // check values
      assertEquals(12, ((BigIntVector) listViewVector.getDataVector()).get(0));
      assertEquals(-7, ((BigIntVector) listViewVector.getDataVector()).get(1));
      assertEquals(25, ((BigIntVector) listViewVector.getDataVector()).get(2));
      assertEquals(0, ((BigIntVector) listViewVector.getDataVector()).get(3));
      assertEquals(-127, ((BigIntVector) listViewVector.getDataVector()).get(4));
      assertEquals(127, ((BigIntVector) listViewVector.getDataVector()).get(5));
      assertEquals(50, ((BigIntVector) listViewVector.getDataVector()).get(6));
      assertEquals(121, ((BigIntVector) listViewVector.getDataVector()).get(7));
      assertEquals(-71, ((BigIntVector) listViewVector.getDataVector()).get(8));
      assertEquals(251, ((BigIntVector) listViewVector.getDataVector()).get(9));

      listViewVector.validate();
    }
  }

  @Test
  public void testGetBufferAddress() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("vector", allocator)) {

      UnionListViewWriter listViewWriter = listViewVector.getWriter();
      boolean error = false;

      listViewWriter.allocate();

      listViewWriter.setPosition(0);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.bigInt().writeBigInt(100);
      listViewWriter.bigInt().writeBigInt(200);
      listViewWriter.endListView();

      listViewWriter.setPosition(1);
      listViewWriter.startListView();
      listViewWriter.bigInt().writeBigInt(250);
      listViewWriter.bigInt().writeBigInt(300);
      listViewWriter.endListView();

      listViewVector.setValueCount(2);

      /* check listVector contents */
      Object result = listViewVector.getObject(0);
      ArrayList<Long> resultSet = (ArrayList<Long>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Long.valueOf(50), resultSet.get(0));
      assertEquals(Long.valueOf(100), resultSet.get(1));
      assertEquals(Long.valueOf(200), resultSet.get(2));

      result = listViewVector.getObject(1);
      resultSet = (ArrayList<Long>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Long.valueOf(250), resultSet.get(0));
      assertEquals(Long.valueOf(300), resultSet.get(1));

      List<ArrowBuf> buffers = listViewVector.getFieldBuffers();

      long bitAddress = listViewVector.getValidityBufferAddress();
      long offsetAddress = listViewVector.getOffsetBufferAddress();
      long sizeAddress = listViewVector.getSizeBufferAddress();

      try {
        listViewVector.getDataBufferAddress();
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
      assertEquals(2.5, listViewVector.getDensity(), 0);
      listViewVector.validate();
    }
  }

  @Test
  public void testConsistentChildName() throws Exception {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      String emptyListStr = listViewVector.getField().toString();
      assertTrue(emptyListStr.contains(ListVector.DATA_VECTOR_NAME));

      listViewVector.addOrGetVector(FieldType.nullable(MinorType.INT.getType()));
      String emptyVectorStr = listViewVector.getField().toString();
      assertTrue(emptyVectorStr.contains(ListVector.DATA_VECTOR_NAME));
    }
  }

  @Test
  public void testSetInitialCapacity() {
    try (final ListViewVector vector = ListViewVector.empty("", allocator)) {
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
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
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

      vector.validate();
    }
  }

  @Test
  public void testWriterGetField() {
    // adopted from ListVector test cases
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
      writer.allocate();

      // set some values
      writer.startListView();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.endListView();
      vector.setValueCount(2);

      Field expectedDataField =
          new Field(
              BaseRepeatedValueVector.DATA_VECTOR_NAME,
              FieldType.nullable(new ArrowType.Int(32, true)),
              null);
      Field expectedField =
          new Field(
              vector.getName(),
              FieldType.nullable(ArrowType.ListView.INSTANCE),
              Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());

      vector.validate();
    }
  }

  @Test
  public void testWriterUsingHolderGetTimestampMilliTZField() {
    // adopted from ListVector test cases
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      org.apache.arrow.vector.complex.writer.FieldWriter writer = vector.getWriter();
      writer.allocate();

      TimeStampMilliTZHolder holder = new TimeStampMilliTZHolder();
      holder.timezone = "SomeFakeTimeZone";
      writer.startListView();
      holder.value = 12341234L;
      writer.timeStampMilliTZ().write(holder);
      holder.value = 55555L;
      writer.timeStampMilliTZ().write(holder);

      // Writing with a different timezone should throw
      holder.timezone = "AsdfTimeZone";
      holder.value = 77777;
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class, () -> writer.timeStampMilliTZ().write(holder));
      assertEquals(
          "holder.timezone: AsdfTimeZone not equal to vector timezone: SomeFakeTimeZone",
          ex.getMessage());

      writer.endListView();
      vector.setValueCount(1);

      Field expectedDataField =
          new Field(
              BaseRepeatedValueVector.DATA_VECTOR_NAME,
              FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "SomeFakeTimeZone")),
              null);
      Field expectedField =
          new Field(
              vector.getName(),
              FieldType.nullable(ArrowType.ListView.INSTANCE),
              Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());

      vector.validate();
    }
  }

  @Test
  public void testWriterGetDurationField() {
    // adopted from ListVector test cases
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      org.apache.arrow.vector.complex.writer.FieldWriter writer = vector.getWriter();
      writer.allocate();

      DurationHolder durationHolder = new DurationHolder();
      durationHolder.unit = TimeUnit.MILLISECOND;

      writer.startListView();
      durationHolder.value = 812374L;
      writer.duration().write(durationHolder);
      durationHolder.value = 143451L;
      writer.duration().write(durationHolder);

      // Writing with a different unit should throw
      durationHolder.unit = TimeUnit.SECOND;
      durationHolder.value = 8888888;
      IllegalArgumentException ex =
          assertThrows(
              IllegalArgumentException.class, () -> writer.duration().write(durationHolder));
      assertEquals("holder.unit: SECOND not equal to vector unit: MILLISECOND", ex.getMessage());

      writer.endListView();
      vector.setValueCount(1);

      Field expectedDataField =
          new Field(
              BaseRepeatedValueVector.DATA_VECTOR_NAME,
              FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND)),
              null);
      Field expectedField =
          new Field(
              vector.getName(),
              FieldType.nullable(ArrowType.ListView.INSTANCE),
              Arrays.asList(expectedDataField));

      assertEquals(expectedField, writer.getField());

      vector.validate();
    }
  }

  @Test
  public void testClose() throws Exception {
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
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
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {

      UnionListViewWriter writer = vector.getWriter();
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
        int offsetBufferSize = valueCount * BaseRepeatedValueViewVector.OFFSET_WIDTH;
        int sizeBufferSize = valueCount * BaseRepeatedValueViewVector.SIZE_WIDTH;

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
    try (final ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      UnionListViewWriter writer = vector.getWriter();
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
    // adopted from ListVector test cases
    final FieldType type = FieldType.nullable(MinorType.INT.getType());
    try (final ListViewVector vector = new ListViewVector("listview", allocator, type, null)) {
      // Force the child vector to be allocated based on the type
      // (this is a bad API: we have to track and repeat the type twice)
      vector.addOrGetVector(type);

      // Specify the allocation size but do not allocate
      vector.setInitialTotalCapacity(10, 100);

      // Finally, actually do the allocation
      vector.allocateNewSafe();

      // Note: allocator rounds up and can be greater than the requested allocation.
      assertTrue(vector.getValueCapacity() >= 10);
      assertTrue(vector.getDataVector().getValueCapacity() >= 100);

      vector.validate();
    }
  }

  @Test
  public void testSetNull1() {
    try (ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      UnionListViewWriter writer = vector.getWriter();
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

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, offsetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(4, offsetBuffer.getInt(5 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(5 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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
    try (ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      // validate setting nulls first and then writing values
      UnionListViewWriter writer = vector.getWriter();
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

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(5 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(0, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(5 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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
    try (ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      // validate setting values first and then writing nulls
      UnionListViewWriter writer = vector.getWriter();
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

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offsetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(5, offsetBuffer.getInt(5 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(0, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(3, sizeBuffer.getInt(5 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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
    try (ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      UnionListViewWriter writer = vector.getWriter();
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
    try (ListViewVector vector = ListViewVector.empty("listview", allocator)) {
      UnionListViewWriter writer = vector.getWriter();
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

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offsetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      vector.setNull(0);

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      vector.setNull(1);

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      assertTrue(vector.isNull(0));
      assertTrue(vector.isNull(1));

      writer.setPosition(0);
      writer.startListView();
      writer.bigInt().writeBigInt(60);
      writer.bigInt().writeBigInt(70);
      writer.endListView();

      assertEquals(0, offsetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      writer.setPosition(1);
      writer.startListView();
      writer.bigInt().writeBigInt(80);
      writer.bigInt().writeBigInt(90);
      writer.endListView();

      assertEquals(2, offsetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(2, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.

      FieldType fieldType = new FieldType(true, new ArrowType.Int(16, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
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
      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 0);
      listViewVector.setValidity(2, 1);
      listViewVector.setValidity(3, 1);
      listViewVector.setValidity(4, 1);

      listViewVector.setOffset(0, 4);
      listViewVector.setOffset(1, 7);
      listViewVector.setOffset(2, 0);
      listViewVector.setOffset(3, 0);
      listViewVector.setOffset(4, 3);

      listViewVector.setSize(0, 3);
      listViewVector.setSize(1, 0);
      listViewVector.setSize(2, 4);
      listViewVector.setSize(3, 0);
      listViewVector.setSize(4, 2);

      // Set value count using `setValueCount` method.
      listViewVector.setValueCount(5);

      final ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      final ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();

      // check offset buffer
      assertEquals(4, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

      // check child vector
      assertEquals(0, ((SmallIntVector) listViewVector.getDataVector()).get(0));
      assertEquals(-127, ((SmallIntVector) listViewVector.getDataVector()).get(1));
      assertEquals(127, ((SmallIntVector) listViewVector.getDataVector()).get(2));
      assertEquals(50, ((SmallIntVector) listViewVector.getDataVector()).get(3));
      assertEquals(12, ((SmallIntVector) listViewVector.getDataVector()).get(4));
      assertEquals(-7, ((SmallIntVector) listViewVector.getDataVector()).get(5));
      assertEquals(25, ((SmallIntVector) listViewVector.getDataVector()).get(6));

      // check values
      Object result = listViewVector.getObject(0);
      ArrayList<Integer> resultSet = (ArrayList<Integer>) result;
      assertEquals(3, resultSet.size());
      assertEquals(Short.valueOf("12"), resultSet.get(0));
      assertEquals(Short.valueOf("-7"), resultSet.get(1));
      assertEquals(Short.valueOf("25"), resultSet.get(2));

      assertTrue(listViewVector.isNull(1));

      result = listViewVector.getObject(2);
      resultSet = (ArrayList<Integer>) result;
      assertEquals(4, resultSet.size());
      assertEquals(Short.valueOf("0"), resultSet.get(0));
      assertEquals(Short.valueOf("-127"), resultSet.get(1));
      assertEquals(Short.valueOf("127"), resultSet.get(2));
      assertEquals(Short.valueOf("50"), resultSet.get(3));

      assertTrue(listViewVector.isEmpty(3));

      result = listViewVector.getObject(4);
      resultSet = (ArrayList<Integer>) result;
      assertEquals(2, resultSet.size());
      assertEquals(Short.valueOf("50"), resultSet.get(0));
      assertEquals(Short.valueOf("12"), resultSet.get(1));

      listViewVector.validate();
    }
  }

  private int validateSizeBufferAndCalculateMinOffset(
      int start,
      int splitLength,
      ArrowBuf fromOffsetBuffer,
      ArrowBuf fromSizeBuffer,
      ArrowBuf toSizeBuffer) {
    int minOffset = fromOffsetBuffer.getInt((long) start * ListViewVector.OFFSET_WIDTH);
    int fromDataLength;
    int toDataLength;

    for (int i = 0; i < splitLength; i++) {
      fromDataLength = fromSizeBuffer.getInt((long) (start + i) * ListViewVector.SIZE_WIDTH);
      toDataLength = toSizeBuffer.getInt((long) (i) * ListViewVector.SIZE_WIDTH);

      /* validate size */
      assertEquals(
          fromDataLength,
          toDataLength,
          "Different data lengths at index: " + i + " and start: " + start);

      /* calculate minimum offset */
      int currentOffset = fromOffsetBuffer.getInt((long) (start + i) * ListViewVector.OFFSET_WIDTH);
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
      offset1 = fromOffsetBuffer.getInt((long) (start + i) * ListViewVector.OFFSET_WIDTH);
      offset2 = toOffsetBuffer.getInt((long) (i) * ListViewVector.OFFSET_WIDTH);
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
      dataLength = fromSizeBuffer.getInt((long) (start + i) * ListViewVector.SIZE_WIDTH);
      for (int j = 0; j < dataLength; j++) {
        fromValue =
            fromDataVector.getObject(
                (fromOffsetBuffer.getInt((long) (start + i) * ListViewVector.OFFSET_WIDTH) + j));
        Long toValue =
            toDataVector.getObject(
                (toOffsetBuffer.getInt((long) i * ListViewVector.OFFSET_WIDTH) + j));
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
      ListViewVector fromVector,
      ListViewVector toVector) {

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
    try (ListViewVector fromVector = ListViewVector.empty("sourceVector", allocator)) {

      /* Explicitly add the dataVector */
      MinorType type = MinorType.BIGINT;
      fromVector.addOrGetVector(FieldType.nullable(type.getType()));

      UnionListViewWriter listViewWriter = fromVector.getWriter();

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
      offset = offsetBuffer.getInt(index * ListViewVector.OFFSET_WIDTH);
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
          Integer.toString(sizeBuffer.getInt(index * ListViewVector.SIZE_WIDTH)));

      /* index 1 */
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListViewVector.OFFSET_WIDTH);
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
          Integer.toString(sizeBuffer.getInt(index * ListViewVector.SIZE_WIDTH)));

      /* index 2 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListViewVector.OFFSET_WIDTH);
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
          Integer.toString(sizeBuffer.getInt(index * ListViewVector.SIZE_WIDTH)));

      /* index 3 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListViewVector.OFFSET_WIDTH);
      assertEquals(Integer.toString(9), Integer.toString(offset));

      actual = dataVector.getObject(offset);
      assertEquals(Long.valueOf(19), actual);
      size++;
      assertEquals(
          Integer.toString(size),
          Integer.toString(sizeBuffer.getInt(index * ListViewVector.SIZE_WIDTH)));

      /* index 4 */
      size = 0;
      index++;
      assertFalse(fromVector.isNull(index));
      offset = offsetBuffer.getInt(index * ListViewVector.OFFSET_WIDTH);
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
          Integer.toString(sizeBuffer.getInt(index * ListViewVector.SIZE_WIDTH)));

      /* do split and transfer */
      try (ListViewVector toVector = ListViewVector.empty("toVector", allocator)) {
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
  public void testOutOfOrderOffsetSplitAndTransfer() {
    // [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
    try (ListViewVector fromVector = ListViewVector.empty("fromVector", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
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
      assertEquals(4, offSetBuffer.getInt(0 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(7, offSetBuffer.getInt(1 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(2 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(0, offSetBuffer.getInt(3 * BaseRepeatedValueViewVector.OFFSET_WIDTH));
      assertEquals(3, offSetBuffer.getInt(4 * BaseRepeatedValueViewVector.OFFSET_WIDTH));

      // check size buffer
      assertEquals(3, sizeBuffer.getInt(0 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(1 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(4, sizeBuffer.getInt(2 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(0, sizeBuffer.getInt(3 * BaseRepeatedValueViewVector.SIZE_WIDTH));
      assertEquals(2, sizeBuffer.getInt(4 * BaseRepeatedValueViewVector.SIZE_WIDTH));

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
      try (ListViewVector toVector = ListViewVector.empty("toVector", allocator)) {
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

  @Test
  public void testRangeChildVector1() {
    /*
     * Non-overlapping ranges
     * offsets: [0, 2]
     * sizes: [4, 1]
     * values: [0, 1, 2, 3]
     *
     * vector: [[0, 1, 2, 3], [2]]
     * */
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.

      FieldType fieldType = new FieldType(true, new ArrowType.Int(32, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
      fieldVector.clear();

      IntVector childVector = (IntVector) fieldVector;

      childVector.allocateNew(8);

      childVector.set(0, 0);
      childVector.set(1, 1);
      childVector.set(2, 2);
      childVector.set(3, 3);
      childVector.set(4, 4);
      childVector.set(5, 5);
      childVector.set(6, 6);
      childVector.set(7, 7);

      childVector.setValueCount(8);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.
      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 1);

      listViewVector.setOffset(0, 0);
      listViewVector.setOffset(1, 2);

      listViewVector.setSize(0, 4);
      listViewVector.setSize(1, 1);

      assertEquals(8, listViewVector.getDataVector().getValueCount());

      listViewVector.setValueCount(2);
      assertEquals(4, listViewVector.getDataVector().getValueCount());

      IntVector childVector1 = (IntVector) listViewVector.getDataVector();
      final ArrowBuf dataBuffer = childVector1.getDataBuffer();
      final ArrowBuf validityBuffer = childVector1.getValidityBuffer();

      // yet the underneath buffer contains the original buffer
      for (int i = 0; i < validityBuffer.capacity(); i++) {
        assertEquals(i, dataBuffer.getInt((long) i * IntVector.TYPE_WIDTH));
      }
    }
  }

  @Test
  public void testRangeChildVector2() {
    /*
     * Overlapping ranges
     * offsets: [0, 2]
     * sizes: [3, 1]
     * values: [0, 1, 2, 3]
     *
     * vector: [[1, 2, 3], [2]]
     * */
    try (ListViewVector listViewVector = ListViewVector.empty("listview", allocator)) {
      // Allocate buffers in listViewVector by calling `allocateNew` method.
      listViewVector.allocateNew();

      // Initialize the child vector using `initializeChildrenFromFields` method.

      FieldType fieldType = new FieldType(true, new ArrowType.Int(32, true), null, null);
      Field field = new Field("child-vector", fieldType, null);
      listViewVector.initializeChildrenFromFields(Collections.singletonList(field));

      // Set values in the child vector.
      FieldVector fieldVector = listViewVector.getDataVector();
      fieldVector.clear();

      IntVector childVector = (IntVector) fieldVector;

      childVector.allocateNew(8);

      childVector.set(0, 0);
      childVector.set(1, 1);
      childVector.set(2, 2);
      childVector.set(3, 3);
      childVector.set(4, 4);
      childVector.set(5, 5);
      childVector.set(6, 6);
      childVector.set(7, 7);

      childVector.setValueCount(8);

      // Set validity, offset and size buffers using `setValidity`,
      //  `setOffset` and `setSize` methods.
      listViewVector.setValidity(0, 1);
      listViewVector.setValidity(1, 1);

      listViewVector.setOffset(0, 1);
      listViewVector.setOffset(1, 2);

      listViewVector.setSize(0, 3);
      listViewVector.setSize(1, 1);

      assertEquals(8, listViewVector.getDataVector().getValueCount());

      listViewVector.setValueCount(2);
      assertEquals(4, listViewVector.getDataVector().getValueCount());

      IntVector childVector1 = (IntVector) listViewVector.getDataVector();
      final ArrowBuf dataBuffer = childVector1.getDataBuffer();
      final ArrowBuf validityBuffer = childVector1.getValidityBuffer();

      // yet the underneath buffer contains the original buffer
      for (int i = 0; i < validityBuffer.capacity(); i++) {
        assertEquals(i, dataBuffer.getInt((long) i * IntVector.TYPE_WIDTH));
      }
    }
  }

  private void writeIntValues(UnionListViewWriter writer, int[] values) {
    writer.startListView();
    for (int v : values) {
      writer.integer().writeInt(v);
    }
    writer.endListView();
  }
}
