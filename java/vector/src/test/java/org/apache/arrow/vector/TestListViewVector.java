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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestListViewVector {
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
  public void testListView() throws Exception {
    // values = [12, -7, 25, 0, -127, 127, 50]
    // offsets = [0, 7, 3, 0]
    // sizes = [3, 0, 4, 0]
    // data to get thru listview: [[12,-7,25], null, [0,-127,127,50], []]
    try (ListViewVector listViewVector = ListViewVector.empty("input", allocator);
         IntVector inVector =
             (IntVector) listViewVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                 .getVector()) {

      listViewVector.allocateNew();
      inVector.allocateNew();

      inVector.setSafe(0, 12);
      inVector.setSafe(1, -7);
      inVector.setSafe(2, 25);
      inVector.setSafe(3, 0);
      inVector.setSafe(4, -127);
      inVector.setSafe(5, 127);
      inVector.setSafe(6, 50);

      listViewVector.startNewValue(0, 0);
      listViewVector.endValue(0, 3);

      listViewVector.setNull(1);

      listViewVector.startNewValue(2, 3);
      listViewVector.endValue(2, 4);

      listViewVector.startNewValue(3, 0);
      listViewVector.endValue(3, 0);

      listViewVector.setValueCount(4);

      assertEquals(inVector.toString(), "[12, -7, 25, 0, -127, 127, 50]");
      assertEquals(listViewVector.toString(), "[[12,-7,25], null, [0,-127,127,50], []]");
    }
  }

  @Test
  public void testListViewWithSharedValues() throws Exception {
    // values = [0, -127, 127, 50, 12, -7, 25]
    // offsets = [4, 7, 0, 0, 3]
    // sizes = [3, 0, 4, 0, 2]
    // data to get thru listview: [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
    // shared values = [50,]
    try (ListViewVector listViewVector = ListViewVector.empty("input", allocator);
         IntVector inVector =
             (IntVector) listViewVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                 .getVector()) {

      listViewVector.allocateNew();
      inVector.allocateNew();

      inVector.setSafe(0, 0);
      inVector.setSafe(1, -127);
      inVector.setSafe(2, 127);
      inVector.setSafe(3, 50);
      inVector.setSafe(4, 12);
      inVector.setSafe(5, -7);
      inVector.setSafe(6, 25);

      listViewVector.startNewValue(0, 4);
      listViewVector.endValue(0, 3);

      listViewVector.setNull(1);

      listViewVector.startNewValue(2, 0);
      listViewVector.endValue(2, 4);

      listViewVector.startNewValue(3, 0);
      listViewVector.endValue(3, 0);

      listViewVector.startNewValue(4, 3);
      listViewVector.endValue(4, 2);

      listViewVector.setValueCount(5);

      assertEquals(inVector.toString(), "[0, -127, 127, 50, 12, -7, 25]");
      assertEquals(listViewVector.toString(), "[[12,-7,25], null, [0,-127,127,50], [], [50,12]]");
    }
  }

  @Test
  public void testPartialListViewWithSharedValues() throws Exception {
    // values = [12, -7, 25, 17, -127, 200, 400, 600, 4, 6, 11, 0]
    // offsets = [2, 5]
    // sizes = [5, 3]
    // data to get thru listview: [[25,17,-127,200], [200,400,600]]
    // shared values = [200, 400, ]
    try (ListViewVector listViewVector = ListViewVector.empty("input", allocator);
         IntVector inVector =
             (IntVector) listViewVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                 .getVector()) {

      listViewVector.allocateNew();
      inVector.allocateNew();

      inVector.setSafe(0, 12);
      inVector.setSafe(1, -7);
      inVector.setSafe(2, 25);
      inVector.setSafe(3, 17);
      inVector.setSafe(4, -127);
      inVector.setSafe(5, 200);
      inVector.setSafe(6, 400);
      inVector.setSafe(7, 600);
      inVector.setSafe(8, 4);
      inVector.setSafe(9, 6);
      inVector.setSafe(10, 11);
      inVector.setSafe(11, 0);

      listViewVector.startNewValue(0, 2);
      listViewVector.endValue(0, 5);

      listViewVector.startNewValue(1, 5);
      listViewVector.endValue(1, 3);

      listViewVector.setValueCount(2);
      inVector.setValueCount(12);

      assertEquals(inVector.toString(), "[12, -7, 25, 17, -127, 200, 400, 600, 4, 6, 11, 0]");
      assertEquals(listViewVector.toString(), "[[25,17,-127,200,400], [200,400,600]]");
    }
  }

  @Test
  public void testListViewWriter() throws Exception {
    // values = [12, -7, 25, 0, -127, 127, 50]
    // offsets = [0, 7, 3, 0]
    // sizes = [3, 0, 4, 0]
    // data to get thru listview: [[12,-7,25], null, [0,-127,127,50], []]
    try (ListViewVector inVector = ListViewVector.empty("input", allocator)) {
      UnionListViewWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList(0);
      writer.bigInt().writeBigInt(12);
      writer.bigInt().writeBigInt(-7);
      writer.bigInt().writeBigInt(25);
      writer.endList(3);


      writer.setPosition(2);
      writer.startList(3);
      writer.bigInt().writeBigInt(0);
      writer.bigInt().writeBigInt(-127);
      writer.bigInt().writeBigInt(127);
      writer.bigInt().writeBigInt(50);
      writer.endList(4);

      writer.setPosition(3);
      writer.startList(0);
      writer.endList(0);

      writer.setValueCount(4);

      assertEquals(inVector.getDataVector().toString(), "[12, -7, 25, 0, -127, 127, 50]");
      assertEquals(inVector.toString(), "[[12,-7,25], null, [0,-127,127,50], []]");
    }
  }

  @Test
  public void testListViewWriterWithSharedValues() throws Exception {
    // values = [0, -127, 127, 50, 12, -7, 25]
    // offsets = [4, 7, 0, 0, 3]
    // sizes = [3, 0, 4, 0, 2]
    // data to get thru listview: [[12, -7, 25], null, [0, -127, 127, 50], [], [50, 12]]
    // shared values = [50,]
    try (ListViewVector inVector = ListViewVector.empty("input", allocator)) {
      UnionListViewWriter writer = inVector.getWriter();
      writer.allocate();

      writer.setPosition(0);
      writer.startList(4);
      writer.bigInt().writeBigInt(12);
      writer.bigInt().writeBigInt(-7);
      writer.bigInt().writeBigInt(25);
      writer.endList(3);


      writer.setPosition(2);
      writer.startList(0);
      writer.bigInt().writeBigInt(0);
      writer.bigInt().writeBigInt(-127);
      writer.bigInt().writeBigInt(127);
      writer.bigInt().writeBigInt(50);
      writer.endList(4);

      writer.setPosition(3);
      writer.startList(0);
      writer.endList(0);

      writer.setPosition(4);
      writer.startList(3);
      writer.bigInt().writeBigInt(50);
      writer.bigInt().writeBigInt(12);
      writer.endList(2);

      writer.setValueCount(5);

      assertEquals(inVector.getDataVector().toString(), "[0, -127, 127, 50, 12, -7, 25]");
      assertEquals(inVector.toString(), "[[12,-7,25], null, [0,-127,127,50], [], [50,12]]");
    }
  }
}
