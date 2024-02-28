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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListViewVector;
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
  public void testOutOfOrderWithoutShareChild() throws Exception {
    // data to try to allocate: [[200, 400, 600], [12, -7, 25], [4, 6], [8, 127]]
    // values: [12, -7, 25, 0, -127, 200, 400, 600, 4, 6]
    // values group by: [[12,-7,25], [0,-127], [200,400,600], [4,6]]
    try (ListViewVector listViewVector = ListViewVector.empty("input", allocator);
         IntVector inVector =
             (IntVector) listViewVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                 .getVector()) {

      listViewVector.allocateNew();
      inVector.allocateNew();

      listViewVector.startNewValue(0, 3);
      inVector.setSafe(0, 12);
      inVector.setSafe(1, -7);
      inVector.setSafe(2, 25);
      listViewVector.endValue(0, 3);

      listViewVector.startNewValue(1, 8);
      inVector.setSafe(3, 0);
      inVector.setSafe(4, -127);
      listViewVector.endValue(1, 2);

      listViewVector.startNewValue(2, 0);
      inVector.setSafe(5, 200);
      inVector.setSafe(6, 400);
      inVector.setSafe(7, 600);
      listViewVector.endValue(2, 3);

      listViewVector.startNewValue(3, 6);
      inVector.setSafe(8, 4);
      inVector.setSafe(9, 6);
      listViewVector.endValue(3, 2);

      listViewVector.setValueCount(4);
      inVector.setValueCount(10);

      assertEquals(inVector.toString(), "[12, -7, 25, 0, -127, 200, 400, 600, 4, 6]");
      assertEquals(listViewVector.toString(), "[[12,-7,25], [0,-127], [200,400,600], [4,6]]");

    }
  }

  @Test
  public void testOutOfOrderWithShareChild() throws Exception {
    // data to try to allocate: [[12, -7, 25], [0, -127, 127, 50], [50, 12]]
    // values: [0, -127, 127, 50, 12, -7, 25]
    // values group by: [ [0, -127, 127, 50], [50, 12], [12, -7, 25]]
    try (ListViewVector listViewVector = ListViewVector.empty("input", allocator);
         IntVector inVector =
             (IntVector) listViewVector.addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                 .getVector()) {

      listViewVector.allocateNew();
      inVector.allocateNew();

      listViewVector.startNewValue(0, 3);
      inVector.setSafe(0, 0);
      inVector.setSafe(1, -127);
      inVector.setSafe(2, 127);
      inVector.setSafe(3, 50);
      listViewVector.endValue(0, 4);

      listViewVector.startNewValue(1, 6);
      inVector.setSafe(4, 12);
      listViewVector.endValue(1, 2);

      listViewVector.startNewValue(2, 8);
      inVector.setSafe(5, -7);
      inVector.setSafe(6, 25);
      listViewVector.endValue(2, 3);

      listViewVector.setValueCount(3);
      inVector.setValueCount(7);
      System.out.println("IntVector: \n" + inVector);
      System.out.println("ListViewVector: \n" + listViewVector);

      assertEquals(inVector.toString(), "[0, -127, 127, 50, 12, -7, 25]");
      // FIXME For borders, try to review sharing of child array values: 50 and 12 should be shared and reused
      assertEquals(listViewVector.toString(), "[[0,-127,127,50], [12,-7], [25,null,null]]");
    }
  }
}
