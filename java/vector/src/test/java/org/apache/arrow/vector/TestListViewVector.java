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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
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
  public void testNestedListVector() {
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

      listWriter.bigInt().writeBigInt(12);
      listWriter.bigInt().writeBigInt(-7);
      listWriter.bigInt().writeBigInt(25);
      listWriter.endList();

      listWriter.setValueCount(1);
      listVector.setValueCount(1);

      System.out.println(listVector);
    }

  }

  @Test
  public void testBasicListViewVector() {
    try (ListViewVector listViewVector = ListViewVector.empty("sourceVector", allocator)) {
      UnionListViewWriter listViewWriter = listViewVector.getWriter();

      /* allocate memory */
      listViewWriter.allocate();

      /* write the first list at index 0 */
      listViewWriter.setPosition(0);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(12);
      listViewWriter.bigInt().writeBigInt(-7);
      listViewWriter.bigInt().writeBigInt(25);
      listViewWriter.endList();

      /* the second list at index 2 is null (we are not setting any)*/

      /* write the third list at index 2 */
      listViewWriter.setPosition(2);
      listViewWriter.startList();

      listViewWriter.bigInt().writeBigInt(0);
      listViewWriter.bigInt().writeBigInt(-127);
      listViewWriter.bigInt().writeBigInt(127);
      listViewWriter.bigInt().writeBigInt(50);
      listViewWriter.endList();

      /* write the fourth list at index 3 (empty list) */
      listViewWriter.setPosition(3);
      listViewWriter.startList();
      listViewWriter.endList();

      listViewWriter.setPosition(4);
      listViewWriter.startList();
      listViewWriter.bigInt().writeBigInt(1);
      listViewWriter.bigInt().writeBigInt(2);
      listViewWriter.bigInt().writeBigInt(3);
      listViewWriter.bigInt().writeBigInt(4);
      listViewWriter.endList();

      // assertEquals(3, listViewVector.getLastSet());

      listViewVector.setValueCount(5);
      // assertEquals(4, listViewVector.getValueCount());
      /* get vector at index 0 -- the value is a BigIntVector*/
      ArrowBuf offSetBuffer = listViewVector.getOffsetBuffer();
      ArrowBuf sizeBuffer = listViewVector.getSizeBuffer();
      FieldVector dataVec = listViewVector.getDataVector();

      for (int i = 0; i < dataVec.getValueCount(); i++) {
        Object o1 = dataVec.getObject(i);
        System.out.println(i + " : " + o1);
      }

      for (int i = 0; i < 5; i++) {
        System.out.println("Index: " + i + " Offset: " + offSetBuffer.getInt(i * 4) +
                " Size: " + sizeBuffer.getInt(i * 4));
      }

      System.out.println(listViewVector);
    }
  }
}
