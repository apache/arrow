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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.BaseLargeRepeatedValueViewVector;
import org.apache.arrow.vector.complex.LargeListViewVector;
import org.apache.arrow.vector.complex.impl.UnionLargeListViewWriter;
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
}
