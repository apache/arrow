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

package org.apache.arrow.algorithm.move;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestVectorDataCopier {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testCopyVariableWidthData() {
    try (VarCharVector srcVector = new VarCharVector("", allocator);
         VarCharVector dstVector = new VarCharVector("", allocator)) {

      srcVector.allocateNew(100, 10);
      srcVector.setValueCount(10);

      dstVector.allocateNew(100, 10);
      dstVector.setValueCount(10);

      // fill source data
      srcVector.set(0, "hello".getBytes());
      srcVector.set(1, "abc".getBytes());
      srcVector.setNull(2);
      srcVector.set(3, "".getBytes());

      // copy data
      VectorDataCopier.variableWidthDataCopy(srcVector, 1, dstVector, 0);
      VectorDataCopier.variableWidthDataCopy(srcVector, 3, dstVector, 1);
      VectorDataCopier.variableWidthDataCopy(srcVector, 2, dstVector, 2);
      VectorDataCopier.variableWidthDataCopy(srcVector, 0, dstVector, 3);

      // verify results
      assertArrayEquals(srcVector.get(1), dstVector.get(0));
      assertArrayEquals(srcVector.get(3), dstVector.get(1));
      assertTrue(dstVector.isNull(2));
      assertArrayEquals(srcVector.get(0), dstVector.get(3));
    }
  }

  @Test
  public void testCopyFixedWidthData() {
    try (IntVector srcVector = new IntVector("src", allocator);
         IntVector dstVector = new IntVector("dst", allocator)) {

      srcVector.allocateNew(10);
      srcVector.setValueCount(10);

      dstVector.allocateNew(10);
      dstVector.setValueCount(10);

      // fill source data
      srcVector.set(0, 0);
      srcVector.set(1, 111);
      srcVector.setNull(2);
      srcVector.set(3, 333);

      // copy data
      VectorDataCopier.fixedWidthDataCopy(srcVector, 1, dstVector, 0, IntVector.TYPE_WIDTH);
      VectorDataCopier.fixedWidthDataCopy(srcVector, 3, dstVector, 1, IntVector.TYPE_WIDTH);
      VectorDataCopier.fixedWidthDataCopy(srcVector, 2, dstVector, 2, IntVector.TYPE_WIDTH);
      VectorDataCopier.fixedWidthDataCopy(srcVector, 0, dstVector, 3, IntVector.TYPE_WIDTH);

      // verify results
      assertEquals(srcVector.get(1), dstVector.get(0));
      assertEquals(srcVector.get(3), dstVector.get(1));
      assertTrue(dstVector.isNull(2));
      assertEquals(srcVector.get(0), dstVector.get(3));
    }
  }
}
