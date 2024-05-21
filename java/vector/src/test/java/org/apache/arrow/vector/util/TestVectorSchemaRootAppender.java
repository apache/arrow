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

package org.apache.arrow.vector.util;

import static junit.framework.TestCase.assertEquals;
import static org.apache.arrow.vector.util.TestVectorAppender.assertVectorsEqual;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorSchemaRootAppender}.
 */
public class TestVectorSchemaRootAppender {

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
  public void testVectorSchemaRootAppend() {
    final int length1 = 5;
    final int length2 = 3;
    final int length3 = 2;

    try (IntVector targetChild1 = new IntVector("t1", allocator);
         VarCharVector targetChild2 = new VarCharVector("t2", allocator);
         BigIntVector targetChild3 = new BigIntVector("t3", allocator);

         IntVector deltaChildOne1 = new IntVector("do1", allocator);
         VarCharVector deltaChildOne2 = new VarCharVector("do2", allocator);
         BigIntVector deltaChildOne3 = new BigIntVector("do3", allocator);

         IntVector deltaChildTwo1 = new IntVector("dt1", allocator);
         VarCharVector deltaChildTwo2 = new VarCharVector("dt2", allocator);
         BigIntVector deltaChildTwo3 = new BigIntVector("dt3", allocator)) {

      ValueVectorDataPopulator.setVector(targetChild1, 0, 1, null, 3, 4);
      ValueVectorDataPopulator.setVector(targetChild2, "zero", "one", null, "three", "four");
      ValueVectorDataPopulator.setVector(targetChild3, 0L, 10L, null, 30L, 40L);
      VectorSchemaRoot root1 = VectorSchemaRoot.of(targetChild1, targetChild2, targetChild3);
      root1.setRowCount(length1);

      ValueVectorDataPopulator.setVector(deltaChildOne1, 5, 6, 7);
      ValueVectorDataPopulator.setVector(deltaChildOne2, "five", "six", "seven");
      ValueVectorDataPopulator.setVector(deltaChildOne3, 50L, 60L, 70L);
      VectorSchemaRoot root2 = VectorSchemaRoot.of(deltaChildOne1, deltaChildOne2, deltaChildOne3);
      root2.setRowCount(length2);

      ValueVectorDataPopulator.setVector(deltaChildTwo1, null, 9);
      ValueVectorDataPopulator.setVector(deltaChildTwo2, null, "nine");
      ValueVectorDataPopulator.setVector(deltaChildTwo3, null, 90L);
      VectorSchemaRoot root3 = VectorSchemaRoot.of(deltaChildTwo1, deltaChildTwo2, deltaChildTwo3);
      root3.setRowCount(length3);

      VectorSchemaRootAppender.append(root1, root2, root3);
      assertEquals(length1 + length2 + length3, root1.getRowCount());
      assertEquals(3, root1.getFieldVectors().size());

      try (IntVector expected1 = new IntVector("", allocator);
           VarCharVector expected2 = new VarCharVector("", allocator);
           BigIntVector expected3 = new BigIntVector("", allocator)) {

        ValueVectorDataPopulator.setVector(expected1, 0, 1, null, 3, 4, 5, 6, 7, null, 9);
        ValueVectorDataPopulator.setVector(
            expected2, "zero", "one", null, "three", "four", "five", "six", "seven", null, "nine");
        ValueVectorDataPopulator.setVector(expected3, 0L, 10L, null, 30L, 40L, 50L, 60L, 70L, null, 90L);

        assertVectorsEqual(expected1, root1.getVector(0));
        assertVectorsEqual(expected2, root1.getVector(1));
        assertVectorsEqual(expected3, root1.getVector(2));
      }
    }
  }

  @Test
  public void testRootWithDifferentChildCounts() {
    try (IntVector targetChild1 = new IntVector("t1", allocator);
         VarCharVector targetChild2 = new VarCharVector("t2", allocator);
         BigIntVector targetChild3 = new BigIntVector("t3", allocator);

         IntVector deltaChild1 = new IntVector("d1", allocator);
         VarCharVector deltaChild2 = new VarCharVector("d2", allocator)) {

      ValueVectorDataPopulator.setVector(targetChild1, 0, 1, null, 3, 4);
      ValueVectorDataPopulator.setVector(targetChild2, "zero", "one", null, "three", "four");
      ValueVectorDataPopulator.setVector(targetChild3, 0L, 10L, null, 30L, 40L);
      VectorSchemaRoot root1 = VectorSchemaRoot.of(targetChild1, targetChild2, targetChild3);
      root1.setRowCount(5);

      ValueVectorDataPopulator.setVector(deltaChild1, 5, 6, 7);
      ValueVectorDataPopulator.setVector(deltaChild2, "five", "six", "seven");
      VectorSchemaRoot root2 = VectorSchemaRoot.of(deltaChild1, deltaChild2);
      root2.setRowCount(3);

      IllegalArgumentException exp = assertThrows(IllegalArgumentException.class,
          () -> VectorSchemaRootAppender.append(root1, root2));

      assertEquals("Vector schema roots have different numbers of child vectors.", exp.getMessage());
    }
  }

  @Test
  public void testRootWithDifferentChildTypes() {
    try (IntVector targetChild1 = new IntVector("t1", allocator);
         VarCharVector targetChild2 = new VarCharVector("t2", allocator);

         IntVector deltaChild1 = new IntVector("d1", allocator);
         VarCharVector deltaChild2 = new VarCharVector("d2", allocator)) {

      ValueVectorDataPopulator.setVector(targetChild1, 0, 1, null, 3, 4);
      ValueVectorDataPopulator.setVector(targetChild2, "zero", "one", null, "three", "four");
      VectorSchemaRoot root1 = VectorSchemaRoot.of(targetChild1, targetChild2);
      root1.setRowCount(5);

      ValueVectorDataPopulator.setVector(deltaChild1, 5, 6, 7);
      ValueVectorDataPopulator.setVector(deltaChild2, "five", "six", "seven");

      // note that the child vectors are in reverse order
      VectorSchemaRoot root2 = VectorSchemaRoot.of(deltaChild2, deltaChild1);
      root2.setRowCount(3);

      IllegalArgumentException exp = assertThrows(IllegalArgumentException.class,
          () -> VectorSchemaRootAppender.append(root1, root2));

      assertEquals("Vector schema roots have different schemas.", exp.getMessage());
    }
  }
}
