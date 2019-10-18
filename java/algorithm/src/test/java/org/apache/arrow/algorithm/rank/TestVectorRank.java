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

package org.apache.arrow.algorithm.rank;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link org.apache.arrow.algorithm.rank.VectorRank}.
 */
public class TestVectorRank {

  private BufferAllocator allocator;

  private static final int VECTOR_LENGTH = 10;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  @Test
  public void testFixedWidthRank() {
    VectorRank<IntVector> rank = new VectorRank<>(allocator);
    try (IntVector vector = new IntVector("int vec", allocator)) {
      vector.allocateNew(VECTOR_LENGTH);
      vector.setValueCount(VECTOR_LENGTH);

      vector.set(0, 1);
      vector.set(1, 5);
      vector.set(2, 3);
      vector.set(3, 7);
      vector.set(4, 9);
      vector.set(5, 8);
      vector.set(6, 2);
      vector.set(7, 0);
      vector.set(8, 4);
      vector.set(9, 6);

      VectorValueComparator<IntVector> comparator =
              DefaultVectorComparators.createDefaultComparator(vector);
      assertEquals(7, rank.indexAtRank(vector, comparator, 0));
      assertEquals(0, rank.indexAtRank(vector, comparator, 1));
      assertEquals(6, rank.indexAtRank(vector, comparator, 2));
      assertEquals(2, rank.indexAtRank(vector, comparator, 3));
      assertEquals(8, rank.indexAtRank(vector, comparator, 4));
      assertEquals(1, rank.indexAtRank(vector, comparator, 5));
      assertEquals(9, rank.indexAtRank(vector, comparator, 6));
      assertEquals(3, rank.indexAtRank(vector, comparator, 7));
      assertEquals(5, rank.indexAtRank(vector, comparator, 8));
      assertEquals(4, rank.indexAtRank(vector, comparator, 9));
    }
  }

  @Test
  public void testVariableWidthRank() {
    VectorRank<VarCharVector> rank = new VectorRank<>(allocator);
    try (VarCharVector vector = new VarCharVector("varchar vec", allocator)) {
      vector.allocateNew(VECTOR_LENGTH * 5, VECTOR_LENGTH);
      vector.setValueCount(VECTOR_LENGTH);

      vector.set(0, String.valueOf(1).getBytes());
      vector.set(1, String.valueOf(5).getBytes());
      vector.set(2, String.valueOf(3).getBytes());
      vector.set(3, String.valueOf(7).getBytes());
      vector.set(4, String.valueOf(9).getBytes());
      vector.set(5, String.valueOf(8).getBytes());
      vector.set(6, String.valueOf(2).getBytes());
      vector.set(7, String.valueOf(0).getBytes());
      vector.set(8, String.valueOf(4).getBytes());
      vector.set(9, String.valueOf(6).getBytes());

      VectorValueComparator<VarCharVector> comparator =
              DefaultVectorComparators.createDefaultComparator(vector);

      assertEquals(7, rank.indexAtRank(vector, comparator, 0));
      assertEquals(0, rank.indexAtRank(vector, comparator, 1));
      assertEquals(6, rank.indexAtRank(vector, comparator, 2));
      assertEquals(2, rank.indexAtRank(vector, comparator, 3));
      assertEquals(8, rank.indexAtRank(vector, comparator, 4));
      assertEquals(1, rank.indexAtRank(vector, comparator, 5));
      assertEquals(9, rank.indexAtRank(vector, comparator, 6));
      assertEquals(3, rank.indexAtRank(vector, comparator, 7));
      assertEquals(5, rank.indexAtRank(vector, comparator, 8));
      assertEquals(4, rank.indexAtRank(vector, comparator, 9));
    }
  }

  @Test
  public void testRankNegative() {
    VectorRank<IntVector> rank = new VectorRank<>(allocator);
    try (IntVector vector = new IntVector("int vec", allocator)) {
      vector.allocateNew(VECTOR_LENGTH);
      vector.setValueCount(VECTOR_LENGTH);

      vector.set(0, 1);
      vector.set(1, 5);
      vector.set(2, 3);
      vector.set(3, 7);
      vector.set(4, 9);
      vector.set(5, 8);
      vector.set(6, 2);
      vector.set(7, 0);
      vector.set(8, 4);
      vector.set(9, 6);

      VectorValueComparator<IntVector> comparator =
              DefaultVectorComparators.createDefaultComparator(vector);

      assertThrows(IllegalArgumentException.class, () -> {
        rank.indexAtRank(vector, comparator, VECTOR_LENGTH + 1);
      });
    }
  }
}
