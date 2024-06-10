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
package org.apache.arrow.algorithm.sort;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for {@link InsertionSorter}. */
public class TestInsertionSorter {

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  private static final int VECTOR_LENGTH = 10;

  private void testSortIntVectorRange(int start, int end, int[] expected) {
    try (IntVector vector = new IntVector("vector", allocator);
        IntVector buffer = new IntVector("buffer", allocator)) {

      buffer.allocateNew(1);

      ValueVectorDataPopulator.setVector(vector, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
      assertEquals(VECTOR_LENGTH, vector.getValueCount());

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);
      InsertionSorter.insertionSort(vector, start, end, comparator, buffer);

      assertEquals(VECTOR_LENGTH, expected.length);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertFalse(vector.isNull(i));
        assertEquals(expected[i], vector.get(i));
      }
    }
  }

  @Test
  public void testSortIntVector() {
    testSortIntVectorRange(2, 5, new int[] {9, 8, 4, 5, 6, 7, 3, 2, 1, 0});
    testSortIntVectorRange(3, 7, new int[] {9, 8, 7, 2, 3, 4, 5, 6, 1, 0});
    testSortIntVectorRange(3, 4, new int[] {9, 8, 7, 5, 6, 4, 3, 2, 1, 0});
    testSortIntVectorRange(7, 7, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
    testSortIntVectorRange(0, 5, new int[] {4, 5, 6, 7, 8, 9, 3, 2, 1, 0});
    testSortIntVectorRange(8, 9, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 0, 1});
    testSortIntVectorRange(0, 9, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  }

  private void testSortIndicesRange(int start, int end, int[] expectedIndices) {
    try (IntVector vector = new IntVector("vector", allocator);
        IntVector indices = new IntVector("indices", allocator)) {

      ValueVectorDataPopulator.setVector(vector, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
      ValueVectorDataPopulator.setVector(indices, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

      assertEquals(VECTOR_LENGTH, vector.getValueCount());
      assertEquals(VECTOR_LENGTH, indices.getValueCount());

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);
      comparator.attachVector(vector);

      InsertionSorter.insertionSort(indices, start, end, comparator);

      // verify results
      assertEquals(VECTOR_LENGTH, expectedIndices.length);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertFalse(indices.isNull(i));
        assertEquals(expectedIndices[i], indices.get(i));
      }
    }
  }

  @Test
  public void testSortIndices() {
    testSortIndicesRange(2, 5, new int[] {0, 1, 5, 4, 3, 2, 6, 7, 8, 9});
    testSortIndicesRange(3, 7, new int[] {0, 1, 2, 7, 6, 5, 4, 3, 8, 9});
    testSortIndicesRange(3, 4, new int[] {0, 1, 2, 4, 3, 5, 6, 7, 8, 9});
    testSortIndicesRange(7, 7, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    testSortIndicesRange(0, 5, new int[] {5, 4, 3, 2, 1, 0, 6, 7, 8, 9});
    testSortIndicesRange(8, 9, new int[] {0, 1, 2, 3, 4, 5, 6, 7, 9, 8});
    testSortIndicesRange(0, 9, new int[] {9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
  }
}
