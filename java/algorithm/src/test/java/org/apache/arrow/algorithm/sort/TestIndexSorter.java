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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for {@link IndexSorter}. */
public class TestIndexSorter {

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
  public void testIndexSort() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      ValueVectorDataPopulator.setVector(vec, 11, 8, 33, 10, 12, 17, null, 23, 35, 2);

      // sort the index
      IndexSorter<IntVector> indexSorter = new IndexSorter<>();
      DefaultVectorComparators.IntComparator intComparator =
          new DefaultVectorComparators.IntComparator();
      intComparator.attachVector(vec);

      IntVector indices = new IntVector("", allocator);
      indices.setValueCount(10);
      indexSorter.sort(vec, indices, intComparator);

      int[] expected = new int[] {6, 9, 1, 3, 0, 4, 5, 7, 2, 8};

      for (int i = 0; i < expected.length; i++) {
        assertTrue(!indices.isNull(i));
        assertEquals(expected[i], indices.get(i));
      }
      indices.close();
    }
  }

  /**
   * Tests the worst case for quick sort. It may cause stack overflow if the algorithm is
   * implemented as a recursive algorithm.
   */
  @Test
  public void testSortLargeIncreasingInt() {
    final int vectorLength = 20000;
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(vectorLength);
      vec.setValueCount(vectorLength);

      // fill data to sort
      for (int i = 0; i < vectorLength; i++) {
        vec.set(i, i);
      }

      // sort the vector
      IndexSorter<IntVector> indexSorter = new IndexSorter<>();
      DefaultVectorComparators.IntComparator intComparator =
          new DefaultVectorComparators.IntComparator();
      intComparator.attachVector(vec);

      try (IntVector indices = new IntVector("", allocator)) {
        indices.setValueCount(vectorLength);
        indexSorter.sort(vec, indices, intComparator);

        for (int i = 0; i < vectorLength; i++) {
          assertTrue(!indices.isNull(i));
          assertEquals(i, indices.get(i));
        }
      }
    }
  }

  @Test
  public void testChoosePivot() {
    final int vectorLength = 100;
    try (IntVector vec = new IntVector("vector", allocator);
        IntVector indices = new IntVector("indices", allocator)) {
      vec.allocateNew(vectorLength);
      indices.allocateNew(vectorLength);

      // the vector is sorted, so the pivot should be in the middle
      for (int i = 0; i < vectorLength; i++) {
        vec.set(i, i * 100);
        indices.set(i, i);
      }
      vec.setValueCount(vectorLength);
      indices.setValueCount(vectorLength);

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      // setup internal data structures
      comparator.attachVector(vec);

      int low = 5;
      int high = 6;
      assertTrue(high - low + 1 < FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD);

      // the range is small enough, so the pivot is simply selected as the low value
      int pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(pivotIndex, low);
      assertEquals(pivotIndex, indices.get(low));

      low = 30;
      high = 80;
      assertTrue(high - low + 1 >= FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD);

      // the range is large enough, so the median is selected as the pivot
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(pivotIndex, (low + high) / 2);
      assertEquals(pivotIndex, indices.get(low));
    }
  }

  /** Evaluates choosing pivot for all possible permutations of 3 numbers. */
  @Test
  public void testChoosePivotAllPermutes() {
    try (IntVector vec = new IntVector("vector", allocator);
        IntVector indices = new IntVector("indices", allocator)) {
      vec.allocateNew();
      indices.allocateNew();

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      // setup internal data structures
      comparator.attachVector(vec);
      int low = 0;
      int high = 2;

      // test all the 6 permutations of 3 numbers
      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 11, 22, 33);
      int pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(1, pivotIndex);
      assertEquals(1, indices.get(low));

      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 11, 33, 22);
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(2, pivotIndex);
      assertEquals(2, indices.get(low));

      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 22, 11, 33);
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(0, pivotIndex);
      assertEquals(0, indices.get(low));

      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 22, 33, 11);
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(0, pivotIndex);
      assertEquals(0, indices.get(low));

      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 33, 11, 22);
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(2, pivotIndex);
      assertEquals(2, indices.get(low));

      ValueVectorDataPopulator.setVector(indices, 0, 1, 2);
      ValueVectorDataPopulator.setVector(vec, 33, 22, 11);
      pivotIndex = IndexSorter.choosePivot(low, high, indices, comparator);
      assertEquals(1, pivotIndex);
      assertEquals(1, indices.get(low));
    }
  }
}
