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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.stream.IntStream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link FixedWidthInPlaceVectorSorter}.
 */
public class TestFixedWidthInPlaceVectorSorter {

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
  public void testSortInt() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10);
      vec.set(1, 8);
      vec.setNull(2);
      vec.set(3, 10);
      vec.set(4, 12);
      vec.set(5, 17);
      vec.setNull(6);
      vec.set(7, 23);
      vec.set(8, 35);
      vec.set(9, 2);

      // sort the vector
      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);

      sorter.sortInPlace(vec, comparator);

      // verify results
      Assert.assertEquals(10, vec.getValueCount());

      assertTrue(vec.isNull(0));
      assertTrue(vec.isNull(1));
      Assert.assertEquals(2, vec.get(2));
      Assert.assertEquals(8, vec.get(3));
      Assert.assertEquals(10, vec.get(4));
      Assert.assertEquals(10, vec.get(5));
      Assert.assertEquals(12, vec.get(6));
      Assert.assertEquals(17, vec.get(7));
      Assert.assertEquals(23, vec.get(8));
      Assert.assertEquals(35, vec.get(9));
    }
  }

  /**
   * Tests the worst case for quick sort.
   * It may cause stack overflow if the algorithm is implemented as a recursive algorithm.
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
      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);

      sorter.sortInPlace(vec, comparator);

      // verify results
      Assert.assertEquals(vectorLength, vec.getValueCount());

      for (int i = 0; i < vectorLength; i++) {
        Assert.assertEquals(i, vec.get(i));
      }
    }
  }

  @Test
  public void testChoosePivot() {
    final int vectorLength = 100;
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(vectorLength);

      // the vector is sorted, so the pivot should be in the middle
      for (int i = 0; i < vectorLength; i++) {
        vec.set(i, i * 100);
      }
      vec.setValueCount(vectorLength);

      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);

      try (IntVector pivotBuffer = (IntVector) vec.getField().createVector(allocator)) {
        // setup internal data structures
        pivotBuffer.allocateNew(1);
        sorter.pivotBuffer = pivotBuffer;
        sorter.comparator = comparator;
        sorter.vec = vec;
        comparator.attachVectors(vec, pivotBuffer);

        int low = 5;
        int high = 6;
        int pivotValue = vec.get(low);
        assertTrue(high - low + 1 < FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD);

        // the range is small enough, so the pivot is simply selected as the low value
        sorter.choosePivot(low, high);
        assertEquals(pivotValue, vec.get(low));

        low = 30;
        high = 80;
        pivotValue = vec.get((low + high) / 2);
        assertTrue(high - low + 1 >= FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD);

        // the range is large enough, so the median is selected as the pivot
        sorter.choosePivot(low, high);
        assertEquals(pivotValue, vec.get(low));
      }
    }
  }

  /**
   * Evaluates choosing pivot for all possible permutations of 3 numbers.
   */
  @Test
  public void testChoosePivotAllPermutes() {
    try (IntVector vec = new IntVector("", allocator)) {
      vec.allocateNew(3);

      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vec);

      try (IntVector pivotBuffer = (IntVector) vec.getField().createVector(allocator)) {
        // setup internal data structures
        pivotBuffer.allocateNew(1);
        sorter.pivotBuffer = pivotBuffer;
        sorter.comparator = comparator;
        sorter.vec = vec;
        comparator.attachVectors(vec, pivotBuffer);

        int low = 0;
        int high = 2;

        ValueVectorDataPopulator.setVector(vec, 11, 22, 33);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));

        ValueVectorDataPopulator.setVector(vec, 11, 33, 22);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));

        ValueVectorDataPopulator.setVector(vec, 22, 11, 33);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));

        ValueVectorDataPopulator.setVector(vec, 22, 33, 11);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));

        ValueVectorDataPopulator.setVector(vec, 33, 11, 22);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));

        ValueVectorDataPopulator.setVector(vec, 33, 22, 11);
        sorter.choosePivot(low, high);
        assertEquals(22, vec.get(0));
      }
    }
  }

  @Test
  public void testSortInt2() {
    try (IntVector vector = new IntVector("vector", allocator)) {
      ValueVectorDataPopulator.setVector(vector,
          0, 1, 2, 3, 4, 5, 30, 31, 32, 33,
          34, 35, 60, 61, 62, 63, 64, 65, 6, 7,
          8, 9, 10, 11, 36, 37, 38, 39, 40, 41,
          66, 67, 68, 69, 70, 71);

      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(vector);

      sorter.sortInPlace(vector, comparator);

      int[] actual = new int[vector.getValueCount()];
      IntStream.range(0, vector.getValueCount()).forEach(
          i -> actual[i] = vector.get(i));

      assertArrayEquals(
          new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
              11, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
              40, 41, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71}, actual);
    }
  }
}
