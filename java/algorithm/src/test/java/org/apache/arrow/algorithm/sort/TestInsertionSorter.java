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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link InsertionSorter}.
 */
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

  private void testSortIntVectorRange(int start, int end, int vectorLength) {
    try (IntVector vector = new IntVector("vector", allocator);
         IntVector buffer = new IntVector("buffer", allocator)) {

      vector.allocateNew(vectorLength);
      buffer.allocateNew(1);

      for (int i = 0; i < vectorLength; i++) {
        // place values in reverse order
        vector.set(i, vectorLength - i - 1);
      }

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);
      InsertionSorter.insertionSort(vector, start, end, comparator, buffer);

      // verify results
      for (int i = start, expected = vectorLength - end - 1; i < end; i++, expected++) {
        assertEquals(expected, vector.get(i));
      }
    }
  }

  @Test
  public void testSortIntVector() {
    testSortIntVectorRange(0, 99, 100);
    testSortIntVectorRange(15, 30, 100);
    testSortIntVectorRange(50, 50, 100);
    testSortIntVectorRange(50, 51, 100);
    testSortIntVectorRange(0, 30, 100);
    testSortIntVectorRange(72, 99, 100);
  }

  private void testSortIndicesRange(int start, int end, int vectorLength) {
    try (IntVector vector = new IntVector("vector", allocator);
         IntVector indices = new IntVector("indices", allocator)) {

      vector.allocateNew(vectorLength);
      indices.allocateNew(vectorLength);

      for (int i = 0; i < vectorLength; i++) {
        // place values in reverse order
        vector.set(i, (vectorLength - i - 1) * 10);
        indices.set(i, i);
      }

      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);
      comparator.attachVector(vector);

      InsertionSorter.insertionSort(indices, start, end, comparator);

      // verify results
      for (int i = start, expected = vectorLength - end - 1; i < end; i++, expected++) {
        assertEquals(expected, indices.get(i));
      }
    }
  }

  @Test
  public void testSortIndices() {
    testSortIntVectorRange(0, 99, 100);
    testSortIntVectorRange(15, 30, 100);
    testSortIntVectorRange(50, 50, 100);
    testSortIntVectorRange(50, 51, 100);
    testSortIntVectorRange(0, 30, 100);
    testSortIntVectorRange(72, 99, 100);
  }
}
