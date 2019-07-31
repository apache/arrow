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

package org.apache.arrow.algorithm.search;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorRangeSearcher}.
 */
public class TestVectorRangeSearcher {

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
  public void testGetLowerBounds() {
    final int maxValue = 100;
    final int repeat = 5;
    try (IntVector intVector = new IntVector("int vec", allocator)) {
      // allocate vector
      intVector.allocateNew(maxValue * repeat);
      intVector.setValueCount(maxValue * repeat);

      // prepare data in sorted order
      // each value is repeated some times
      for (int i = 0; i < maxValue; i++) {
        for (int j = 0; j < repeat; j++) {
          if (i == 0) {
            intVector.setNull(i * repeat + j);
          } else {
            intVector.set(i * repeat + j, i);
          }
        }
      }

      // do search
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
      for (int i = 0; i < maxValue; i++) {
        int result = VectorRangeSearcher.getFirstMatch(intVector, comparator, intVector, i * repeat);
        assertEquals(i * repeat, result);
      }
    }
  }

  @Test
  public void testGetLowerBoundsNegative() {
    final int maxValue = 100;
    final int repeat = 5;
    try (IntVector intVector = new IntVector("int vec", allocator);
    IntVector negVector = new IntVector("neg vec", allocator)) {
      // allocate vector
      intVector.allocateNew(maxValue * repeat);
      intVector.setValueCount(maxValue * repeat);

      negVector.allocateNew(maxValue);
      negVector.setValueCount(maxValue);

      // prepare data in sorted order
      // each value is repeated some times
      for (int i = 0; i < maxValue; i++) {
        for (int j = 0; j < repeat; j++) {
          if (i == 0) {
            intVector.setNull(i * repeat + j);
          } else {
            intVector.set(i * repeat + j, i);
          }
        }
        negVector.set(i, maxValue + i);
      }

      // do search
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
      for (int i = 0; i < maxValue; i++) {
        int result = VectorRangeSearcher.getFirstMatch(intVector, comparator, negVector, i);
        assertEquals(-1, result);
      }
    }
  }

  @Test
  public void testGetUpperBounds() {
    final int maxValue = 100;
    final int repeat = 5;
    try (IntVector intVector = new IntVector("int vec", allocator)) {
      // allocate vector
      intVector.allocateNew(maxValue * repeat);
      intVector.setValueCount(maxValue * repeat);

      // prepare data in sorted order
      // each value is repeated some times
      for (int i = 0; i < maxValue; i++) {
        for (int j = 0; j < repeat; j++) {
          if (i == 0) {
            intVector.setNull(i * repeat + j);
          } else {
            intVector.set(i * repeat + j, i);
          }
        }
      }

      // do search
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
      for (int i = 0; i < maxValue; i++) {
        int result = VectorRangeSearcher.getLastMatch(intVector, comparator, intVector, i * repeat);
        assertEquals((i + 1) * repeat - 1, result);
      }
    }
  }

  @Test
  public void testGetUpperBoundsNegative() {
    final int maxValue = 100;
    final int repeat = 5;
    try (IntVector intVector = new IntVector("int vec", allocator);
         IntVector negVector = new IntVector("neg vec", allocator)) {
      // allocate vector
      intVector.allocateNew(maxValue * repeat);
      intVector.setValueCount(maxValue * repeat);

      negVector.allocateNew(maxValue);
      negVector.setValueCount(maxValue);

      // prepare data in sorted order
      // each value is repeated some times
      for (int i = 0; i < maxValue; i++) {
        for (int j = 0; j < repeat; j++) {
          if (i == 0) {
            intVector.setNull(i * repeat + j);
          } else {
            intVector.set(i * repeat + j, i);
          }
        }
        negVector.set(i, maxValue + i);
      }

      // do search
      VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVector);
      for (int i = 0; i < maxValue; i++) {
        int result = VectorRangeSearcher.getLastMatch(intVector, comparator, negVector, i);
        assertEquals(-1, result);
      }
    }
  }
}
