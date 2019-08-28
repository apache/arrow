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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link StableVectorComparator}.
 */
public class TestStableVectorComparator {

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
  public void testCompare() {
    try (VarCharVector vec = new VarCharVector("", allocator)) {
      vec.allocateNew(100, 5);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, "ba".getBytes());
      vec.set(1, "abc".getBytes());
      vec.set(2, "aa".getBytes());
      vec.set(3, "abc".getBytes());
      vec.set(4, "a".getBytes());

      VectorValueComparator<VarCharVector> comparator = new TestVarCharSorter();
      VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparator);
      stableComparator.attachVector(vec);

      assertTrue(stableComparator.compare(0, 1) > 0);
      assertTrue(stableComparator.compare(1, 2) < 0);
      assertTrue(stableComparator.compare(2, 3) < 0);
      assertTrue(stableComparator.compare(1, 3) < 0);
      assertTrue(stableComparator.compare(3, 1) > 0);
      assertTrue(stableComparator.compare(3, 3) == 0);
    }
  }

  @Test
  public void testStableSortString() {
    try (VarCharVector vec = new VarCharVector("", allocator)) {
      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, "a".getBytes());
      vec.set(1, "abc".getBytes());
      vec.set(2, "aa".getBytes());
      vec.set(3, "a1".getBytes());
      vec.set(4, "abcdefg".getBytes());
      vec.set(5, "accc".getBytes());
      vec.set(6, "afds".getBytes());
      vec.set(7, "0".getBytes());
      vec.set(8, "01".getBytes());
      vec.set(9, "0c".getBytes());

      // sort the vector
      VariableWidthOutOfPlaceVectorSorter sorter = new VariableWidthOutOfPlaceVectorSorter();
      VectorValueComparator<VarCharVector> comparator = new TestVarCharSorter();
      VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparator);

      try (VarCharVector sortedVec =
              (VarCharVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null)) {
        sortedVec.allocateNew(vec.getByteCapacity(), vec.getValueCount());
        sortedVec.setLastSet(vec.getValueCount() - 1);
        sortedVec.setValueCount(vec.getValueCount());

        sorter.sortOutOfPlace(vec, sortedVec, stableComparator);

        // verify results
        // the results are stable
        assertEquals("0", new String(sortedVec.get(0)));
        assertEquals("01", new String(sortedVec.get(1)));
        assertEquals("0c", new String(sortedVec.get(2)));
        assertEquals("a", new String(sortedVec.get(3)));
        assertEquals("abc", new String(sortedVec.get(4)));
        assertEquals("aa", new String(sortedVec.get(5)));
        assertEquals("a1", new String(sortedVec.get(6)));
        assertEquals("abcdefg", new String(sortedVec.get(7)));
        assertEquals("accc", new String(sortedVec.get(8)));
        assertEquals("afds", new String(sortedVec.get(9)));
      }
    }
  }

  /**
   * Utility comparator that compares varchars by the first character.
   */
  private static class TestVarCharSorter extends VectorValueComparator<VarCharVector> {

    @Override
    public int compareNotNull(int index1, int index2) {
      byte b1 = vector1.get(index1)[0];
      byte b2 = vector2.get(index2)[0];
      return b1 - b2;
    }
  }
}
