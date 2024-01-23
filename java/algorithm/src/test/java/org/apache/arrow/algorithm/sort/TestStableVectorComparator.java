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

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

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
      vec.set(0, "ba".getBytes(StandardCharsets.UTF_8));
      vec.set(1, "abc".getBytes(StandardCharsets.UTF_8));
      vec.set(2, "aa".getBytes(StandardCharsets.UTF_8));
      vec.set(3, "abc".getBytes(StandardCharsets.UTF_8));
      vec.set(4, "a".getBytes(StandardCharsets.UTF_8));

      VectorValueComparator<VarCharVector> comparator = new TestVarCharSorter();
      VectorValueComparator<VarCharVector> stableComparator = new StableVectorComparator<>(comparator);
      stableComparator.attachVector(vec);

      assertTrue(stableComparator.compare(0, 1) > 0);
      assertTrue(stableComparator.compare(1, 2) < 0);
      assertTrue(stableComparator.compare(2, 3) < 0);
      assertTrue(stableComparator.compare(1, 3) < 0);
      assertTrue(stableComparator.compare(3, 1) > 0);
      Assertions.assertEquals(0, stableComparator.compare(3, 3));
    }
  }

  @Test
  public void testStableSortString() {
    try (VarCharVector vec = new VarCharVector("", allocator)) {
      vec.allocateNew(100, 10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, "a".getBytes(StandardCharsets.UTF_8));
      vec.set(1, "abc".getBytes(StandardCharsets.UTF_8));
      vec.set(2, "aa".getBytes(StandardCharsets.UTF_8));
      vec.set(3, "a1".getBytes(StandardCharsets.UTF_8));
      vec.set(4, "abcdefg".getBytes(StandardCharsets.UTF_8));
      vec.set(5, "accc".getBytes(StandardCharsets.UTF_8));
      vec.set(6, "afds".getBytes(StandardCharsets.UTF_8));
      vec.set(7, "0".getBytes(StandardCharsets.UTF_8));
      vec.set(8, "01".getBytes(StandardCharsets.UTF_8));
      vec.set(9, "0c".getBytes(StandardCharsets.UTF_8));

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
        assertEquals("0", new String(Objects.requireNonNull(sortedVec.get(0)), StandardCharsets.UTF_8));
        assertEquals("01", new String(Objects.requireNonNull(sortedVec.get(1)), StandardCharsets.UTF_8));
        assertEquals("0c", new String(Objects.requireNonNull(sortedVec.get(2)), StandardCharsets.UTF_8));
        assertEquals("a", new String(Objects.requireNonNull(sortedVec.get(3)), StandardCharsets.UTF_8));
        assertEquals("abc", new String(Objects.requireNonNull(sortedVec.get(4)), StandardCharsets.UTF_8));
        assertEquals("aa", new String(Objects.requireNonNull(sortedVec.get(5)), StandardCharsets.UTF_8));
        assertEquals("a1", new String(Objects.requireNonNull(sortedVec.get(6)), StandardCharsets.UTF_8));
        assertEquals("abcdefg", new String(Objects.requireNonNull(sortedVec.get(7)), StandardCharsets.UTF_8));
        assertEquals("accc", new String(Objects.requireNonNull(sortedVec.get(8)), StandardCharsets.UTF_8));
        assertEquals("afds", new String(Objects.requireNonNull(sortedVec.get(9)), StandardCharsets.UTF_8));
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

    @Override
    public VectorValueComparator<VarCharVector> createNew() {
      return new TestVarCharSorter();
    }
  }
}
