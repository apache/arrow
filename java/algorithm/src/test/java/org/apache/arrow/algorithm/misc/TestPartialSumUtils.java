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
package org.apache.arrow.algorithm.misc;

import static org.junit.Assert.assertEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test cases for {@link PartialSumUtils}. */
public class TestPartialSumUtils {

  private static final int PARTIAL_SUM_VECTOR_LENGTH = 101;

  private static final int DELTA_VECTOR_LENGTH = 100;

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
  public void testToPartialSumVector() {
    try (IntVector delta = new IntVector("delta", allocator);
        IntVector partialSum = new IntVector("partial sum", allocator)) {
      delta.allocateNew(DELTA_VECTOR_LENGTH);
      delta.setValueCount(DELTA_VECTOR_LENGTH);

      partialSum.allocateNew(PARTIAL_SUM_VECTOR_LENGTH);

      // populate delta vector
      for (int i = 0; i < delta.getValueCount(); i++) {
        delta.set(i, 3);
      }

      final long sumBase = 10;
      PartialSumUtils.toPartialSumVector(delta, partialSum, sumBase);

      // verify results
      assertEquals(PARTIAL_SUM_VECTOR_LENGTH, partialSum.getValueCount());
      for (int i = 0; i < partialSum.getValueCount(); i++) {
        assertEquals(i * 3L + sumBase, partialSum.get(i));
      }
    }
  }

  @Test
  public void testToDeltaVector() {
    try (IntVector partialSum = new IntVector("partial sum", allocator);
        IntVector delta = new IntVector("delta", allocator)) {
      partialSum.allocateNew(PARTIAL_SUM_VECTOR_LENGTH);
      partialSum.setValueCount(PARTIAL_SUM_VECTOR_LENGTH);

      delta.allocateNew(DELTA_VECTOR_LENGTH);

      // populate delta vector
      final int sumBase = 10;
      for (int i = 0; i < partialSum.getValueCount(); i++) {
        partialSum.set(i, sumBase + 3 * i);
      }

      PartialSumUtils.toDeltaVector(partialSum, delta);

      // verify results
      assertEquals(DELTA_VECTOR_LENGTH, delta.getValueCount());
      for (int i = 0; i < delta.getValueCount(); i++) {
        assertEquals(3, delta.get(i));
      }
    }
  }

  @Test
  public void testFindPositionInPartialSumVector() {
    try (IntVector partialSum = new IntVector("partial sum", allocator)) {
      partialSum.allocateNew(PARTIAL_SUM_VECTOR_LENGTH);
      partialSum.setValueCount(PARTIAL_SUM_VECTOR_LENGTH);

      // populate delta vector
      final int sumBase = 10;
      for (int i = 0; i < partialSum.getValueCount(); i++) {
        partialSum.set(i, sumBase + 3 * i);
      }

      // search and verify results
      for (int i = 0; i < PARTIAL_SUM_VECTOR_LENGTH - 1; i++) {
        assertEquals(
            i, PartialSumUtils.findPositionInPartialSumVector(partialSum, sumBase + 3 * i + 1));
      }
    }
  }

  @Test
  public void testFindPositionInPartialSumVectorNegative() {
    try (IntVector partialSum = new IntVector("partial sum", allocator)) {
      partialSum.allocateNew(PARTIAL_SUM_VECTOR_LENGTH);
      partialSum.setValueCount(PARTIAL_SUM_VECTOR_LENGTH);

      // populate delta vector
      final int sumBase = 10;
      for (int i = 0; i < partialSum.getValueCount(); i++) {
        partialSum.set(i, sumBase + 3 * i);
      }

      // search and verify results
      assertEquals(0, PartialSumUtils.findPositionInPartialSumVector(partialSum, sumBase));
      assertEquals(-1, PartialSumUtils.findPositionInPartialSumVector(partialSum, sumBase - 1));
      assertEquals(
          -1,
          PartialSumUtils.findPositionInPartialSumVector(
              partialSum, sumBase + 3 * (PARTIAL_SUM_VECTOR_LENGTH - 1)));
    }
  }
}
