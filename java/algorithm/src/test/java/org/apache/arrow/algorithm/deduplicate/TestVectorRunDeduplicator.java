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

package org.apache.arrow.algorithm.deduplicate;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link VectorRunDeduplicator}.
 */
public class TestVectorRunDeduplicator {

  private static final int VECTOR_LENGTH = 100;

  private static final int REPETITION_COUNT = 3;

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
  public void testDeduplicateFixedWidth() {
    try (IntVector origVec = new IntVector("original vec", allocator);
    IntVector dedupVec = new IntVector("deduplicated vec", allocator);
    IntVector lengthVec = new IntVector("length vec", allocator);
    VectorRunDeduplicator<IntVector> deduplicator =
                 new VectorRunDeduplicator<>(origVec, allocator)) {
      origVec.allocateNew(VECTOR_LENGTH * REPETITION_COUNT);
      origVec.setValueCount(VECTOR_LENGTH * REPETITION_COUNT);
      lengthVec.allocateNew();

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        for (int j = 0; j < REPETITION_COUNT; j++) {
          origVec.set(i * REPETITION_COUNT + j, i);
        }
      }

      int distinctCount = deduplicator.getRunCount();
      assertEquals(VECTOR_LENGTH, distinctCount);

      dedupVec.allocateNew(distinctCount);

      deduplicator.populateDeduplicatedValues(dedupVec);
      assertEquals(VECTOR_LENGTH, dedupVec.getValueCount());

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertEquals(i, dedupVec.get(i));
      }

      deduplicator.populateRunLengths(lengthVec);
      assertEquals(VECTOR_LENGTH, lengthVec.getValueCount());

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertEquals(REPETITION_COUNT, lengthVec.get(i));
      }
    }
  }

  @Test
  public void testDeduplicateVariableWidth() {
    try (VarCharVector origVec = new VarCharVector("original vec", allocator);
         VarCharVector dedupVec = new VarCharVector("deduplicated vec", allocator);
         IntVector lengthVec = new IntVector("length vec", allocator);
         VectorRunDeduplicator<VarCharVector> deduplicator =
                 new VectorRunDeduplicator<>(origVec, allocator)) {
      origVec.allocateNew(
              VECTOR_LENGTH * REPETITION_COUNT * 10, VECTOR_LENGTH * REPETITION_COUNT);
      origVec.setValueCount(VECTOR_LENGTH * REPETITION_COUNT);
      lengthVec.allocateNew();

      // prepare data
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        String str = String.valueOf(i * i);
        for (int j = 0; j < REPETITION_COUNT; j++) {
          origVec.set(i * REPETITION_COUNT + j, str.getBytes());
        }
      }

      int distinctCount = deduplicator.getRunCount();
      assertEquals(VECTOR_LENGTH, distinctCount);

      dedupVec.allocateNew(distinctCount * 10, distinctCount);

      deduplicator.populateDeduplicatedValues(dedupVec);
      assertEquals(VECTOR_LENGTH, dedupVec.getValueCount());

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertArrayEquals(String.valueOf(i * i).getBytes(), dedupVec.get(i));
      }

      deduplicator.populateRunLengths(lengthVec);
      assertEquals(VECTOR_LENGTH, lengthVec.getValueCount());

      for (int i = 0; i < VECTOR_LENGTH; i++) {
        assertEquals(REPETITION_COUNT, lengthVec.get(i));
      }
    }
  }
}
