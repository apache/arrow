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
import org.apache.arrow.vector.VarCharVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link org.apache.arrow.algorithm.search.VectorSearcher}.
 */
public class TestVectorSearcher {

  private final int VECTOR_LENGTH = 100;

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
  public void testBinarySearchInt() {
    try (IntVector rawVector = new IntVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          rawVector.set(i, i);
        }
      }

      // do search
      VectorValueComparator<IntVector> comparator = new DefaultVectorComparators.IntComparator();
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }
    }
  }

  @Test
  public void testLinearSearchInt() {
    try (IntVector rawVector = new IntVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          rawVector.set(i, i);
        }
      }

      // do search
      VectorValueComparator<IntVector> comparator = new DefaultVectorComparators.IntComparator();
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }
    }
  }

  @Test
  public void testBinarySearchVarChar() {
    try (VarCharVector rawVector = new VarCharVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH * 16, VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);

      byte[] content = new byte[2];

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          int q = i / 10;
          int r = i % 10;

          content[0] = (byte) ('a' + q);
          content[1] = (byte) r;
          rawVector.set(i, content);
        }
      }

      // do search
      VectorValueComparator<VarCharVector> comparator = new DefaultVectorComparators.VarCharComparator();
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }
    }
  }

  @Test
  public void testLinearSearchVarChar() {
    try (VarCharVector rawVector = new VarCharVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH * 16, VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);

      byte[] content = new byte[2];

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          int q = i / 10;
          int r = i % 10;

          content[0] = (byte) ('a' + q);
          content[1] = (byte) r;
          rawVector.set(i, content);
        }
      }

      // do search
      VectorValueComparator<VarCharVector> comparator = new DefaultVectorComparators.VarCharComparator();
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }
    }
  }
}
