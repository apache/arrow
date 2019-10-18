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

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;
import static org.junit.Assert.assertEquals;

import org.apache.arrow.algorithm.sort.DefaultVectorComparators;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

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
    try (IntVector rawVector = new IntVector("", allocator);
         IntVector negVector = new IntVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);
      negVector.allocateNew(1);
      negVector.setValueCount(1);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          rawVector.set(i, i);
        }
      }
      negVector.set(0, -333);

      // do search
      VectorValueComparator<IntVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      assertEquals(-1, VectorSearcher.binarySearch(rawVector, comparator, negVector, 0));
    }
  }

  @Test
  public void testLinearSearchInt() {
    try (IntVector rawVector = new IntVector("", allocator);
         IntVector negVector = new IntVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);
      negVector.allocateNew(1);
      negVector.setValueCount(1);

      // prepare data in sorted order
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        if (i == 0) {
          rawVector.setNull(i);
        } else {
          rawVector.set(i, i);
        }
      }
      negVector.set(0, -333);

      // do search
      VectorValueComparator<IntVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      assertEquals(-1, VectorSearcher.linearSearch(rawVector, comparator, negVector, 0));
    }
  }

  @Test
  public void testBinarySearchVarChar() {
    try (VarCharVector rawVector = new VarCharVector("", allocator);
         VarCharVector negVector = new VarCharVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH * 16, VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);
      negVector.allocateNew(VECTOR_LENGTH, 1);
      negVector.setValueCount(1);

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
      negVector.set(0, "abcd".getBytes());

      // do search
      VectorValueComparator<BaseVariableWidthVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      assertEquals(-1, VectorSearcher.binarySearch(rawVector, comparator, negVector, 0));
    }
  }

  @Test
  public void testLinearSearchVarChar() {
    try (VarCharVector rawVector = new VarCharVector("", allocator);
         VarCharVector negVector = new VarCharVector("", allocator)) {
      rawVector.allocateNew(VECTOR_LENGTH * 16, VECTOR_LENGTH);
      rawVector.setValueCount(VECTOR_LENGTH);
      negVector.allocateNew(VECTOR_LENGTH, 1);
      negVector.setValueCount(1);

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
      negVector.set(0, "abcd".getBytes());

      // do search
      VectorValueComparator<BaseVariableWidthVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < VECTOR_LENGTH; i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      assertEquals(-1, VectorSearcher.linearSearch(rawVector, comparator, negVector, 0));
    }
  }

  private ListVector createListVector() {
    final int innerCount = 100;
    final int outerCount = 10;
    final int listLength = innerCount / outerCount;

    ListVector listVector = ListVector.empty("list vector", allocator);

    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));

    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < innerCount; i++) {
      dataVector.set(i, i);
    }
    dataVector.setValueCount(innerCount);

    for (int i = 0; i < outerCount; i++) {
      BitVectorHelper.setValidityBitToOne(listVector.getValidityBuffer(), i);
      listVector.getOffsetBuffer().setInt(i * OFFSET_WIDTH, i * listLength);
      listVector.getOffsetBuffer().setInt((i + 1) * OFFSET_WIDTH, (i + 1) * listLength);
    }
    listVector.setLastSet(outerCount - 1);
    listVector.setValueCount(outerCount);

    return listVector;
  }

  private ListVector createNegativeListVector() {
    final int innerCount = 100;
    final int outerCount = 10;
    final int listLength = innerCount / outerCount;

    ListVector listVector = ListVector.empty("list vector", allocator);

    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));

    listVector.allocateNew();

    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < innerCount; i++) {
      dataVector.set(i, i + 1000);
    }
    dataVector.setValueCount(innerCount);

    for (int i = 0; i < outerCount; i++) {
      BitVectorHelper.setValidityBitToOne(listVector.getValidityBuffer(), i);
      listVector.getOffsetBuffer().setInt(i * OFFSET_WIDTH, i * listLength);
      listVector.getOffsetBuffer().setInt((i + 1) * OFFSET_WIDTH, (i + 1) * listLength);
    }
    listVector.setValueCount(outerCount);

    return listVector;
  }

  @Test
  public void testBinarySearchList() {
    try (ListVector rawVector = createListVector();
         ListVector negVector = createNegativeListVector()) {

      // do search
      VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < rawVector.getValueCount(); i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      for (int i = 0; i < rawVector.getValueCount(); i++) {
        int result = VectorSearcher.binarySearch(rawVector, comparator, negVector, i);
        assertEquals(-1, result);
      }
    }
  }

  @Test
  public void testLinearSearchList() {
    try (ListVector rawVector = createListVector();
         ListVector negVector = createNegativeListVector()) {

      // do search
      VectorValueComparator<ListVector> comparator =
              DefaultVectorComparators.createDefaultComparator(rawVector);
      for (int i = 0; i < rawVector.getValueCount(); i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, rawVector, i);
        assertEquals(i, result);
      }

      // negative case
      for (int i = 0; i < rawVector.getValueCount(); i++) {
        int result = VectorSearcher.linearSearch(rawVector, comparator, negVector, i);
        assertEquals(-1, result);
      }
    }
  }
}
