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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Test cases for {@link FixedWidthOutOfPlaceVectorSorter}. */
public class TestFixedWidthOutOfPlaceVectorSorter extends TestOutOfPlaceVectorSorter {

  private BufferAllocator allocator;

  <V extends BaseFixedWidthVector> OutOfPlaceVectorSorter<V> getSorter(boolean generalSorter) {
    return generalSorter
        ? new GeneralOutOfPlaceVectorSorter<>()
        : new FixedWidthOutOfPlaceVectorSorter<>();
  }

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(1024 * 1024);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortByte(boolean generalSorter) {
    try (TinyIntVector vec = new TinyIntVector("", allocator)) {
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
      OutOfPlaceVectorSorter<TinyIntVector> sorter = getSorter(generalSorter);
      VectorValueComparator<TinyIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      TinyIntVector sortedVec =
          (TinyIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals((byte) 2, sortedVec.get(2));
      assertEquals((byte) 8, sortedVec.get(3));
      assertEquals((byte) 10, sortedVec.get(4));
      assertEquals((byte) 10, sortedVec.get(5));
      assertEquals((byte) 12, sortedVec.get(6));
      assertEquals((byte) 17, sortedVec.get(7));
      assertEquals((byte) 23, sortedVec.get(8));
      assertEquals((byte) 35, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortShort(boolean generalSorter) {
    try (SmallIntVector vec = new SmallIntVector("", allocator)) {
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
      OutOfPlaceVectorSorter<SmallIntVector> sorter = getSorter(generalSorter);
      VectorValueComparator<SmallIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      SmallIntVector sortedVec =
          (SmallIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals((short) 2, sortedVec.get(2));
      assertEquals((short) 8, sortedVec.get(3));
      assertEquals((short) 10, sortedVec.get(4));
      assertEquals((short) 10, sortedVec.get(5));
      assertEquals((short) 12, sortedVec.get(6));
      assertEquals((short) 17, sortedVec.get(7));
      assertEquals((short) 23, sortedVec.get(8));
      assertEquals((short) 35, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortInt(boolean generalSorter) {
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
      OutOfPlaceVectorSorter<IntVector> sorter = getSorter(generalSorter);
      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      IntVector sortedVec =
          (IntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals(2, sortedVec.get(2));
      assertEquals(8, sortedVec.get(3));
      assertEquals(10, sortedVec.get(4));
      assertEquals(10, sortedVec.get(5));
      assertEquals(12, sortedVec.get(6));
      assertEquals(17, sortedVec.get(7));
      assertEquals(23, sortedVec.get(8));
      assertEquals(35, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortLong(boolean generalSorter) {
    try (BigIntVector vec = new BigIntVector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10L);
      vec.set(1, 8L);
      vec.setNull(2);
      vec.set(3, 10L);
      vec.set(4, 12L);
      vec.set(5, 17L);
      vec.setNull(6);
      vec.set(7, 23L);
      vec.set(8, 1L << 35L);
      vec.set(9, 2L);

      // sort the vector
      OutOfPlaceVectorSorter<BigIntVector> sorter = getSorter(generalSorter);
      VectorValueComparator<BigIntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      BigIntVector sortedVec =
          (BigIntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals(2L, sortedVec.get(2));
      assertEquals(8L, sortedVec.get(3));
      assertEquals(10L, sortedVec.get(4));
      assertEquals(10L, sortedVec.get(5));
      assertEquals(12L, sortedVec.get(6));
      assertEquals(17L, sortedVec.get(7));
      assertEquals(23L, sortedVec.get(8));
      assertEquals(1L << 35L, sortedVec.get(9));

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortFloat(boolean generalSorter) {
    try (Float4Vector vec = new Float4Vector("", allocator)) {
      vec.allocateNew(10);
      vec.setValueCount(10);

      // fill data to sort
      vec.set(0, 10f);
      vec.set(1, 8f);
      vec.setNull(2);
      vec.set(3, 10f);
      vec.set(4, 12f);
      vec.set(5, 17f);
      vec.setNull(6);
      vec.set(7, 23f);
      vec.set(8, Float.NaN);
      vec.set(9, 2f);

      // sort the vector
      OutOfPlaceVectorSorter<Float4Vector> sorter = getSorter(generalSorter);
      VectorValueComparator<Float4Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      Float4Vector sortedVec =
          (Float4Vector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals(2f, sortedVec.get(2), 0f);
      assertEquals(8f, sortedVec.get(3), 0f);
      assertEquals(10f, sortedVec.get(4), 0f);
      assertEquals(10f, sortedVec.get(5), 0f);
      assertEquals(12f, sortedVec.get(6), 0f);
      assertEquals(17f, sortedVec.get(7), 0f);
      assertEquals(23f, sortedVec.get(8), 0f);
      assertEquals(Float.NaN, sortedVec.get(9), 0f);

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortDouble(boolean generalSorter) {
    try (Float8Vector vec = new Float8Vector("", allocator)) {
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
      vec.set(7, Double.NaN);
      vec.set(8, 35);
      vec.set(9, 2);

      // sort the vector
      OutOfPlaceVectorSorter<Float8Vector> sorter = getSorter(generalSorter);
      VectorValueComparator<Float8Vector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      Float8Vector sortedVec =
          (Float8Vector) vec.getField().getFieldType().createNewSingleVector("", allocator, null);
      sortedVec.allocateNew(vec.getValueCount());
      sortedVec.setValueCount(vec.getValueCount());

      sorter.sortOutOfPlace(vec, sortedVec, comparator);

      // verify results
      assertEquals(vec.getValueCount(), sortedVec.getValueCount());

      assertTrue(sortedVec.isNull(0));
      assertTrue(sortedVec.isNull(1));
      assertEquals(2, sortedVec.get(2), 0);
      assertEquals(8, sortedVec.get(3), 0);
      assertEquals(10, sortedVec.get(4), 0);
      assertEquals(10, sortedVec.get(5), 0);
      assertEquals(12, sortedVec.get(6), 0);
      assertEquals(17, sortedVec.get(7), 0);
      assertEquals(35, sortedVec.get(8), 0);
      assertEquals(Double.NaN, sortedVec.get(9), 0);

      sortedVec.close();
    }
  }

  @ParameterizedTest
  @MethodSource("getParameter")
  public void testSortInt2(boolean generalSorter) {
    try (IntVector vec = new IntVector("", allocator)) {
      ValueVectorDataPopulator.setVector(
          vec, 0, 1, 2, 3, 4, 5, 30, 31, 32, 33, 34, 35, 60, 61, 62, 63, 64, 65, 6, 7, 8, 9, 10, 11,
          36, 37, 38, 39, 40, 41, 66, 67, 68, 69, 70, 71);

      // sort the vector
      OutOfPlaceVectorSorter<IntVector> sorter = getSorter(generalSorter);
      VectorValueComparator<IntVector> comparator =
          DefaultVectorComparators.createDefaultComparator(vec);

      try (IntVector sortedVec =
          (IntVector) vec.getField().getFieldType().createNewSingleVector("", allocator, null)) {
        sortedVec.allocateNew(vec.getValueCount());
        sortedVec.setValueCount(vec.getValueCount());

        sorter.sortOutOfPlace(vec, sortedVec, comparator);

        // verify results
        int[] actual = new int[sortedVec.getValueCount()];
        IntStream.range(0, sortedVec.getValueCount()).forEach(i -> actual[i] = sortedVec.get(i));

        assertArrayEquals(
            new int[] {
              0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41,
              60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71
            },
            actual);
      }
    }
  }
}
