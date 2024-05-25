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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test sorting fixed width vectors with random data.
 */
@RunWith(Parameterized.class)
public class TestFixedWidthSorting<V extends BaseFixedWidthVector, U extends Comparable<U>> {

  static final int[] VECTOR_LENGTHS = new int[] {2, 5, 10, 50, 100, 1000, 3000};

  static final double[] NULL_FRACTIONS = {0, 0.1, 0.3, 0.5, 0.7, 0.9, 1};

  private final int length;

  private final double nullFraction;

  private final boolean inPlace;

  private final Function<BufferAllocator, V> vectorGenerator;

  private final TestSortingUtil.DataGenerator<V, U> dataGenerator;

  private BufferAllocator allocator;

  @Before
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void shutdown() {
    allocator.close();
  }

  public TestFixedWidthSorting(
      int length, double nullFraction, boolean inPlace, String desc,
      Function<BufferAllocator, V> vectorGenerator, TestSortingUtil.DataGenerator<V, U> dataGenerator) {
    this.length = length;
    this.nullFraction = nullFraction;
    this.inPlace = inPlace;
    this.vectorGenerator = vectorGenerator;
    this.dataGenerator = dataGenerator;
  }

  @Test
  public void testSort() {
    if (inPlace) {
      sortInPlace();
    } else {
      sortOutOfPlace();
    }
  }

  void sortInPlace() {
    try (V vector = vectorGenerator.apply(allocator)) {
      U[] array = dataGenerator.populate(vector, length, nullFraction);
      TestSortingUtil.sortArray(array);

      FixedWidthInPlaceVectorSorter sorter = new FixedWidthInPlaceVectorSorter();
      VectorValueComparator<V> comparator = DefaultVectorComparators.createDefaultComparator(vector);

      sorter.sortInPlace(vector, comparator);

      TestSortingUtil.verifyResults(vector, array);
    }
  }

  void sortOutOfPlace() {
    try (V vector = vectorGenerator.apply(allocator)) {
      U[] array = dataGenerator.populate(vector, length, nullFraction);
      TestSortingUtil.sortArray(array);

      // sort the vector
      FixedWidthOutOfPlaceVectorSorter sorter = new FixedWidthOutOfPlaceVectorSorter();
      VectorValueComparator<V> comparator = DefaultVectorComparators.createDefaultComparator(vector);

      try (V sortedVec = (V) vector.getField().getFieldType().createNewSingleVector("", allocator, null)) {
        sortedVec.allocateNew(vector.getValueCount());
        sortedVec.setValueCount(vector.getValueCount());

        sorter.sortOutOfPlace(vector, sortedVec, comparator);

        // verify results
        TestSortingUtil.verifyResults(sortedVec, array);
      }
    }
  }

  @Parameterized.Parameters(name = "length = {0}, null fraction = {1}, in place = {2}, vector = {3}")
  public static Collection<Object[]> getParameters() {
    List<Object[]> params = new ArrayList<>();
    for (int length : VECTOR_LENGTHS) {
      for (double nullFrac : NULL_FRACTIONS) {
        for (boolean inPlace : new boolean[] {true, false}) {
          params.add(new Object[] {
              length, nullFrac, inPlace, "TinyIntVector",
              (Function<BufferAllocator, TinyIntVector>) allocator -> new TinyIntVector("vector", allocator),
              TestSortingUtil.TINY_INT_GENERATOR
          });

          params.add(new Object[] {
              length, nullFrac, inPlace, "SmallIntVector",
              (Function<BufferAllocator, SmallIntVector>) allocator -> new SmallIntVector("vector", allocator),
              TestSortingUtil.SMALL_INT_GENERATOR
          });

          params.add(new Object[] {
              length, nullFrac, inPlace, "IntVector",
              (Function<BufferAllocator, IntVector>) allocator -> new IntVector("vector", allocator),
              TestSortingUtil.INT_GENERATOR
          });

          params.add(new Object[] {
              length, nullFrac, inPlace, "BigIntVector",
              (Function<BufferAllocator, BigIntVector>) allocator -> new BigIntVector("vector", allocator),
              TestSortingUtil.LONG_GENERATOR
          });

          params.add(new Object[] {
              length, nullFrac, inPlace, "Float4Vector",
              (Function<BufferAllocator, Float4Vector>) allocator -> new Float4Vector("vector", allocator),
              TestSortingUtil.FLOAT_GENERATOR
          });

          params.add(new Object[] {
              length, nullFrac, inPlace, "Float8Vector",
              (Function<BufferAllocator, Float8Vector>) allocator -> new Float8Vector("vector", allocator),
              TestSortingUtil.DOUBLE_GENERATOR
          });
        }
      }
    }
    return params;
  }
}
