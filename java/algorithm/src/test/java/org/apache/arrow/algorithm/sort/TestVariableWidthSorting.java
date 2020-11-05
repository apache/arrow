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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test sorting variable width vectors with random data.
 */
@RunWith(Parameterized.class)
public class TestVariableWidthSorting<V extends BaseVariableWidthVector, U extends Comparable<U>> {

  static final int[] VECTOR_LENGTHS = new int[] {2, 5, 10, 50, 100, 1000, 3000};

  static final double[] NULL_FRACTIONS = {0, 0.1, 0.3, 0.5, 0.7, 0.9, 1};

  private final int length;

  private final double nullFraction;

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

  public TestVariableWidthSorting(
      int length, double nullFraction, String desc,
      Function<BufferAllocator, V> vectorGenerator, TestSortingUtil.DataGenerator<V, U> dataGenerator) {
    this.length = length;
    this.nullFraction = nullFraction;
    this.vectorGenerator = vectorGenerator;
    this.dataGenerator = dataGenerator;
  }

  @Test
  public void testSort() {
    sortOutOfPlace();
  }

  void sortOutOfPlace() {
    try (V vector = vectorGenerator.apply(allocator)) {
      U[] array = dataGenerator.populate(vector, length, nullFraction);
      Arrays.sort(array, (Comparator<? super U>) new StringComparator());

      // sort the vector
      VariableWidthOutOfPlaceVectorSorter sorter = new VariableWidthOutOfPlaceVectorSorter();
      VectorValueComparator<V> comparator = DefaultVectorComparators.createDefaultComparator(vector);

      try (V sortedVec = (V) vector.getField().getFieldType().createNewSingleVector("", allocator, null)) {
        int dataSize = vector.getOffsetBuffer().getInt(vector.getValueCount() * 4);
        sortedVec.allocateNew(dataSize, vector.getValueCount());
        sortedVec.setValueCount(vector.getValueCount());

        sorter.sortOutOfPlace(vector, sortedVec, comparator);

        // verify results
        verifyResults(sortedVec, (String[]) array);
      }
    }
  }

  @Parameterized.Parameters(name = "length = {0}, null fraction = {1}, vector = {2}")
  public static Collection<Object[]> getParameters() {
    List<Object[]> params = new ArrayList<>();
    for (int length : VECTOR_LENGTHS) {
      for (double nullFrac : NULL_FRACTIONS) {
        params.add(new Object[]{
            length, nullFrac, "VarCharVector",
            (Function<BufferAllocator, VarCharVector>) (allocator -> new VarCharVector("vector", allocator)),
            TestSortingUtil.STRING_GENERATOR
        });
      }
    }
    return params;
  }

  /**
   * Verify results as byte arrays.
   */
  public static <V extends ValueVector> void verifyResults(V vector, String[] expected) {
    assertEquals(vector.getValueCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(vector.isNull(i));
      } else {
        assertArrayEquals(((Text) vector.getObject(i)).getBytes(), expected[i].getBytes());
      }
    }
  }

  /**
   * String comparator with the same behavior as that of
   * {@link DefaultVectorComparators.VariableWidthComparator}.
   */
  static class StringComparator implements Comparator<String> {

    @Override
    public int compare(String str1, String str2) {
      if (str1 == null || str2 == null) {
        if (str1 == null && str2 == null) {
          return 0;
        }

        return str1 == null ? -1 : 1;
      }

      byte[] bytes1 = str1.getBytes();
      byte[] bytes2 = str2.getBytes();

      for (int i = 0; i < bytes1.length && i < bytes2.length; i++) {
        if (bytes1[i] != bytes2[i]) {
          return (bytes1[i] & 0xff) < (bytes2[i] & 0xff) ? -1 : 1;
        }
      }
      return bytes1.length - bytes2.length;
    }
  }
}
