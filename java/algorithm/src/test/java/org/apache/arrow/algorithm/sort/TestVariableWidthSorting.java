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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Test sorting variable width vectors with random data. */
public class TestVariableWidthSorting<V extends BaseVariableWidthVector, U extends Comparable<U>> {

  static final int[] VECTOR_LENGTHS = new int[] {2, 5, 10, 50, 100, 1000, 3000};

  static final double[] NULL_FRACTIONS = {0, 0.1, 0.3, 0.5, 0.7, 0.9, 1};

  private BufferAllocator allocator;

  @BeforeEach
  public void prepare() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterEach
  public void shutdown() {
    allocator.close();
  }

  @ParameterizedTest
  @MethodSource("getParameters")
  public void testSort(
      int length,
      double nullFraction,
      Function<BufferAllocator, V> vectorGenerator,
      TestSortingUtil.DataGenerator<V, U> dataGenerator) {
    sortOutOfPlace(length, nullFraction, vectorGenerator, dataGenerator);
  }

  void sortOutOfPlace(
      int length,
      double nullFraction,
      Function<BufferAllocator, V> vectorGenerator,
      TestSortingUtil.DataGenerator<V, U> dataGenerator) {
    try (V vector = vectorGenerator.apply(allocator)) {
      U[] array = dataGenerator.populate(vector, length, nullFraction);
      Arrays.sort(array, (Comparator<? super U>) new StringComparator());

      // sort the vector
      VariableWidthOutOfPlaceVectorSorter sorter = new VariableWidthOutOfPlaceVectorSorter();
      VectorValueComparator<V> comparator =
          DefaultVectorComparators.createDefaultComparator(vector);

      try (V sortedVec =
          (V) vector.getField().getFieldType().createNewSingleVector("", allocator, null)) {
        int dataSize = vector.getOffsetBuffer().getInt(vector.getValueCount() * 4L);
        sortedVec.allocateNew(dataSize, vector.getValueCount());
        sortedVec.setValueCount(vector.getValueCount());

        sorter.sortOutOfPlace(vector, sortedVec, comparator);

        // verify results
        verifyResults(sortedVec, (String[]) array);
      }
    }
  }

  public static Stream<Arguments> getParameters() {
    List<Arguments> params = new ArrayList<>();
    for (int length : VECTOR_LENGTHS) {
      for (double nullFrac : NULL_FRACTIONS) {
        params.add(
            Arguments.of(
                length,
                nullFrac,
                (Function<BufferAllocator, VarCharVector>)
                    allocator -> new VarCharVector("vector", allocator),
                TestSortingUtil.STRING_GENERATOR));
      }
    }
    return params.stream();
  }

  /** Verify results as byte arrays. */
  public static <V extends ValueVector> void verifyResults(V vector, String[] expected) {
    assertEquals(vector.getValueCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        assertTrue(vector.isNull(i));
      } else {
        assertArrayEquals(
            ((Text) vector.getObject(i)).getBytes(), expected[i].getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  /**
   * String comparator with the same behavior as that of {@link
   * DefaultVectorComparators.VariableWidthComparator}.
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

      byte[] bytes1 = str1.getBytes(StandardCharsets.UTF_8);
      byte[] bytes2 = str2.getBytes(StandardCharsets.UTF_8);

      for (int i = 0; i < bytes1.length && i < bytes2.length; i++) {
        if (bytes1[i] != bytes2[i]) {
          return (bytes1[i] & 0xff) < (bytes2[i] & 0xff) ? -1 : 1;
        }
      }
      return bytes1.length - bytes2.length;
    }
  }
}
