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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.testing.RandomDataGenerator;
import org.apache.arrow.vector.testing.ValueVectorDataPopulator;

/**
 * Utilities for sorting related utilities.
 */
public class TestSortingUtil {

  static final Random random = new Random(0);

  static final DataGenerator<TinyIntVector, Byte> TINY_INT_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.TINY_INT_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Byte.class);

  static final DataGenerator<SmallIntVector, Short> SMALL_INT_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.SMALL_INT_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Short.class);

  static final DataGenerator<IntVector, Integer> INT_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.INT_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Integer.class);

  static final DataGenerator<BigIntVector, Long> LONG_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.LONG_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Long.class);

  static final DataGenerator<Float4Vector, Float> FLOAT_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.FLOAT_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Float.class);

  static final DataGenerator<Float8Vector, Double> DOUBLE_GENERATOR = new DataGenerator<>(
      RandomDataGenerator.DOUBLE_GENERATOR,
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), Double.class);

  static final DataGenerator<VarCharVector, String> STRING_GENERATOR = new DataGenerator<>(
      () -> {
        int strLength = random.nextInt(20) + 1;
        return generateRandomString(strLength);
      },
      (vector, array) -> ValueVectorDataPopulator.setVector(vector, array), String.class);

  private TestSortingUtil() {
  }

  /**
   * Verify that a vector is equal to an array.
   */
  public static <V extends ValueVector, U> void verifyResults(V vector, U[] expected) {
    assertEquals(vector.getValueCount(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(vector.getObject(i), expected[i]);
    }
  }

  /**
   * Sort an array with null values come first.
   */
  public static <U extends Comparable<U>> void sortArray(U[] array) {
    Arrays.sort(array, (a, b) -> {
      if (a == null || b == null) {
        if (a == null && b == null) {
          return 0;
        }

        // exactly one is null
        if (a == null) {
          return -1;
        } else {
          return 1;
        }
      }
      return a.compareTo(b);
    });
  }

  /**
   * Generate a string with alphabetic characters only.
   */
  static String generateRandomString(int length) {
    byte[] str = new byte[length];
    final int lower = 'a';
    final int upper = 'z';

    for (int i = 0; i < length; i++) {
      // make r non-negative
      int r = random.nextInt() & Integer.MAX_VALUE;
      str[i] = (byte) (r % (upper - lower + 1) + lower);
    }

    return new String(str, StandardCharsets.UTF_8);
  }

  /**
   * Utility to generate data for testing.
   * @param <V> vector type.
   * @param <U> data element type.
   */
  static class DataGenerator<V extends ValueVector, U extends Comparable<U>> {

    final Supplier<U> dataGenerator;

    final BiConsumer<V, U[]> vectorPopulator;

    final Class<U> clazz;

    DataGenerator(
        Supplier<U> dataGenerator, BiConsumer<V, U[]> vectorPopulator, Class<U> clazz) {
      this.dataGenerator = dataGenerator;
      this.vectorPopulator = vectorPopulator;
      this.clazz = clazz;
    }

    /**
     * Populate the vector according to the specified parameters.
     * @param vector the vector to populate.
     * @param length vector length.
     * @param nullFraction the fraction of null values.
     * @return An array with the same data as the vector.
     */
    U[] populate(V vector, int length, double nullFraction) {
      U[] array = (U[]) Array.newInstance(clazz, length);
      for (int i = 0; i < length; i++) {
        double r = Math.random();
        U value = r < nullFraction ? null : dataGenerator.get();
        array[i] = value;
      }
      vectorPopulator.accept(vector, array);
      return array;
    }
  }
}
