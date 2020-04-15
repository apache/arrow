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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Insertion sorter.
 */
class InsertionSorter {

  /**
   * Sorts the range of a vector by insertion sort.
   *
   * @param vector     the vector to be sorted.
   * @param startIdx   the start index of the range (inclusive).
   * @param endIdx     the end index of the range (inclusive).
   * @param buffer     an extra buffer with capacity 1 to hold the current key.
   * @param comparator the criteria for vector element comparison.
   * @param <V>        the vector type.
   */
  static <V extends BaseFixedWidthVector> void insertionSort(
      V vector, int startIdx, int endIdx, VectorValueComparator<V> comparator, V buffer) {
    comparator.attachVectors(vector, buffer);
    for (int i = startIdx; i <= endIdx; i++) {
      buffer.copyFrom(i, 0, vector);
      int j = i - 1;
      while (j >= startIdx && comparator.compare(j, 0) > 0) {
        vector.copyFrom(j, j + 1, vector);
        j = j - 1;
      }
      vector.copyFrom(0, j + 1, buffer);
    }
  }

  /**
   * Sorts the range of vector indices by insertion sort.
   *
   * @param indices    the vector  indices.
   * @param startIdx   the start index of the range (inclusive).
   * @param endIdx     the end index of the range (inclusive).
   * @param comparator the criteria for vector element comparison.
   * @param <V>        the vector type.
   */
  static <V extends ValueVector> void insertionSort(
      IntVector indices, int startIdx, int endIdx, VectorValueComparator<V> comparator) {
    for (int i = startIdx; i <= endIdx; i++) {
      int key = indices.get(i);
      int j = i - 1;
      while (j >= startIdx && comparator.compare(indices.get(j), key) > 0) {
        indices.set(j + 1, indices.get(j));
        j = j - 1;
      }
      indices.set(j + 1, key);
    }
  }
}
