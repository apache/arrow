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

import org.apache.arrow.vector.ValueVector;

/**
 * Sorter for the indices of a vector.
 * @param <V> vector type.
 */
public class IndexSorter<V extends ValueVector> {

  /**
   * Comparator for vector indices.
   */
  private VectorValueComparator<V> comparator;

  /**
   * Vector indices to sort.
   */
  private int[] indices;

  /**
   * Sorts indices, by quick-sort. Suppose the vector is denoted by v.
   * After calling this method, the following relations hold:
   * v(indices[0]) <= v(indices[1]) <= ...
   * @param indices the indices to sort.
   * @param comparator the comparator to sort indices.
   */
  public void sort(int[] indices, VectorValueComparator<V> comparator) {
    this.indices = indices;
    this.comparator = comparator;

    quickSort(0, indices.length - 1);
  }

  private void quickSort(int low, int high) {
    if (low < high) {
      int mid = partition(low, high);
      quickSort(low, mid - 1);
      quickSort(mid + 1, high);
    }
  }

  private int partition(int low, int high) {
    int pivotIndex = indices[low];

    while (low < high) {
      while (low < high && comparator.compare(indices[high], pivotIndex) >= 0) {
        high -= 1;
      }
      indices[low] = indices[high];

      while (low < high && comparator.compare(indices[low], pivotIndex) <= 0) {
        low += 1;
      }
      indices[high] = indices[low];
    }

    indices[low] = pivotIndex;
    return low;
  }
}
