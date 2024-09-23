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

import java.util.stream.IntStream;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Sorter for the indices of a vector.
 *
 * @param <V> vector type.
 */
public class IndexSorter<V extends ValueVector> {

  /**
   * If the number of items is smaller than this threshold, we will use another algorithm to sort
   * the data.
   */
  public static final int CHANGE_ALGORITHM_THRESHOLD = 15;

  /** Comparator for vector indices. */
  private VectorValueComparator<V> comparator;

  /** Vector indices to sort. */
  private IntVector indices;

  /**
   * Sorts indices, by quick-sort. Suppose the vector is denoted by v. After calling this method,
   * the following relations hold: v(indices[0]) <= v(indices[1]) <= ...
   *
   * @param vector the vector whose indices need to be sorted.
   * @param indices the vector for storing the sorted indices.
   * @param comparator the comparator to sort indices.
   */
  public void sort(V vector, IntVector indices, VectorValueComparator<V> comparator) {
    comparator.attachVector(vector);

    this.indices = indices;

    IntStream.range(0, vector.getValueCount()).forEach(i -> indices.set(i, i));

    this.comparator = comparator;

    quickSort();
  }

  private void quickSort() {
    try (OffHeapIntStack rangeStack = new OffHeapIntStack(indices.getAllocator())) {
      rangeStack.push(0);
      rangeStack.push(indices.getValueCount() - 1);

      while (!rangeStack.isEmpty()) {
        int high = rangeStack.pop();
        int low = rangeStack.pop();

        if (low < high) {
          if (high - low < CHANGE_ALGORITHM_THRESHOLD) {
            InsertionSorter.insertionSort(indices, low, high, comparator);
            continue;
          }

          int mid = partition(low, high, indices, comparator);

          // push the larger part to stack first,
          // to reduce the required stack size
          if (high - mid < mid - low) {
            rangeStack.push(low);
            rangeStack.push(mid - 1);

            rangeStack.push(mid + 1);
            rangeStack.push(high);
          } else {
            rangeStack.push(mid + 1);
            rangeStack.push(high);

            rangeStack.push(low);
            rangeStack.push(mid - 1);
          }
        }
      }
    }
  }

  /** Select the pivot as the median of 3 samples. */
  static <T extends ValueVector> int choosePivot(
      int low, int high, IntVector indices, VectorValueComparator<T> comparator) {
    // we need at least 3 items
    if (high - low + 1 < FixedWidthInPlaceVectorSorter.STOP_CHOOSING_PIVOT_THRESHOLD) {
      return indices.get(low);
    }

    int mid = low + (high - low) / 2;

    // find the median by at most 3 comparisons
    int medianIdx;
    if (comparator.compare(indices.get(low), indices.get(mid)) < 0) {
      if (comparator.compare(indices.get(mid), indices.get(high)) < 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(indices.get(low), indices.get(high)) < 0) {
          medianIdx = high;
        } else {
          medianIdx = low;
        }
      }
    } else {
      if (comparator.compare(indices.get(mid), indices.get(high)) > 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(indices.get(low), indices.get(high)) < 0) {
          medianIdx = low;
        } else {
          medianIdx = high;
        }
      }
    }

    // move the pivot to the low position, if necessary
    if (medianIdx != low) {
      int tmp = indices.get(medianIdx);
      indices.set(medianIdx, indices.get(low));
      indices.set(low, tmp);
      return tmp;
    } else {
      return indices.get(low);
    }
  }

  /**
   * Partition a range of values in a vector into two parts, with elements in one part smaller than
   * elements from the other part. The partition is based on the element indices, so it does not
   * modify the underlying vector.
   *
   * @param low the lower bound of the range.
   * @param high the upper bound of the range.
   * @param indices vector element indices.
   * @param comparator criteria for comparison.
   * @param <T> the vector type.
   * @return the index of the split point.
   */
  public static <T extends ValueVector> int partition(
      int low, int high, IntVector indices, VectorValueComparator<T> comparator) {
    int pivotIndex = choosePivot(low, high, indices, comparator);

    while (low < high) {
      while (low < high && comparator.compare(indices.get(high), pivotIndex) >= 0) {
        high -= 1;
      }
      indices.set(low, indices.get(high));

      while (low < high && comparator.compare(indices.get(low), pivotIndex) <= 0) {
        low += 1;
      }
      indices.set(high, indices.get(low));
    }

    indices.set(low, pivotIndex);
    return low;
  }
}
