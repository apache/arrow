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

/**
 * Default in-place sorter for fixed-width vectors. It is based on quick-sort, with average time
 * complexity O(n*log(n)).
 *
 * @param <V> vector type.
 */
public class FixedWidthInPlaceVectorSorter<V extends BaseFixedWidthVector>
    implements InPlaceVectorSorter<V> {

  /**
   * If the number of items is smaller than this threshold, we will use another algorithm to sort
   * the data.
   */
  public static final int CHANGE_ALGORITHM_THRESHOLD = 15;

  static final int STOP_CHOOSING_PIVOT_THRESHOLD = 3;

  VectorValueComparator<V> comparator;

  /** The vector to sort. */
  V vec;

  /** The buffer to hold the pivot. It always has length 1. */
  V pivotBuffer;

  @Override
  public void sortInPlace(V vec, VectorValueComparator<V> comparator) {
    try {
      this.vec = vec;
      this.comparator = comparator;
      this.pivotBuffer = (V) vec.getField().createVector(vec.getAllocator());
      this.pivotBuffer.allocateNew(1);
      this.pivotBuffer.setValueCount(1);

      comparator.attachVectors(vec, pivotBuffer);
      quickSort();
    } finally {
      this.pivotBuffer.close();
    }
  }

  private void quickSort() {
    try (OffHeapIntStack rangeStack = new OffHeapIntStack(vec.getAllocator())) {
      rangeStack.push(0);
      rangeStack.push(vec.getValueCount() - 1);

      while (!rangeStack.isEmpty()) {
        int high = rangeStack.pop();
        int low = rangeStack.pop();
        if (low < high) {
          if (high - low < CHANGE_ALGORITHM_THRESHOLD) {
            // switch to insertion sort
            InsertionSorter.insertionSort(vec, low, high, comparator, pivotBuffer);
            continue;
          }

          int mid = partition(low, high);

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
  void choosePivot(int low, int high) {
    // we need at least 3 items
    if (high - low + 1 < STOP_CHOOSING_PIVOT_THRESHOLD) {
      pivotBuffer.copyFrom(low, 0, vec);
      return;
    }

    comparator.attachVector(vec);
    int mid = low + (high - low) / 2;

    // find the median by at most 3 comparisons
    int medianIdx;
    if (comparator.compare(low, mid) < 0) {
      if (comparator.compare(mid, high) < 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(low, high) < 0) {
          medianIdx = high;
        } else {
          medianIdx = low;
        }
      }
    } else {
      if (comparator.compare(mid, high) > 0) {
        medianIdx = mid;
      } else {
        if (comparator.compare(low, high) < 0) {
          medianIdx = low;
        } else {
          medianIdx = high;
        }
      }
    }

    // move the pivot to the low position, if necessary
    if (medianIdx != low) {
      pivotBuffer.copyFrom(medianIdx, 0, vec);
      vec.copyFrom(low, medianIdx, vec);
      vec.copyFrom(0, low, pivotBuffer);
    } else {
      pivotBuffer.copyFrom(low, 0, vec);
    }

    comparator.attachVectors(vec, pivotBuffer);
  }

  private int partition(int low, int high) {
    choosePivot(low, high);

    while (low < high) {
      while (low < high && comparator.compare(high, 0) >= 0) {
        high -= 1;
      }
      vec.copyFrom(high, low, vec);

      while (low < high && comparator.compare(low, 0) <= 0) {
        low += 1;
      }
      vec.copyFrom(low, high, vec);
    }

    vec.copyFrom(0, low, pivotBuffer);
    return low;
  }
}
