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

import java.util.Stack;

/**
 * Default in-place sorter for fixed-width vectors.
 * It is based on quick-sort, with average time complexity O(n*log(n)).
 * @param <V> vector type.
 */
public class FixedWidthInPlaceVectorSorter<V extends BaseFixedWidthVector> implements InPlaceVectorSorter<V> {

  private VectorValueComparator<V> comparator;

  /**
   * The vector to sort.
   */
  private V vec;

  /**
   * The buffer to hold the pivot.
   * It always has length 1.
   */
  private V pivotBuffer;

  @Override
  public void sortInPlace(V vec, VectorValueComparator<V> comparator) {
    try {
      this.vec = vec;
      this.comparator = comparator;
      this.pivotBuffer = (V) vec.getField().createVector(vec.getAllocator());
      this.pivotBuffer.allocateNew(1);

      comparator.attachVectors(vec, pivotBuffer);
      quickSort();
    } finally {
      this.pivotBuffer.close();
    }
  }

  private void quickSort() {
    Stack<int[]> rangeStack = new Stack<>();
    int[] range = new int[2];
    range[0] = 0;
    range[1] = vec.getValueCount() - 1;
    rangeStack.push(range);

    while (!rangeStack.isEmpty()) {
      range = rangeStack.pop();
      int low = range[0];
      int high = range[1];
      if (low < high) {
        int mid = partition(low, high);

        int[] lowRange = new int[2];
        lowRange[0] = low;
        lowRange[1] = mid - 1;
        rangeStack.push(lowRange);

        int[] highRange = new int[2];
        highRange[0] = mid + 1;
        highRange[1] = high;
        rangeStack.push(highRange);
      }
    }
  }

  private int partition(int low, int high) {
    pivotBuffer.copyFrom(low, 0, vec);

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
