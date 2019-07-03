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
      quickSort(0, vec.getValueCount() - 1);
    } finally {
      this.pivotBuffer.close();
    }
  }

  private void quickSort(int low, int high) {
    if (low < high) {
      int mid = partition(low, high);
      quickSort(low, mid - 1);
      quickSort(mid + 1, high);
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
