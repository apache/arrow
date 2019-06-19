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
  private IntVector indices;

  /**
   * The buffer to store the pivot which is used in quick sort.
   * This vector always have a length of 1.
   */
  private IntVector pivotBuffer;

  /**
   * Sorts indices, by quick-sort. Suppose the vector is denoted by v.
   * After calling this method, the following relations hold:
   * v(indices[0]) <= v(indices[1]) <= ...
   * @param vector the vector whose indices need to be sorted.
   * @param indices the vector for storing the sorted indices.
   * @param comparator the comparator to sort indices.
   */
  public void sort(V vector, IntVector indices, VectorValueComparator<V> comparator) {
    comparator.attachVector(vector);

    this.indices = indices;

    this.pivotBuffer = new IntVector("", vector.getAllocator());
    pivotBuffer.allocateNew(1);
    pivotBuffer.setValueCount(1);

    IntStream.range(0, vector.getValueCount()).forEach(i -> indices.set(i, i));

    this.comparator = comparator;

    quickSort(0, indices.getValueCount() - 1);

    pivotBuffer.close();
  }

  private void quickSort(int low, int high) {
    if (low < high) {
      int mid = partition(low, high);
      quickSort(low, mid - 1);
      quickSort(mid + 1, high);
    }
  }

  private int partition(int low, int high) {
    copyIntElement(indices, low, pivotBuffer, 0);

    while (low < high) {
      while (low < high && comparator.compare(indices.get(high), pivotBuffer.get(0)) >= 0) {
        high -= 1;
      }
      copyIntElement(indices, high, indices, low);

      while (low < high && comparator.compare(indices.get(low), pivotBuffer.get(0)) <= 0) {
        low += 1;
      }
      copyIntElement(indices, low, indices, high);
    }

    copyIntElement(pivotBuffer, 0, indices, low);
    return low;
  }

  private void copyIntElement(IntVector srcVector, int srcIndex, IntVector dstVector, int dstIndex) {
    if (srcVector.isNull(srcIndex)) {
      dstVector.setNull(dstIndex);
    } else {
      dstVector.set(dstIndex, 1, srcVector.get(srcIndex));
    }
  }
}
