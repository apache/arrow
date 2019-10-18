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

package org.apache.arrow.algorithm.search;

import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.ValueVector;

/**
 * Search for a particular element in the vector.
 */
public final class VectorSearcher {

  /**
   * Result returned when a search fails.
   */
  public static final int SEARCH_FAIL_RESULT = -1;

  /**
   * Search for a particular element from the key vector in the target vector by binary search.
   * The target vector must be sorted.
   * @param targetVector the vector from which to perform the sort.
   * @param comparator the criterion for the sort.
   * @param keyVector the vector containing the element to search.
   * @param keyIndex the index of the search key in the key vector.
   * @param <V> the vector type.
   * @return the index of a matched element if any, and -1 otherwise.
   */
  public static <V extends ValueVector> int binarySearch(
          V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
    comparator.attachVectors(keyVector, targetVector);

    // perform binary search
    int low = 0;
    int high = targetVector.getValueCount() - 1;

    while (low <= high) {
      int mid = low + (high - low) / 2;
      int cmp = comparator.compare(keyIndex, mid);
      if (cmp < 0) {
        high = mid - 1;
      } else if (cmp > 0) {
        low = mid + 1;
      } else {
        return mid;
      }
    }
    return SEARCH_FAIL_RESULT;
  }

  /**
   * Search for a particular element from the key vector in the target vector by traversing the vector in sequence.
   * @param targetVector the vector from which to perform the sort.
   * @param comparator the criterion for element equality.
   * @param keyVector the vector containing the element to search.
   * @param keyIndex the index of the search key in the key vector.
   * @param <V> the vector type.
   * @return the index of a matched element if any, and -1 otherwise.
   */
  public static <V extends ValueVector> int linearSearch(
          V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
    comparator.attachVectors(keyVector, targetVector);
    for (int i = 0; i < targetVector.getValueCount(); i++) {
      if (comparator.compare(keyIndex, i) == 0) {
        return i;
      }
    }
    return SEARCH_FAIL_RESULT;
  }

  private VectorSearcher() {

  }
}
