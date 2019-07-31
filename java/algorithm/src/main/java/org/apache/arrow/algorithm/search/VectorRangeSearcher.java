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
 * Search for the range of a particular element in the target vector.
 */
public class VectorRangeSearcher {

  /**
   * Result returned when a search fails.
   */
  public static final int SEARCH_FAIL_RESULT = -1;

  /**
   * Search for the first occurrence of an element.
   * The search is based on the binary search algorithm. So the target vector must be sorted.
   * @param targetVector the vector from which to perform the search.
   * @param comparator the criterion for the comparison.
   * @param keyVector the vector containing the element to search.
   * @param keyIndex the index of the search key in the key vector.
   * @param <V> the vector type.
   * @return the index of the first matched element if any, and -1 otherwise.
   */
  public static <V extends ValueVector> int getFirstMatch(
          V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
    comparator.attachVectors(keyVector, targetVector);

    int low = 0;
    int high = targetVector.getValueCount() - 1;

    while (low <= high) {
      int mid = low + (high - low) / 2;
      int result = comparator.compare(keyIndex, mid);
      if (result < 0) {
        // the key is smaller
        high = mid - 1;
      } else if (result > 0) {
        // the key is larger
        low = mid + 1;
      } else {
        // the key equals the mid value, find the lower bound by going left-ward.

        // compare with the left neighbour
        int left = mid - 1;
        if (left == -1) {
          // this is the first value in the vector
          return mid;
        } else {
          int leftResult = comparator.compare(keyIndex, left);
          if (leftResult > 0) {
            // the key is greater than the left neighbour, and equal to the current one
            // we find it
            return mid;
          } else if (leftResult == 0) {
            // the left neighbour is also equal, continue to go left
            high = mid - 1;
          } else {
            // the key is larger than the left neighbour, this is not possible
            throw new IllegalStateException("The target vector is not sorted ");
          }
        }
      }
    }
    return SEARCH_FAIL_RESULT;
  }

  /**
   * Search for the last occurrence of an element.
   * The search is based on the binary search algorithm. So the target vector must be sorted.
   * @param targetVector the vector from which to perform the search.
   * @param comparator the criterion for the comparison.
   * @param keyVector the vector containing the element to search.
   * @param keyIndex the index of the search key in the key vector.
   * @param <V> the vector type.
   * @return the index of the last matched element if any, and -1 otherwise.
   */
  public static <V extends ValueVector> int getLastMatch(
          V targetVector, VectorValueComparator<V> comparator, V keyVector, int keyIndex) {
    comparator.attachVectors(keyVector, targetVector);

    int low = 0;
    int high = targetVector.getValueCount() - 1;

    while (low <= high) {
      int mid = low + (high - low) / 2;
      int result = comparator.compare(keyIndex, mid);
      if (result < 0) {
        // the key is smaller
        high = mid - 1;
      } else if (result > 0) {
        // the key is larger
        low = mid + 1;
      } else {
        // the key equals the mid value, find the upper bound by going right-ward.

        // compare with the right neighbour
        int right = mid + 1;
        if (right == targetVector.getValueCount()) {
          // this is the last value in the vector
          return mid;
        } else {
          int rightResult = comparator.compare(keyIndex, right);
          if (rightResult < 0) {
            // the key is smaller than the right neighbour, and equal to the current one
            // we find it
            return mid;
          } else if (rightResult == 0) {
            // the right neighbour is also equal, continue to go right
            low = mid + 1;
          } else {
            // the key is smaller than the right neighbour, this is not possible
            throw new IllegalStateException("The target vector is not sorted ");
          }
        }
      }
    }
    return SEARCH_FAIL_RESULT;
  }
}
