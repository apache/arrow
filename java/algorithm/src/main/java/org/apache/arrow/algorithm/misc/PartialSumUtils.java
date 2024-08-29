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

package org.apache.arrow.algorithm.misc;

import org.apache.arrow.vector.BaseIntVector;

/**
 * Partial sum related utilities.
 */
public class PartialSumUtils {

  /**
   * Converts an input vector to a partial sum vector.
   * This is an inverse operation of {@link PartialSumUtils#toDeltaVector(BaseIntVector, BaseIntVector)}.
   * Suppose we have input vector a and output vector b.
   * Then we have b(0) = sumBase; b(i + 1) = b(i) + a(i) (i = 0, 1, 2, ...).
   * @param deltaVector the input vector.
   * @param partialSumVector the output vector.
   * @param sumBase the base of the partial sums.
   */
  public static void toPartialSumVector(BaseIntVector deltaVector, BaseIntVector partialSumVector, long sumBase) {
    long sum = sumBase;
    partialSumVector.setWithPossibleTruncate(0, sumBase);

    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      sum += deltaVector.getValueAsLong(i);
      partialSumVector.setWithPossibleTruncate(i + 1, sum);
    }
    partialSumVector.setValueCount(deltaVector.getValueCount() + 1);
  }

  /**
   * Converts an input vector to the delta vector.
   * This is an inverse operation of {@link PartialSumUtils#toPartialSumVector(BaseIntVector, BaseIntVector, long)}.
   * Suppose we have input vector a and output vector b.
   * Then we have b(i) = a(i + 1) - a(i)  (i = 0, 1, 2, ...).
   * @param partialSumVector the input vector.
   * @param deltaVector the output vector.
   */
  public static void toDeltaVector(BaseIntVector partialSumVector, BaseIntVector deltaVector) {
    for (int i = 0; i < partialSumVector.getValueCount() - 1; i++) {
      long delta = partialSumVector.getValueAsLong(i + 1) - partialSumVector.getValueAsLong(i);
      deltaVector.setWithPossibleTruncate(i, delta);
    }
    deltaVector.setValueCount(partialSumVector.getValueCount() - 1);
  }

  /**
   * Given a value and a partial sum vector, finds its position in the partial sum vector.
   * In particular, given an integer value a and partial sum vector v, we try to find a
   * position i, so that v(i) <= a < v(i + 1).
   * The algorithm is based on binary search, so it takes O(log(n)) time, where n is
   * the length of the partial sum vector.
   * @param partialSumVector the input partial sum vector.
   * @param value the value to search.
   * @return the position in the partial sum vector, if any, or -1, if none is found.
   */
  public static int findPositionInPartialSumVector(BaseIntVector partialSumVector, long value) {
    if (value < partialSumVector.getValueAsLong(0) ||
            value >= partialSumVector.getValueAsLong(partialSumVector.getValueCount() - 1)) {
      return -1;
    }

    int low = 0;
    int high = partialSumVector.getValueCount() - 1;
    while (low <= high) {
      int mid = low + (high - low) / 2;
      long midValue = partialSumVector.getValueAsLong(mid);

      if (midValue <= value) {
        if (mid == partialSumVector.getValueCount() - 1) {
          // the mid is the last element, we have found it
          return mid;
        }
        long nextMidValue = partialSumVector.getValueAsLong(mid + 1);
        if (value < nextMidValue) {
          // midValue <= value < nextMidValue
          // this is exactly what we want.
          return mid;
        } else {
          // value >= nextMidValue
          // continue to search from the next value on the right
          low = mid + 1;
        }
      } else {
        // midValue > value
        long prevMidValue = partialSumVector.getValueAsLong(mid - 1);
        if (prevMidValue <= value) {
          // prevMidValue <= value < midValue
          // this is exactly what we want
          return mid - 1;
        } else {
          // prevMidValue > value
          // continue to search from the previous value on the left
          high = mid - 1;
        }
      }
    }
    throw new IllegalStateException("Should never get here");
  }

  private PartialSumUtils() {
  }
}
