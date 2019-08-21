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
 * Compare two values at the given indices in the vectors.
 * This is used for vector sorting.
 * @param <V> type of the vector.
 */
public abstract class VectorValueComparator<V extends ValueVector> {

  /**
   * The first vector to compare.
   */
  protected V vector1;

  /**
   * The second vector to compare.
   */
  protected V vector2;

  /**
   * Width of the vector value. For variable-length vectors, this value makes no sense.
   */
  protected int valueWidth;

  /**
   * Constructor for variable-width vectors.
   */
  protected VectorValueComparator() {

  }

  /**
   * Constructor for fixed-width vectors.
   * @param valueWidth the record width (in bytes).
   */
  protected VectorValueComparator(int valueWidth) {
    this.valueWidth = valueWidth;
  }

  public int getValueWidth() {
    return valueWidth;
  }

  /**
   * Attach both vectors to compare to the same input vector.
   * @param vector the vector to attach.
   */
  public void attachVector(V vector) {
    attachVectors(vector, vector);
  }

  /**
   * Attach vectors to compare.
   * @param vector1 the first vector to compare.
   * @param vector2 the second vector to compare.
   */
  public void attachVectors(V vector1, V vector2) {
    this.vector1 = vector1;
    this.vector2 = vector2;
  }

  /**
   * Compare two values, given their indices.
   * @param index1 index of the first value to compare.
   * @param index2 index of the second value to compare.
   * @return an integer greater than 0, if the first value is greater;
   *     an integer smaller than 0, if the first value is smaller; or 0, if both
   *     values are equal.
   */
  public int compare(int index1, int index2) {
    boolean isNull1 = vector1.isNull(index1);
    boolean isNull2 = vector2.isNull(index2);

    if (isNull1 || isNull2) {
      if (isNull1 && isNull2) {
        return 0;
      } else if (isNull1) {
        // null is smaller
        return -1;
      } else {
        return 1;
      }
    }
    return compareNotNull(index1, index2);
  }

  /**
   * Compare two values, given their indices.
   * This is a fast path for comparing non-null values, so the caller
   * must make sure that values at both indices are not null.
   * @param index1 index of the first value to compare.
   * @param index2 index of the second value to compare.
   * @return an integer greater than 0, if the first value is greater;
   *     an integer smaller than 0, if the first value is smaller; or 0, if both
   *     values are equal.
   */
  public abstract int compareNotNull(int index1, int index2);
}
