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

package org.apache.arrow.vector.sort;

import java.util.Comparator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

/**
 * Compare two values at the given indices in the vectors.
 * This is used for vector sorting.
 * @param <V> type of the vector.
 */
public abstract class VectorValueComparator<V extends ValueVector> implements Comparator<Integer> {

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
    this.vector1 = vector;
    this.vector2 = vector;
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
   * Create a new vector with the same type. This can be used for out-of-place sort.
   * @param allocator the allocator for creating the new vector.
   * @return the new vector.
   */
  public abstract V newVector(BufferAllocator allocator);
}
