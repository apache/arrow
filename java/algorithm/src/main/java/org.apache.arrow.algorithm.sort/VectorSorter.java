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
 * Basic interface for sorting a vector.
 *
 * @param <V> the vector type.
 */
public interface VectorSorter<V extends ValueVector> {

  /**
   * Sort the vector by the given criteria.
   * The sort can be in-place or out-of-place, depending on the algorithm.
   * So the returned vector can be the same vector as, or a different vector from the input vector.
   *
   * @param vec        the vector to sort.
   * @param comparator the criteria for sort.
   * @return the sorted vector.
   */
  V sort(V vec, VectorValueComparator<V> comparator);

  /**
   * Check if this is a in-place sort.
   * @return if the sort is in-place.
   */
  boolean isInPlace();
}
