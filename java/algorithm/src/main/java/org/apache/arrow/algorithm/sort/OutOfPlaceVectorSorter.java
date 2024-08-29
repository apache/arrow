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
 * Basic interface for sorting a vector out-of-place.
 * That is, the sorting is performed on a newly-created vector,
 * and the original vector is not modified.
 * @param <V> the vector type.
 */
public interface OutOfPlaceVectorSorter<V extends ValueVector> {

  /**
   * Sort a vector out-of-place.
   * @param inVec the input vector.
   * @param outVec the output vector, which has the same size as the input vector.
   * @param comparator the criteria for sort.
   */
  void sortOutOfPlace(V inVec, V outVec, VectorValueComparator<V> comparator);
}
