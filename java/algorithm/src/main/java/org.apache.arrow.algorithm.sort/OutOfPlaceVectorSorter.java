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


import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

/**
 * Basic interface for sorting a vector out-of-place.
 * That is, the sorting is performed on a newly-created vector,
 * and the original vector is not modified.
 * @param <V> the vector type.
 */
public interface OutOfPlaceVectorSorter<V extends ValueVector> extends VectorSorter<V> {

  /**
   * Create the output vector based on the input vector.
   * @param allocator the allocator for creating the new vector.
   * @param inputVector the input vector.
   * @return the output vector.
   */
  V createOutputVector(BufferAllocator allocator, V inputVector);
}
