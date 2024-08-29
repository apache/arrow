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

package org.apache.arrow.vector.compare;

import org.apache.arrow.vector.ValueVector;

/**
 * A function to determine if two vectors are equal at specified positions.
 * @param <V> the vector type.
 */
public interface VectorValueEqualizer<V extends ValueVector> extends Cloneable {

  /**
   * Checks if the vectors are equal at the given positions, given that the values
   * at both positions are non-null.
   * @param vector1 the first vector.
   * @param index1 index in the first vector.
   * @param vector2 the second vector.
   * @param index2 index in the second vector.
   * @return true if the two values are considered to be equal, and false otherwise.
   */
  boolean valuesEqual(V vector1, int index1, V vector2, int index2);

  /**
   * Creates a equalizer of the same type.
   * @return the newly created equalizer.
   */
  VectorValueEqualizer<V> clone();
}
