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

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.vector.BaseVariableWidthVector;

/**
 * This class provides the functionality to expand output vectors using a callback mechanism from
 * gandiva.
 */
public class VectorExpander {
  private final BaseVariableWidthVector[] vectors;

  public VectorExpander(BaseVariableWidthVector[] vectors) {
    this.vectors = vectors;
  }

  /**
   * Result of vector expansion.
   */
  public static class ExpandResult {
    public long address;
    public int capacity;

    public ExpandResult(long address, int capacity) {
      this.address = address;
      this.capacity = capacity;
    }
  }

  /**
   * Expand vector at specified index. This is used as a back call from jni, and is only
   * relevant for variable width vectors.
   *
   * @param index index of buffer in the list passed to jni.
   * @param toCapacity the size to which the buffer should be expanded to.
   *
   * @return address and size  of the buffer after expansion.
   */
  public ExpandResult expandOutputVectorAtIndex(int index, int toCapacity) {
    if (index >= vectors.length || vectors[index] == null) {
      throw new IllegalArgumentException("invalid index " + index);
    }

    BaseVariableWidthVector vector = vectors[index];
    while (vector.getDataBuffer().capacity() < toCapacity) {
      vector.reallocDataBuffer();
    }
    return new ExpandResult(
        vector.getDataBuffer().memoryAddress(),
        vector.getDataBuffer().capacity());
  }

}
