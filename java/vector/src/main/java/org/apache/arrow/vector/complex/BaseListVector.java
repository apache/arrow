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

package org.apache.arrow.vector.complex;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;

/**
 * Abstraction for all list type vectors.
 */
public abstract class BaseListVector extends BaseValueVector implements PromotableVector {

  protected BaseListVector(BufferAllocator allocator) {
    super(allocator);
  }

  /**
   * Get data vector start index with the given list index.
   */
  public abstract int getStartIndex(int index);

  /**
   * Get data vector end index with the given list index.
   */
  public abstract int getEndIndex(int index);

  /**
   * Get the inner data vector for this list vector.
   */
  public abstract FieldVector getDataVector();

  /**
   * Set offset buffer value at (index + 1) if needed.
   */
  public abstract void setOffsetBufferValueIfNeeded(int index, int value);
}
