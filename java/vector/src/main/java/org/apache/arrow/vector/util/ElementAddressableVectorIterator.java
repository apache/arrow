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

package org.apache.arrow.vector.util;

import java.util.Iterator;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.apache.arrow.vector.ElementAddressableVector;

/**
 * Iterator for traversing elements of a {@link ElementAddressableVector}.
 * @param <T> vector type.
 */
public class ElementAddressableVectorIterator<T extends ElementAddressableVector>
        implements Iterator<ArrowBufPointer> {

  private final T vector;

  /**
   * Index of the next element to access.
   */
  private int index = 0;

  private final ArrowBufPointer reusablePointer;

  /**
   * Constructs an iterator for the {@link ElementAddressableVector}.
   * @param vector the vector to iterate.
   */
  public ElementAddressableVectorIterator(T vector) {
    this(vector, SimpleHasher.INSTANCE);
  }

  /**
   * Constructs an iterator for the {@link ElementAddressableVector}.
   * @param vector the vector to iterate.
   * @param hasher the hasher to calculate the hash code.
   */
  public ElementAddressableVectorIterator(T vector, ArrowBufHasher hasher) {
    this.vector = vector;
    reusablePointer = new ArrowBufPointer(hasher);
  }

  @Override
  public boolean hasNext() {
    return index < vector.getValueCount();
  }

  /**
   * Retrieves the next pointer from the vector.
   * @return the pointer pointing to the next element in the vector.
   *     Note that the returned pointer is only valid before the next call to this method.
   */
  @Override
  public ArrowBufPointer next() {
    vector.getDataPointer(index, reusablePointer);
    index += 1;
    return reusablePointer;
  }

  /**
   * Retrieves the next pointer from the vector.
   * @param outPointer the pointer to populate.
   */
  public void next(ArrowBufPointer outPointer) {
    vector.getDataPointer(index, outPointer);
    index += 1;
  }
}
