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
package org.apache.arrow.vector;

import java.util.Iterator;

public interface ValueIterableVector<T> extends ValueVector {
  /**
   * Get an Iterable that can be used to iterate over the values in the vector.
   *
   * @return an Iterable for the vector's values
   */
  default Iterator<T> getValueIterator() {
    return new Iterator<T>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index < ValueIterableVector.this.getValueCount();
      }

      @Override
      public T next() {
        return (T) ValueIterableVector.this.getObject(index++);
      }
    };
  }

  /**
   * Get an Iterator for the values in the vector.
   *
   * @return an Iterator for the values in the vector
   */
  default Iterable<T> getValueIterable() {
    return this::getValueIterator;
  }
}
