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
 * A composite vector comparator compares a number of vectors
 * by a number of inner comparators.
 * <p>
 *   It works by first using the first comparator, if a non-zero value
 *   is returned, it simply returns it. Otherwise, it uses the second comparator,
 *   and so on, until a non-zero value is produced, or all inner comparators have
 *   been used.
 * </p>
 */
public class CompositeVectorComparator extends VectorValueComparator<ValueVector> {

  private final VectorValueComparator[] innerComparators;

  public CompositeVectorComparator(VectorValueComparator[] innerComparators) {
    this.innerComparators = innerComparators;
  }

  @Override
  public int compareNotNull(int index1, int index2) {
    // short-cut for scenarios when the caller can be sure that the vectors are non-nullable.
    for (int i = 0; i < innerComparators.length; i++) {
      int result = innerComparators[i].compareNotNull(index1, index2);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  @Override
  public int compare(int index1, int index2) {
    for (int i = 0; i < innerComparators.length; i++) {
      int result = innerComparators[i].compare(index1, index2);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  @Override
  public VectorValueComparator<ValueVector> createNew() {
    VectorValueComparator[] newInnerComparators = new VectorValueComparator[innerComparators.length];
    for (int i = 0; i < innerComparators.length; i++) {
      newInnerComparators[i] = innerComparators[i].createNew();
    }
    return new CompositeVectorComparator(newInnerComparators);
  }
}
