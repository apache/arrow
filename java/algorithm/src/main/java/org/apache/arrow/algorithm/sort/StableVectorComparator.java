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

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;

/**
 * Stable sorter. It compares values like ordinary comparators.
 * However, when values are equal, it breaks ties by the value indices.
 * Therefore, sort algorithms using this comparator always produce
 * stable sort results.
 * @param <V> type of the vector.
 */
public class StableVectorComparator<V extends ValueVector> extends VectorValueComparator<V> {

  private final VectorValueComparator<V> innerComparator;

  /**
   * Constructs a stable comparator from a given comparator.
   * @param innerComparator the comparator to convert to stable comparator..
   */
  public StableVectorComparator(VectorValueComparator<V> innerComparator) {
    this.innerComparator = innerComparator;
  }

  @Override
  public void attachVector(V vector) {
    super.attachVector(vector);
    innerComparator.attachVector(vector);
  }

  @Override
  public void attachVectors(V vector1, V vector2) {
    Preconditions.checkArgument(vector1 == vector2,
            "Stable comparator only supports comparing values from the same vector");
    super.attachVectors(vector1, vector2);
    innerComparator.attachVectors(vector1, vector2);
  }

  @Override
  public int compareNotNull(int index1, int index2) {
    int result = innerComparator.compare(index1, index2);
    return result != 0 ? result : index1 - index2;
  }


}
