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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * An out-of-place sorter for vectors of arbitrary type, with time complexity O(n*log(n)). Since it
 * does not make any assumptions about the memory layout of the vector, its performance can be
 * sub-optimal. So if another sorter is applicable ({@link FixedWidthInPlaceVectorSorter}), it
 * should be used in preference.
 *
 * @param <V> vector type.
 */
public class GeneralOutOfPlaceVectorSorter<V extends ValueVector>
    implements OutOfPlaceVectorSorter<V> {

  @Override
  public void sortOutOfPlace(V srcVector, V dstVector, VectorValueComparator<V> comparator) {
    comparator.attachVector(srcVector);

    // check vector capacity
    Preconditions.checkArgument(
        dstVector.getValueCapacity() >= srcVector.getValueCount(),
        "Not enough capacity for the target vector. " + "Expected capacity %s, actual capacity %s",
        srcVector.getValueCount(),
        dstVector.getValueCapacity());

    // sort value indices
    try (IntVector sortedIndices = new IntVector("", srcVector.getAllocator())) {
      sortedIndices.allocateNew(srcVector.getValueCount());
      sortedIndices.setValueCount(srcVector.getValueCount());

      IndexSorter<V> indexSorter = new IndexSorter<>();
      indexSorter.sort(srcVector, sortedIndices, comparator);

      // copy sorted values to the output vector
      for (int dstIndex = 0; dstIndex < sortedIndices.getValueCount(); dstIndex++) {
        int srcIndex = sortedIndices.get(dstIndex);

        dstVector.copyFromSafe(srcIndex, dstIndex, srcVector);
      }
    }
  }
}
