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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;

/**
 * Default out-of-place sorter for fixed-width vectors.
 * It is an out-of-place sort, with time complexity O(n*log(n)).
 * @param <V> vector type.
 */
public class FixedWidthOutOfPlaceVectorSorter<V extends BaseFixedWidthVector> implements OutOfPlaceVectorSorter<V> {

  protected IndexSorter<V> indexSorter = new IndexSorter<>();

  @Override
  public void sortOutOfPlace(V srcVector, V dstVector, VectorValueComparator<V> comparator) {
    comparator.attachVector(srcVector);

    int valueWidth = comparator.getValueWidth();

    // buffers referenced in the sort
    ArrowBuf srcValueBuffer = srcVector.getDataBuffer();
    ArrowBuf dstValidityBuffer = dstVector.getValidityBuffer();
    ArrowBuf dstValueBuffer = dstVector.getDataBuffer();

    // check buffer size
    Preconditions.checkArgument(dstValidityBuffer.capacity() * 8 >= srcVector.getValueCount(),
        "Not enough capacity for the validity buffer of the dst vector. " +
            "Expected capacity %s, actual capacity %s",
        (srcVector.getValueCount() + 7) / 8, dstValidityBuffer.capacity());
    Preconditions.checkArgument(
        dstValueBuffer.capacity() >= srcVector.getValueCount() * srcVector.getTypeWidth(),
        "Not enough capacity for the data buffer of the dst vector. " +
            "Expected capacity %s, actual capacity %s",
        srcVector.getValueCount() * srcVector.getTypeWidth(), dstValueBuffer.capacity());

    // sort value indices
    try (IntVector sortedIndices = new IntVector("", srcVector.getAllocator())) {
      sortedIndices.allocateNew(srcVector.getValueCount());
      sortedIndices.setValueCount(srcVector.getValueCount());
      indexSorter.sort(srcVector, sortedIndices, comparator);

      // copy sorted values to the output vector
      for (int dstIndex = 0; dstIndex < sortedIndices.getValueCount(); dstIndex++) {
        int srcIndex = sortedIndices.get(dstIndex);
        if (srcVector.isNull(srcIndex)) {
          BitVectorHelper.unsetBit(dstValidityBuffer, dstIndex);
        } else {
          BitVectorHelper.setBit(dstValidityBuffer, dstIndex);
          MemoryUtil.UNSAFE.copyMemory(
                  srcValueBuffer.memoryAddress() + srcIndex * valueWidth,
                  dstValueBuffer.memoryAddress() + dstIndex * valueWidth,
                  valueWidth);
        }
      }
    }
  }
}
