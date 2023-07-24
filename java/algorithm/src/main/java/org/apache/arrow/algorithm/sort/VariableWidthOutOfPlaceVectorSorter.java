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
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;

/**
 * Default sorter for variable-width vectors.
 * It is an out-of-place sort, with time complexity O(n*log(n)).
 * @param <V> vector type.
 */
public class VariableWidthOutOfPlaceVectorSorter<V extends BaseVariableWidthVector>
        implements OutOfPlaceVectorSorter<V> {

  protected IndexSorter<V> indexSorter = new IndexSorter<>();

  @Override
  public void sortOutOfPlace(V srcVector, V dstVector, VectorValueComparator<V> comparator) {
    comparator.attachVector(srcVector);

    // buffers referenced in the sort
    ArrowBuf srcValueBuffer = srcVector.getDataBuffer();
    ArrowBuf srcOffsetBuffer = srcVector.getOffsetBuffer();
    ArrowBuf dstValidityBuffer = dstVector.getValidityBuffer();
    ArrowBuf dstValueBuffer = dstVector.getDataBuffer();
    ArrowBuf dstOffsetBuffer = dstVector.getOffsetBuffer();

    // check buffer size
    Preconditions.checkArgument(dstValidityBuffer.capacity() * 8 >= srcVector.getValueCount(),
        "Not enough capacity for the validity buffer of the dst vector. " +
            "Expected capacity %s, actual capacity %s",
        (srcVector.getValueCount() + 7) / 8, dstValidityBuffer.capacity());
    Preconditions.checkArgument(
        dstOffsetBuffer.capacity() >= (srcVector.getValueCount() + 1) * BaseVariableWidthVector.OFFSET_WIDTH,
        "Not enough capacity for the offset buffer of the dst vector. " +
            "Expected capacity %s, actual capacity %s",
        (srcVector.getValueCount() + 1) * BaseVariableWidthVector.OFFSET_WIDTH, dstOffsetBuffer.capacity());
    long dataSize = srcVector.getOffsetBuffer().getInt(
        srcVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
    Preconditions.checkArgument(
        dstValueBuffer.capacity() >= dataSize, "No enough capacity for the data buffer of the dst vector. " +
            "Expected capacity %s, actual capacity %s", dataSize, dstValueBuffer.capacity());

    // sort value indices
    try (IntVector sortedIndices = new IntVector("", srcVector.getAllocator())) {
      sortedIndices.allocateNew(srcVector.getValueCount());
      sortedIndices.setValueCount(srcVector.getValueCount());
      indexSorter.sort(srcVector, sortedIndices, comparator);

      int dstOffset = 0;
      dstOffsetBuffer.setInt(0, 0);

      // copy sorted values to the output vector
      for (int dstIndex = 0; dstIndex < sortedIndices.getValueCount(); dstIndex++) {
        int srcIndex = sortedIndices.get(dstIndex);
        if (srcVector.isNull(srcIndex)) {
          BitVectorHelper.unsetBit(dstValidityBuffer, dstIndex);
        } else {
          BitVectorHelper.setBit(dstValidityBuffer, dstIndex);
          int srcOffset = srcOffsetBuffer.getInt(srcIndex * BaseVariableWidthVector.OFFSET_WIDTH);
          int valueLength = srcOffsetBuffer.getInt((srcIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH) - srcOffset;
          MemoryUtil.UNSAFE.copyMemory(
                  srcValueBuffer.memoryAddress() + srcOffset,
                  dstValueBuffer.memoryAddress() + dstOffset,
                  valueLength);
          dstOffset += valueLength;
        }
        dstOffsetBuffer.setInt((dstIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH, dstOffset);
      }
    }
  }
}
