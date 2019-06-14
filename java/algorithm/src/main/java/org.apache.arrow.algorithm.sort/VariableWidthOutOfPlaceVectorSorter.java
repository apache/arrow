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

import java.util.stream.IntStream;

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Default sorter for variable-width vectors.
 * It is an out-of-place sort, with time complexity O(n*log(n)).
 * @param <V> vector type.
 */
public class VariableWidthOutOfPlaceVectorSorter<V extends BaseVariableWidthVector> implements VectorSorter<V> {

  private final IndexSorter<V> indexSorter = new IndexSorter<>();

  @Override
  public V sort(V srcVector, VectorValueComparator<V> comparator) {
    comparator.attachVector(srcVector);

    // create the output vector
    V dstVector = comparator.newVector(srcVector.getAllocator());
    dstVector.allocateNew(srcVector.getByteCapacity(), srcVector.getValueCount());

    // buffers referenced in the sort
    ArrowBuf srcValueBuffer = srcVector.getDataBuffer();
    ArrowBuf srcOffsetBuffer = srcVector.getOffsetBuffer();
    ArrowBuf dstValidityBuffer = dstVector.getValidityBuffer();
    ArrowBuf dstValueBuffer = dstVector.getDataBuffer();
    ArrowBuf dstOffsetBuffer = dstVector.getOffsetBuffer();

    // sort value indices
    int[] sortedIndices = IntStream.range(0, srcVector.getValueCount()).toArray();
    indexSorter.sort(sortedIndices, comparator);

    int dstIndex = 0;
    int dstOffset = 0;
    dstOffsetBuffer.setInt(dstIndex, dstOffset);

    // copy sorted values to the output vector
    for (int srcIndex : sortedIndices) {
      if (srcVector.isNull(srcIndex)) {
        BitVectorHelper.setValidityBit(dstValidityBuffer, dstIndex, 0);
      } else {
        BitVectorHelper.setValidityBit(dstValidityBuffer, dstIndex, 1);
        int srcOffset = srcOffsetBuffer.getInt(srcIndex * BaseVariableWidthVector.OFFSET_WIDTH);
        int valueLength = srcOffsetBuffer.getInt((srcIndex + 1) * BaseVariableWidthVector.OFFSET_WIDTH) - srcOffset;
        PlatformDependent.copyMemory(
                srcValueBuffer.memoryAddress() + srcOffset,
                dstValueBuffer.memoryAddress() + dstOffset,
                valueLength);
        dstOffset += valueLength;
      }
      dstIndex += 1;
      dstOffsetBuffer.setInt(dstIndex * BaseVariableWidthVector.OFFSET_WIDTH, dstOffset);
    }
    dstVector.setLastSet(dstIndex - 1);
    dstVector.setValueCount(srcVector.getValueCount());
    return dstVector;
  }

  @Override
  public boolean isInPlace() {
    return false;
  }
}
