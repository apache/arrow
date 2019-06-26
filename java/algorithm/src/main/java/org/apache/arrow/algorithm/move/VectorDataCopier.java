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

package org.apache.arrow.algorithm.move;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Utilities for moving data between vectors.
 */
public class VectorDataCopier {

  /**
   * Copy data for fixed-width vectors.
   * @param srcVector the source vector.
   * @param srcIndex the data index in the source vector.
   * @param dstVector the destination vector.
   * @param dstIndex the data index in the destination vector.
   * @param typeWidth the width of the data type.
   */
  public static void fixedWidthDataCopy(
          BaseFixedWidthVector srcVector, int srcIndex, BaseFixedWidthVector dstVector, int dstIndex, int typeWidth) {
    if (srcVector.isNull(srcIndex)) {
      BitVectorHelper.setValidityBit(dstVector.getValidityBuffer(), dstIndex, 0);
    } else {
      BitVectorHelper.setValidityBit(dstVector.getValidityBuffer(), dstIndex, 1);
      PlatformDependent.copyMemory(srcVector.getDataBuffer().memoryAddress() + srcIndex * typeWidth,
              dstVector.getDataBuffer().memoryAddress() + dstIndex * typeWidth, typeWidth);
    }
  }

  /**
   * Copy data for variable-width vectors.
   * @param srcVector the source vector.
   * @param srcIndex the data index in the source vector.
   * @param dstVector the destination vector.
   * @param dstIndex the data index in the destination vector.
   */
  public static void variableWidthDataCopy(
          BaseVariableWidthVector srcVector, int srcIndex, BaseVariableWidthVector dstVector, int dstIndex) {
    if (srcVector.isNull(srcIndex)) {
      BitVectorHelper.setValidityBit(dstVector.getValidityBuffer(), dstIndex, 0);

      // set offset buffer
      ArrowBuf dstOffsetBuf = dstVector.getOffsetBuffer();
      int dstStart = dstOffsetBuf.getInt(dstIndex * BaseVariableWidthVector.OFFSET_WIDTH);
      dstOffsetBuf.setInt((dstIndex + 1)  * BaseVariableWidthVector.OFFSET_WIDTH, dstStart);
    } else {
      BitVectorHelper.setValidityBit(dstVector.getValidityBuffer(), dstIndex, 1);

      // set offset buffer
      ArrowBuf srcOffsetBuf = srcVector.getOffsetBuffer();
      int srcStart = srcOffsetBuf.getInt(srcIndex * BaseVariableWidthVector.OFFSET_WIDTH);
      int dataLength = srcOffsetBuf.getInt((srcIndex + 1)  * BaseVariableWidthVector.OFFSET_WIDTH) - srcStart;

      ArrowBuf dstOffsetBuf = dstVector.getOffsetBuffer();
      int dstStart = dstOffsetBuf.getInt(dstIndex * BaseVariableWidthVector.OFFSET_WIDTH);
      dstOffsetBuf.setInt((dstIndex + 1)  * BaseVariableWidthVector.OFFSET_WIDTH, dstStart + dataLength);

      // copy data buffer
      PlatformDependent.copyMemory(srcVector.getDataBuffer().memoryAddress() + srcStart,
              dstVector.getDataBuffer().memoryAddress() + dstStart, dataLength);
      dstVector.setLastSet(dstIndex);
    }
  }

  private VectorDataCopier() {

  }
}
