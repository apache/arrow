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

package org.apache.arrow.algorithm.swap;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVectorHelper;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Utility for swapping vector data elements.
 * @param <V> vector type.
 */
public class VectorDataSwapper<V extends BaseFixedWidthVector> implements AutoCloseable {

  private final int typeWidth;

  /**
   * The buffer for swapping.
   */
  private final ArrowBuf swapBuf;

  /**
   * A flag indicating if the value in the swap buffer is null.
   */
  private boolean isSwapBufNull;

  public VectorDataSwapper(int typeWidth, BufferAllocator allocator) {
    this.typeWidth = typeWidth;
    swapBuf = allocator.buffer(typeWidth);
  }

  /**
   * Swap vector data.
   * @param vector1 the first vector.
   * @param index1 the index in the first vector.
   * @param vector2 the second vector.
   * @param index2 the index in the second vector.
   */
  public void swap(V vector1, int index1, V vector2, int index2) {
    // vector 1 -> swap
    if (vector1.isNull(index1)) {
      isSwapBufNull = true;
    } else {
      isSwapBufNull = false;
      PlatformDependent.copyMemory(vector1.getDataBuffer().memoryAddress() + index1 * typeWidth,
              swapBuf.memoryAddress(), typeWidth);
    }

    // vector 2 -> vector 1
    if (vector2.isNull(index2)) {
      BitVectorHelper.setValidityBit(vector1.getValidityBuffer(), index1, 0);
    } else {
      BitVectorHelper.setValidityBit(vector1.getValidityBuffer(), index1, 1);
      PlatformDependent.copyMemory(vector2.getDataBuffer().memoryAddress() + index2 * typeWidth,
              vector1.getDataBuffer().memoryAddress() + index1 * typeWidth, typeWidth);
    }

    // swap -> vector2
    if (isSwapBufNull) {
      BitVectorHelper.setValidityBit(vector2.getValidityBuffer(), index2, 0);
    } else {
      BitVectorHelper.setValidityBit(vector2.getValidityBuffer(), index2, 1);
      PlatformDependent.copyMemory(swapBuf.memoryAddress(),
              vector2.getDataBuffer().memoryAddress() + index2 * typeWidth, typeWidth);
    }
  }

  @Override
  public void close() {
    swapBuf.close();
  }
}
