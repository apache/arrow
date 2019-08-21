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

package org.apache.arrow.algorithm.deduplicate;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;

import io.netty.buffer.ArrowBuf;

/**
 * Remove adjacent equal elements from a vector.
 * If the vector is sorted, it removes all duplicated values in the vector.
 * @param <V> vector type.
 */
public class VectorRunDeduplicator<V extends ValueVector> implements AutoCloseable {

  /**
   * Bit set for distinct values.
   * If the value at some index is not equal to the previous value,
   * its bit is set to 1, otherwise its bit is set to 0.
   */
  private ArrowBuf distinctValueBuffer;

  /**
   * The vector to deduplicate.
   */
  private final V vector;

  private final BufferAllocator allocator;

  /**
   * Constructs a vector run deduplicator for a given vector.
   * @param vector the vector to dedulicate.  Ownership is NOT taken.
   * @param allocator the allocator used for allocating buffers for start indices.
   */
  public VectorRunDeduplicator(V vector, BufferAllocator allocator) {
    this.vector = vector;
    this.allocator = allocator;
  }

  private void createDistinctValueBuffer() {
    Preconditions.checkArgument(distinctValueBuffer == null);
    int bufSize = DataSizeRoundingUtil.divideBy8Ceil(vector.getValueCount());
    distinctValueBuffer = allocator.buffer(bufSize);
    DeduplicationUtils.populateRunStartIndicators(vector, distinctValueBuffer);
  }

  /**
   * Gets the number of values which are different from their predecessor.
   * @return the run count.
   */
  public int getRunCount() {
    if (distinctValueBuffer == null) {
      createDistinctValueBuffer();
    }
    return vector.getValueCount() - BitVectorHelper.getNullCount(distinctValueBuffer, vector.getValueCount());
  }

  /**
   * Gets the vector with deduplicated adjacent values removed.
   * @param outVector the output vector.
   */
  public void populateDeduplicatedValues(V outVector) {
    if (distinctValueBuffer == null) {
      createDistinctValueBuffer();
    }

    DeduplicationUtils.populateDeduplicatedValues(distinctValueBuffer, vector, outVector);
  }

  /**
   * Gets the length of each distinct value.
   * @param lengthVector the vector for holding length values.
   */
  public void populateRunLengths(IntVector lengthVector) {
    if (distinctValueBuffer == null) {
      createDistinctValueBuffer();
    }

    DeduplicationUtils.populateRunLengths(distinctValueBuffer, lengthVector, vector.getValueCount());
  }

  @Override
  public void close() {
    if (distinctValueBuffer != null) {
      distinctValueBuffer.close();
      distinctValueBuffer = null;
    }
  }
}
