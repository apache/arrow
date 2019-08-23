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

import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;

import io.netty.buffer.ArrowBuf;

/**
 * Utilities for vector deduplication.
 */
class DeduplicationUtils {

  /**
   * Gets the start positions of the first distinct values in a vector.
   * @param vector the target vector.
   * @param runStarts the bit set to hold the start positions.
   * @param <V> vector type.
   */
  public static <V extends ValueVector> void populateRunStartIndicators(V vector, ArrowBuf runStarts) {
    int bufSize = DataSizeRoundingUtil.divideBy8Ceil(vector.getValueCount());
    Preconditions.checkArgument(runStarts.capacity() >= bufSize);
    runStarts.setZero(0, bufSize);

    BitVectorHelper.setValidityBitToOne(runStarts, 0);

    for (int i = 1; i < vector.getValueCount(); i++) {
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(
              vector, i - 1, i, /* length */1, /* need check type*/false);
      if (!visitor.equals(vector)) {
        BitVectorHelper.setValidityBitToOne(runStarts, i);
      }
    }
  }

  /**
   * Gets the run lengths, given the start positions.
   * @param runStarts the bit set for start positions.
   * @param runLengths the run length vector to populate.
   * @param valueCount the number of values in the bit set.
   */
  public static void populateRunLengths(ArrowBuf runStarts,  IntVector runLengths, int valueCount) {
    int curStart = 0;
    int lengthIndex = 0;
    for (int i = 1; i < valueCount; i++) {
      if (BitVectorHelper.get(runStarts, i) != 0) {
        // we get a new distinct value
        runLengths.setSafe(lengthIndex++, i - curStart);
        curStart = i;
      }
    }

    // process the last value
    runLengths.setSafe(lengthIndex++, valueCount - curStart);
    runLengths.setValueCount(lengthIndex);
  }

  /**
   * Gets distinct values from the input vector by removing adjacent
   * duplicated values.
   * @param indicators the bit set containing the start positions of disctinct values.
   * @param inputVector the input vector.
   * @param outputVector the output vector.
   * @param <V> vector type.
   */
  public static <V extends ValueVector> void populateDeduplicatedValues(
          ArrowBuf indicators, V inputVector, V outputVector) {
    int dstIdx = 0;
    for (int srcIdx = 0; srcIdx < inputVector.getValueCount(); srcIdx++) {
      if (BitVectorHelper.get(indicators, srcIdx) != 0) {
        outputVector.copyFromSafe(srcIdx, dstIdx++, inputVector);
      }
    }
    outputVector.setValueCount(dstIdx);
  }
}
