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

package org.apache.arrow.algorithm.compact;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Utility for compact/uncompact vector data element.
 */
public class VectorDataCompactor {

  /**
   * Compact the vector and reset valueCount, meanwhile generate a {@link BitVector} to trace non-null indices.
   * @param vector the vector to compact.
   */
  public static <V extends BaseFixedWidthVector> BitVector compact(V vector) {

    int valueCount = vector.getValueCount();

    BitVector bitVector = new BitVector("", vector.getAllocator());
    bitVector.allocateNew(valueCount);
    bitVector.setValueCount(valueCount);

    //record index from 0 to valueCount
    int recordIndex = 0;
    //index indicates the location to write value
    int writeIndex = 0;

    while (recordIndex < valueCount) {
      if (vector.isNull(recordIndex)) {
        recordIndex++;
      } else {
        bitVector.setSafe(recordIndex, 1);
        //copy the value at recordIndex to writeIndex
        vector.copyFromSafe(recordIndex, writeIndex, vector);
        recordIndex++;
        writeIndex++;
      }
    }
    vector.setValueCount(writeIndex);

    return bitVector;

  }

  /**
   * Recovery the vector with the given compacted vector and {@link BitVector}.
   * @param bitVector the vector which holds non-null indices
   * @param compactedVector the compacted vector.
   */
  public static <V extends BaseFixedWidthVector> V uncompact(BitVector bitVector, V compactedVector) {
    int valueCount = bitVector.getValueCount();

    TransferPair transfer = compactedVector.getTransferPair(compactedVector.getAllocator());

    int readIndex = 0;
    for (int i = 0; i < valueCount; i++) {
      if (!bitVector.isNull(i)) {
        transfer.copyValueSafe(readIndex++, i);
      }
    }
    V result = (V) transfer.getTo();
    result.setValueCount(valueCount);
    return result;
  }
}
