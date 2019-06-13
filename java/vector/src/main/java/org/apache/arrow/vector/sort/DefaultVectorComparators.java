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

package org.apache.arrow.vector.sort;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

/**
 * Default comparator implementations for different types of vectors.
 */
public class DefaultVectorComparators {

  /**
   * Default comparator for 32-bit integers.
   * The comparison is based on int values, with null comes first.
   */
  public static class IntComparator extends VectorValueComparator<IntVector> {

    public IntComparator() {
      valueWidth = 4;
    }

    @Override
    public IntVector newVector(BufferAllocator allocator) {
      return  new IntVector("", allocator);
    }

    @Override
    public int compare(Integer index1, Integer index2) {
      boolean isNull1 = vector1.isNull(index1);
      boolean isNull2 = vector2.isNull(index2);

      if (isNull1 || isNull2) {
        if (isNull1) {
          // null is smaller
          return -1;
        } else {
          return 1;
        }
      }

      int value1 = vector1.get(index1);
      int value2 = vector2.get(index2);
      if (value1 != value2) {
        return value1 - value2;
      } else {
        // never return 0, so no value will be considered duplicated and removed.
        return 1;
      }
    }
  }

  /**
   * Default comparator for varchars.
   * The comparison is in lexicographic order, with null comes first.
   */
  public static class VarCharComparator extends VectorValueComparator<VarCharVector> {

    @Override
    public VarCharVector newVector(BufferAllocator allocator) {
      return new VarCharVector("", allocator);
    }

    @Override
    public int compare(Integer index1, Integer index2) {
      NullableVarCharHolder holder1 = new NullableVarCharHolder();
      NullableVarCharHolder holder2 = new NullableVarCharHolder();

      vector1.get(index1, holder1);
      vector2.get(index2, holder2);

      if (holder1.isSet == 0 || holder2.isSet == 0) {
        if (holder1.isSet == 0) {
          // null is smaller
          return -1;
        } else {
          return 1;
        }
      }

      int length1 = holder1.end - holder1.start;
      int length2 = holder2.end - holder2.start;

      for (int i = 0; i < length1 && i < length2; i++) {
        byte b1 = holder1.buffer.getByte(holder1.start + i);
        byte b2 = holder2.buffer.getByte(holder2.start + i);

        if (b1 != b2) {
          return b1 - b2;
        }
      }

      if (length1 != length2) {
        return length1 - length2;
      }
      return -1;
    }
  }

  private DefaultVectorComparators() {
  }
}
