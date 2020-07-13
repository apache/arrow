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

package org.apache.arrow.vector.util;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BufferLayout;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.validate.ValidateVectorVisitor;

/**
 * Utility methods for {@link ValueVector}.
 */
public class ValueVectorUtility {

  /**
   * Get the toString() representation of vector suitable for debugging.
   * Note since vectors may have millions of values, this method only show max 20 values.
   * Examples as below (v represents value):
   * <li>
   *   vector with 0 value:
   *   []
   * </li>
   * <li>
   *   vector with 5 values (no more than 20 values):
   *   [v0, v1, v2, v3, v4]
   * </li>
   * <li>
   *  vector with 100 values (more than 20 values):
   *  [v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, ..., v90, v91, v92, v93, v94, v95, v96, v97, v98, v99]
   * </li>
   */
  public static String getToString(ValueVector vector, int start, int end) {
    Preconditions.checkNotNull(vector);
    final int length = end - start;
    Preconditions.checkArgument(length >= 0);
    Preconditions.checkArgument(start >= 0);
    Preconditions.checkArgument(end <= vector.getValueCount());

    if (length == 0) {
      return "[]";
    }

    final int window = 10;
    boolean skipComma = false;

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = start; i < end; i++) {
      if (skipComma) {
        skipComma = false;
      }
      if (i - start >= window && i < end - window) {
        sb.append("...");
        i = end - window - 1;
        skipComma = true;
      } else {
        sb.append(vector.getObject(i));
      }

      if (i == end - 1) {
        sb.append(']');
      } else {
        if (!skipComma) {
          sb.append(',');
        }
        sb.append(' ');
      }
    }

    return sb.toString();
  }

  /**
   * Validate field vector.
   */
  public static void validate(FieldVector vector) {
    Preconditions.checkNotNull(vector);

    ArrowType arrowType = vector.getField().getType();
    int typeBufferCount = TypeLayout.getTypeBufferCount(arrowType);
    TypeLayout typeLayout = TypeLayout.getTypeLayout(arrowType);

    if (vector.getValueCount() < 0) {
      throw new IllegalArgumentException("vector valueCount is negative");
    }

    if (vector.getFieldBuffers().size() != typeBufferCount) {
      throw new IllegalArgumentException(String.format("Expected %s buffers in vector of type %s, got %s",
          typeBufferCount, vector.getField().getType().toString(), vector.getBufferSize()));
    }

    for (int i = 0; i < typeBufferCount; i++) {
      ArrowBuf buffer = vector.getFieldBuffers().get(i);
      BufferLayout bufferLayout = typeLayout.getBufferLayouts().get(i);
      if (buffer == null) {
        continue;
      }
      int minBufferSize = vector.getValueCount() * bufferLayout.getTypeBitWidth();

      if (buffer.capacity() < minBufferSize / 8) {
        throw new IllegalArgumentException(String.format("Buffer #%s too small in vector of type %s" +
                "and valueCount %s : expected at least %s byte(s), got %s",
            i, vector.getField().getType().toString(),
            vector.getValueCount(), minBufferSize, buffer.capacity()));
      }
    }

    ValidateVectorVisitor visitor = new ValidateVectorVisitor();
    vector.accept(visitor, null);
  }


  /**
   * Pre allocate memory for BaseFixedWidthVector.
   */
  public static void preAllocate(VectorSchemaRoot root, int targetSize) {
    for (ValueVector vector : root.getFieldVectors()) {
      if (vector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) vector).allocateNew(targetSize);
      }
    }
  }

  /**
   * Ensure capacity for BaseFixedWidthVector.
   */
  public static void ensureCapacity(VectorSchemaRoot root, int targetCapacity) {
    for (ValueVector vector : root.getFieldVectors()) {
      if (vector instanceof BaseFixedWidthVector) {
        while (vector.getValueCapacity() < targetCapacity) {
          vector.reAlloc();
        }
      }
    }
  }

}
