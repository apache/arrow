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

import static org.apache.arrow.vector.validate.ValidateUtil.validateOrThrow;

import java.util.function.BiFunction;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.validate.ValidateVectorBufferVisitor;
import org.apache.arrow.vector.validate.ValidateVectorDataVisitor;
import org.apache.arrow.vector.validate.ValidateVectorTypeVisitor;

/**
 * Utility methods for {@link ValueVector}.
 */
public class ValueVectorUtility {

  private ValueVectorUtility() {
  }

  /**
   * Get the toString() representation of vector suitable for debugging.
   * Note since vectors may have millions of values, this method only shows max 20 values.
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
  public static <V extends ValueVector> String getToString(V vector, int start, int end) {
    return getToString(vector, start, end, (v, i) -> v.getObject(i));
  }

  /**
   * Get the toString() representation of vector suitable for debugging.
   * Note since vectors may have millions of values, this method only shows at most 20 values.
   * @param vector the vector for which to get toString representation.
   * @param start the starting index, inclusive.
   * @param end the end index, exclusive.
   * @param valueToString the function to transform individual elements to strings.
   */
  public static <V extends ValueVector> String getToString(
      V vector, int start, int end, BiFunction<V, Integer, Object> valueToString) {
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
        sb.append(valueToString.apply(vector, i));
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
   * Utility to validate vector in O(1) time.
   */
  public static void validate(ValueVector vector) {
    Preconditions.checkNotNull(vector);

    ValidateVectorTypeVisitor typeVisitor = new ValidateVectorTypeVisitor();
    vector.accept(typeVisitor, null);

    ValidateVectorBufferVisitor bufferVisitor = new ValidateVectorBufferVisitor();
    vector.accept(bufferVisitor, null);
  }

  /**
   * Utility to validate vector in O(n) time, where n is the value count.
   */
  public static void validateFull(ValueVector vector) {
    validate(vector);

    ValidateVectorDataVisitor dataVisitor = new ValidateVectorDataVisitor();
    vector.accept(dataVisitor, null);
  }

  /**
   * Utility to validate vector schema root in O(1) time.
   */
  public static void validate(VectorSchemaRoot root) {
    Preconditions.checkNotNull(root);
    int valueCount = root.getRowCount();
    validateOrThrow(valueCount >= 0, "The row count of vector schema root %s is negative.", valueCount);
    for (ValueVector childVec : root.getFieldVectors()) {
      validateOrThrow(valueCount == childVec.getValueCount(),
          "Child vector and vector schema root have different value counts. " +
              "Child vector value count %s, vector schema root value count %s", childVec.getValueCount(), valueCount);
      validate(childVec);
    }
  }

  /**
   * Utility to validate vector in O(n) time, where n is the value count.
   */
  public static void validateFull(VectorSchemaRoot root) {
    Preconditions.checkNotNull(root);
    int valueCount = root.getRowCount();
    validateOrThrow(valueCount >= 0, "The row count of vector schema root %s is negative.", valueCount);
    for (ValueVector childVec : root.getFieldVectors()) {
      validateOrThrow(valueCount == childVec.getValueCount(),
          "Child vector and vector schema root have different value counts. " +
              "Child vector value count %s, vector schema root value count %s", childVec.getValueCount(), valueCount);
      validateFull(childVec);
    }
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
