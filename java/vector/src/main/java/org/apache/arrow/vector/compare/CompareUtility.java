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

package org.apache.arrow.vector.compare;

import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;

/**
 * Utility to compare two single values in two vectors with same type.
 */
public class CompareUtility {

  /**
   * Compare {@link BaseFixedWidthVector} value equals with given indices and vectors.
   * @param left left vector to compare
   * @param leftIndex left index
   * @param right right vector to compare
   * @param rightIndex right index
   * @return true if equals, otherwise false
   */
  public static boolean compare(BaseFixedWidthVector left, int leftIndex, BaseFixedWidthVector right, int rightIndex) {

    boolean isNull = left.isNull(leftIndex);

    if (isNull != right.isNull(rightIndex)) {
      return false;
    }

    int typeWidth = left.getTypeWidth();
    if (!isNull) {
      int startByteLeft = typeWidth * leftIndex;
      int endByteLeft = typeWidth * (leftIndex + 1);

      int startByteRight = typeWidth * rightIndex;
      int endByteRight = typeWidth * (rightIndex + 1);

      int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), startByteLeft, endByteLeft,
          right.getDataBuffer(), startByteRight, endByteRight);

      if (ret == 0) {
        return false;
      }

    }
    return true;
  }

  /**
   * Compare {@link BaseVariableWidthVector} value equals with given indices and vectors.
   * @param left left vector to compare
   * @param leftIndex left index
   * @param right right vector to compare
   * @param rightIndex right index
   * @return true if equals, otherwise false
   */
  public static boolean compare(BaseVariableWidthVector left, int leftIndex,
      BaseVariableWidthVector right, int rightIndex) {

    boolean isNull = left.isNull(leftIndex);
    if (isNull != right.isNull(rightIndex)) {
      return false;
    }

    int offsetWidth = BaseVariableWidthVector.OFFSET_WIDTH;

    if (!isNull) {
      final int start1 = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
      final int end1 = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

      final int start2 = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
      final int end2 = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

      int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), start1, end1,
          right.getDataBuffer(), start2, end2);

      if (ret == 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare {@link ListVector} value equals with given indices and vectors.
   * @param left left vector to compare
   * @param leftIndex left index
   * @param right right vector to compare
   * @param rightIndex right index
   * @return true if equals, otherwise false
   */
  public static boolean compare(ListVector left, int leftIndex, ListVector right, int rightIndex) {

    boolean isNull = left.isNull(leftIndex);
    if (isNull != right.isNull(rightIndex)) {
      return false;
    }

    int offsetWidth = BaseRepeatedValueVector.OFFSET_WIDTH;

    if (!isNull) {
      final int startByteLeft = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
      final int endByteLeft = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

      final int startByteRight = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
      final int endByteRight = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

      if ((endByteLeft - startByteLeft) != (endByteRight - startByteRight)) {
        return false;
      }

      ValueVector dataVector1 = left.getDataVector();
      ValueVector dataVector2 = right.getDataVector();

      if (!dataVector1.accept(new RangeEqualsVisitor(dataVector2, startByteLeft,
          startByteRight, (endByteLeft - startByteLeft)))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare {@link FixedSizeListVector} value equals with given indices and vectors.
   * @param left left vector to compare
   * @param leftIndex left index
   * @param right right vector to compare
   * @param rightIndex right index
   * @return true if equals, otherwise false
   */
  public static boolean compare(FixedSizeListVector left, int leftIndex, FixedSizeListVector right, int rightIndex) {
    boolean isNull = left.isNull(leftIndex);
    if (isNull != right.isNull(rightIndex)) {
      return false;
    }

    int listSize = left.getListSize();

    if (!isNull) {
      final int start1 = leftIndex * listSize;
      final int end1 = (leftIndex + 1) * listSize;

      final int start2 = rightIndex * listSize;
      final int end2 = (rightIndex + 1) * listSize;

      if ((end1 - start1) != (end2 - start2)) {
        return false;
      }

      ValueVector dataVector1 = left.getDataVector();
      ValueVector dataVector2 = right.getDataVector();

      if (!dataVector1.accept(new RangeEqualsVisitor(dataVector2, start1, start2, (end1 - start1)))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compare {@link NonNullableStructVector} value equals with given indices and vectors.
   * @param left left vector to compare
   * @param leftIndex left index
   * @param right right vector to compare
   * @param rightIndex right index
   * @return true if equals, otherwise false
   */
  public static boolean compare(NonNullableStructVector left, int leftIndex, NonNullableStructVector right,
      int rightIndex) {
    boolean isNull = left.isNull(leftIndex);
    if (isNull != right.isNull(rightIndex)) {
      return false;
    }

    if (!isNull) {
      if (!left.getChildFieldNames().equals(right.getChildFieldNames())) {
        return false;
      }

      for (String name : left.getChildFieldNames()) {
        if (!left.getChild(name).equals(leftIndex, right.getChild(name), rightIndex)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check whether indices are valid.
   */
  public static boolean checkIndices(ValueVector left, int leftIndex, ValueVector right, int rightIndex) {
    if (leftIndex < 0 || rightIndex < 0) {
      throw new IllegalStateException(String.format("indices must be non negative, left index: %s, right index: %s",
          leftIndex, rightIndex));
    }

    if (leftIndex >= left.getValueCount() || rightIndex >= right.getValueCount()) {
      throw new IllegalStateException(String.format("indices mush be less than valueCount, left index: %s, " +
              "right index: %s, left valueCount: %s, right valueCount: %s", leftIndex, left.getValueCount(),
          rightIndex, right.getValueCount()));
    }

    return true;
  }
}
