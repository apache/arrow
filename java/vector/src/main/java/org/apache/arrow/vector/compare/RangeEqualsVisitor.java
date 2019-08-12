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

import java.util.List;

import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Visitor to compare a range of values for vectors.
 */
public class RangeEqualsVisitor {

  protected final ValueVector right;
  protected final int leftStart;
  protected final int rightStart;
  protected final int length;

  /**
   * Constructs a new instance.
   */
  public RangeEqualsVisitor(ValueVector right, int leftStart, int rightStart, int length) {
    this.leftStart = leftStart;
    this.rightStart = rightStart;
    this.right = right;
    this.length = length;
  }

  public boolean visit(BaseFixedWidthVector left) {
    return compareBaseFixedWidthVectors(left);
  }

  public boolean visit(BaseVariableWidthVector left) {
    return compareBaseVariableWidthVectors(left);
  }

  public boolean visit(ListVector left) {
    return compareListVectors(left);
  }

  public boolean visit(FixedSizeListVector left) {
    return compareFixedSizeListVectors(left);
  }

  public boolean visit(NonNullableStructVector left) {
    return compareStructVectors(left);
  }

  public boolean visit(UnionVector left) {
    return compareUnionVectors(left);
  }

  public boolean visit(ZeroVector left) {
    return compareValueVector(left, right);
  }

  public boolean visit(ValueVector left) {
    throw new UnsupportedOperationException();
  }

  protected boolean compareValueVector(ValueVector left, ValueVector right) {
    return left.getField().getType().equals(right.getField().getType());
  }

  protected boolean compareUnionVectors(UnionVector left) {

    if (!compareValueVector(left, right)) {
      return false;
    }

    UnionVector rightVector = (UnionVector) right;

    List<FieldVector> leftChildren = left.getChildrenFromFields();
    List<FieldVector> rightChildren = rightVector.getChildrenFromFields();

    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    for (int k = 0; k < leftChildren.size(); k++) {
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(rightChildren.get(k),
          leftStart, rightStart, length);
      if (!leftChildren.get(k).accept(visitor)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareStructVectors(NonNullableStructVector left) {
    if (!compareValueVector(left, right)) {
      return false;
    }

    NonNullableStructVector rightVector = (NonNullableStructVector) right;

    if (!left.getChildFieldNames().equals(rightVector.getChildFieldNames())) {
      return false;
    }

    for (String name : left.getChildFieldNames()) {
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(rightVector.getChild(name),
          leftStart, rightStart, length);
      if (!left.getChild(name).accept(visitor)) {
        return false;
      }
    }

    return true;
  }

  protected boolean compareBaseFixedWidthVectors(BaseFixedWidthVector left) {

    if (!compareValueVector(left, right)) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

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
    }
    return true;
  }

  protected boolean compareBaseVariableWidthVectors(BaseVariableWidthVector left) {
    if (!compareValueVector(left, right)) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);
      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseVariableWidthVector.OFFSET_WIDTH;

      if (!isNull) {
        final int startByteLeft = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int endByteLeft = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int startByteRight = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int endByteRight = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), startByteLeft, endByteLeft,
            right.getDataBuffer(), startByteRight, endByteRight);

        if (ret == 0) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareListVectors(ListVector left) {
    if (!compareValueVector(left, right)) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

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

        ValueVector leftDataVector = left.getDataVector();
        ValueVector rightDataVector = ((ListVector)right).getDataVector();

        if (!leftDataVector.accept(new RangeEqualsVisitor(rightDataVector, startByteLeft,
            startByteRight, (endByteLeft - startByteLeft)))) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareFixedSizeListVectors(FixedSizeListVector left) {
    if (!compareValueVector(left, right)) {
      return false;
    }

    if (left.getListSize() != ((FixedSizeListVector)right).getListSize()) {
      return false;
    }

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);
      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      int listSize = left.getListSize();

      if (!isNull) {
        final int startByteLeft = leftIndex * listSize;
        final int endByteLeft = (leftIndex + 1) * listSize;

        final int startByteRight = rightIndex * listSize;
        final int endByteRight = (rightIndex + 1) * listSize;

        if ((endByteLeft - startByteLeft) != (endByteRight - startByteRight)) {
          return false;
        }

        ValueVector leftDataVector = left.getDataVector();
        ValueVector rightDataVector = ((FixedSizeListVector)right).getDataVector();

        if (!leftDataVector.accept(new RangeEqualsVisitor(rightDataVector, startByteLeft, startByteRight,
            (endByteLeft - startByteLeft)))) {
          return false;
        }
      }
    }
    return true;
  }

}
