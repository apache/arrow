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
import org.apache.arrow.util.Preconditions;
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
  protected int leftStart;
  protected int rightStart;
  protected int length;

  protected boolean typeCheckNeeded = true;

  /**
   * Constructs a new instance.
   */
  public RangeEqualsVisitor(ValueVector right, int rightStart,  int leftStart, int length, boolean typeCheckNeeded) {
    this.leftStart = leftStart;
    this.rightStart = rightStart;
    this.right = right;
    this.length = length;
    this.typeCheckNeeded = typeCheckNeeded;
    Preconditions.checkArgument(length >= 0, "length must be non negative");
  }

  /**
   * Constructs a new instance.
   */
  public RangeEqualsVisitor(ValueVector right, int leftStart, int rightStart, int length) {
    this(right, rightStart, leftStart, length, true);
  }

  /**
   * Do some validation work, like type check and indices check.
   */
  private boolean validate(ValueVector left) {

    if (!compareValueVector(left, right)) {
      return false;
    }

    Preconditions.checkArgument(leftStart >= 0,
        "leftStart %s must be non negative.", leftStart);
    Preconditions.checkArgument((leftStart + length) <= left.getValueCount(),
        "(leftStart + length) %s out of range[0, %s].", 0, left.getValueCount());
    Preconditions.checkArgument(rightStart >= 0,
        "rightStart %s must be non negative.", rightStart);
    Preconditions.checkArgument((rightStart + length) <= right.getValueCount(),
        "(rightStart + length) %s out of range[0, %s].", 0, right.getValueCount());

    return true;
  }

  public boolean visit(BaseFixedWidthVector left) {
    return validate(left) && compareBaseFixedWidthVectors(left);
  }

  public boolean visit(BaseVariableWidthVector left) {
    return validate(left) && compareBaseVariableWidthVectors(left);
  }

  public boolean visit(ListVector left) {
    return validate(left) && compareListVectors(left);
  }

  public boolean visit(FixedSizeListVector left) {
    return validate(left) && compareFixedSizeListVectors(left);
  }

  public boolean visit(NonNullableStructVector left) {
    return validate(left) && compareStructVectors(left);
  }

  public boolean visit(UnionVector left) {
    return validate(left) && compareUnionVectors(left);
  }

  public boolean visit(ZeroVector left) {
    return validate(left);
  }

  public boolean visit(ValueVector left) {
    throw new UnsupportedOperationException();
  }

  protected boolean compareValueVector(ValueVector left, ValueVector right) {
    if (!typeCheckNeeded) {
      return true;
    }
    return left.getField().getType().equals(right.getField().getType());
  }

  protected boolean compareUnionVectors(UnionVector left) {

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

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);

      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      int typeWidth = left.getTypeWidth();
      if (!isNull) {
        int startIndexLeft = typeWidth * leftIndex;
        int endIndexLeft = typeWidth * (leftIndex + 1);

        int startIndexRight = typeWidth * rightIndex;
        int endIndexRight = typeWidth * (rightIndex + 1);

        int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), startIndexLeft, endIndexLeft,
            right.getDataBuffer(), startIndexRight, endIndexRight);

        if (ret == 0) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareBaseVariableWidthVectors(BaseVariableWidthVector left) {

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);
      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseVariableWidthVector.OFFSET_WIDTH;

      if (!isNull) {
        final int startIndexLeft = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int endIndexLeft = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int startIndexRight = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int endIndexRight = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), startIndexLeft, endIndexLeft,
            right.getDataBuffer(), startIndexRight, endIndexRight);

        if (ret == 0) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareListVectors(ListVector left) {

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);
      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseRepeatedValueVector.OFFSET_WIDTH;

      if (!isNull) {
        final int startIndexLeft = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int endIndexLeft = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int startIndexRight = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int endIndexRight = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        ValueVector leftDataVector = left.getDataVector();
        ValueVector rightDataVector = ((ListVector)right).getDataVector();

        if (!leftDataVector.accept(new RangeEqualsVisitor(rightDataVector, startIndexLeft,
            startIndexRight, (endIndexLeft - startIndexLeft)))) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareFixedSizeListVectors(FixedSizeListVector left) {

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
        final int startIndexLeft = leftIndex * listSize;
        final int endIndexLeft = (leftIndex + 1) * listSize;

        final int startIndexRight = rightIndex * listSize;
        final int endIndexRight = (rightIndex + 1) * listSize;

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        ValueVector leftDataVector = left.getDataVector();
        ValueVector rightDataVector = ((FixedSizeListVector)right).getDataVector();

        if (!leftDataVector.accept(new RangeEqualsVisitor(rightDataVector, startIndexLeft, startIndexRight,
            (endIndexLeft - startIndexLeft)))) {
          return false;
        }
      }
    }
    return true;
  }
}
