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
public class RangeEqualsVisitor implements VectorVisitor<Boolean, Range> {
  private ValueVector left;
  private ValueVector right;
  private boolean isTypeCheckNeeded;
  private boolean typeCompareResult;

  /**
   * Constructs a new instance.
   *
   * @param left left vector
   * @param right right vector
   * @param isTypeCheckNeeded type check needed
   */
  public RangeEqualsVisitor(ValueVector left, ValueVector right, boolean isTypeCheckNeeded) {
    this.left = left;
    this.right = right;
    this.isTypeCheckNeeded = isTypeCheckNeeded;

    Preconditions.checkArgument(left != null,
        "left vector cannot be null");
    Preconditions.checkArgument(right != null,
        "right vector cannot be null");

    // types cannot change for a visitor instance. so, the check is done only once.
    if (!isTypeCheckNeeded) {
      typeCompareResult = true;
    } else if (left == right) {
      typeCompareResult = true;
    } else {
      typeCompareResult = left.getField().getType().equals(right.getField().getType());
    }
  }

  /**
   * Constructs a new instance.
   *
   * @param left left vector
   * @param right right vector
   */
  public RangeEqualsVisitor(ValueVector left, ValueVector right) {
    this(left, right, true);
  }

  /**
   * Check range equals without passing IN param in VectorVisitor.
   */
  public boolean rangeEquals(Range range) {
    if (!typeCompareResult) {
      return false;
    }

    Preconditions.checkArgument(range.getLeftStart() >= 0,
        "leftStart %s must be non negative.", range.getLeftStart());
    Preconditions.checkArgument(range.getRightStart() >= 0,
        "rightStart %s must be non negative.", range.getRightStart());

    Preconditions.checkArgument(range.getRightStart() + range.getLength() <= right.getValueCount(),
        "(rightStart + length) %s out of range[0, %s].", 0, right.getValueCount());
    Preconditions.checkArgument(range.getLeftStart() + range.getLength() <= left.getValueCount(),
        "(leftStart + length) %s out of range[0, %s].", 0, left.getValueCount());

    return left.accept(this, range);
  }

  public ValueVector getLeft() {
    return left;
  }

  public ValueVector getRight() {
    return right;
  }

  public boolean isTypeCheckNeeded() {
    return isTypeCheckNeeded;
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, Range range) {
    return compareBaseFixedWidthVectors(range);
  }

  @Override
  public Boolean visit(BaseVariableWidthVector left, Range range) {
    return compareBaseVariableWidthVectors(range);
  }

  @Override
  public Boolean visit(ListVector left, Range range) {
    return compareListVectors(range);
  }

  @Override
  public Boolean visit(FixedSizeListVector left, Range range) {
    return compareFixedSizeListVectors(range);
  }

  @Override
  public Boolean visit(NonNullableStructVector left, Range range) {
    return compareStructVectors(range);
  }

  @Override
  public Boolean visit(UnionVector left, Range range) {
    return compareUnionVectors(range);
  }

  @Override
  public Boolean visit(ZeroVector left, Range range) {
    return true;
  }

  /**
   * Creates a visitor to visit child vectors.
   * It is used for complex vector types.
   * @return the visitor for child vecors.
   */
  protected RangeEqualsVisitor createInnerVisitor(ValueVector leftInner, ValueVector rightInner) {
    return new RangeEqualsVisitor(leftInner, rightInner, isTypeCheckNeeded);
  }

  protected boolean compareUnionVectors(Range range) {
    UnionVector leftVector = (UnionVector) left;
    UnionVector rightVector = (UnionVector) right;

    List<FieldVector> leftChildren = leftVector.getChildrenFromFields();
    List<FieldVector> rightChildren = rightVector.getChildrenFromFields();

    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    for (int k = 0; k < leftChildren.size(); k++) {
      RangeEqualsVisitor visitor = createInnerVisitor(leftChildren.get(k), rightChildren.get(k));
      if (!visitor.rangeEquals(range)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareStructVectors(Range range) {
    NonNullableStructVector leftVector = (NonNullableStructVector) left;
    NonNullableStructVector rightVector = (NonNullableStructVector) right;

    List<String> leftChildNames = leftVector.getChildFieldNames();
    if (!leftChildNames.equals(rightVector.getChildFieldNames())) {
      return false;
    }

    for (String name : leftChildNames) {
      RangeEqualsVisitor visitor = createInnerVisitor(leftVector.getChild(name), rightVector.getChild(name));
      if (!visitor.rangeEquals(range)) {
        return false;
      }
    }

    return true;
  }

  protected boolean compareBaseFixedWidthVectors(Range range) {
    BaseFixedWidthVector leftVector = (BaseFixedWidthVector) left;
    BaseFixedWidthVector rightVector = (BaseFixedWidthVector) right;

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);

      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      int typeWidth = leftVector.getTypeWidth();
      if (!isNull) {
        int startIndexLeft = typeWidth * leftIndex;
        int endIndexLeft = typeWidth * (leftIndex + 1);

        int startIndexRight = typeWidth * rightIndex;
        int endIndexRight = typeWidth * (rightIndex + 1);

        int ret = ByteFunctionHelpers.equal(leftVector.getDataBuffer(), startIndexLeft, endIndexLeft,
            rightVector.getDataBuffer(), startIndexRight, endIndexRight);

        if (ret == 0) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareBaseVariableWidthVectors(Range range) {
    BaseVariableWidthVector leftVector = (BaseVariableWidthVector) left;
    BaseVariableWidthVector rightVector = (BaseVariableWidthVector) right;

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseVariableWidthVector.OFFSET_WIDTH;

      if (!isNull) {
        final int startIndexLeft = leftVector.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int endIndexLeft = leftVector.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int startIndexRight = rightVector.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int endIndexRight = rightVector.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        int ret = ByteFunctionHelpers.equal(leftVector.getDataBuffer(), startIndexLeft, endIndexLeft,
            rightVector.getDataBuffer(), startIndexRight, endIndexRight);

        if (ret == 0) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareListVectors(Range range) {
    ListVector leftVector = (ListVector) left;
    ListVector rightVector = (ListVector) right;

    RangeEqualsVisitor innerVisitor = createInnerVisitor(leftVector.getDataVector(), rightVector.getDataVector());
    Range innerRange = new Range();

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseRepeatedValueVector.OFFSET_WIDTH;

      if (!isNull) {
        final int startIndexLeft = leftVector.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int endIndexLeft = leftVector.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int startIndexRight = rightVector.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int endIndexRight = rightVector.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        innerRange = innerRange
            .setRightStart(startIndexRight)
            .setLeftStart(startIndexLeft)
            .setLength(endIndexLeft - startIndexLeft);
        if (!innerVisitor.rangeEquals(innerRange)) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareFixedSizeListVectors(Range range) {
    FixedSizeListVector leftVector = (FixedSizeListVector) left;
    FixedSizeListVector rightVector = (FixedSizeListVector) right;

    if (leftVector.getListSize() != rightVector.getListSize()) {
      return false;
    }

    int listSize = leftVector.getListSize();
    RangeEqualsVisitor innerVisitor = createInnerVisitor(leftVector, rightVector);
    Range innerRange = new Range(0, 0, listSize);

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {
        final int startIndexLeft = leftIndex * listSize;
        final int endIndexLeft = (leftIndex + 1) * listSize;

        final int startIndexRight = rightIndex * listSize;
        final int endIndexRight = (rightIndex + 1) * listSize;

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        innerRange = innerRange.setLeftStart(startIndexLeft)
            .setRightStart(startIndexRight);
        if (!innerVisitor.rangeEquals(innerRange)) {
          return false;
        }
      }
    }
    return true;
  }
}
