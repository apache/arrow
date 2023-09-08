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

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.util.List;
import java.util.function.BiFunction;

import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Visitor to compare a range of values for vectors.
 */
public class RangeEqualsVisitor implements VectorVisitor<Boolean, Range> {
  private ValueVector left;
  private ValueVector right;

  private BiFunction<ValueVector, ValueVector, Boolean> typeComparator;
  private boolean typeCompareResult;

  /**
   * Default type comparator.
   */
  public static final BiFunction<ValueVector, ValueVector, Boolean> DEFAULT_TYPE_COMPARATOR =
      (v1, v2) -> new TypeEqualsVisitor(v2).equals(v1);

  /**
   * Constructs a new instance with default type comparator.
   * @param left left vector
   * @param right right vector
   */
  public RangeEqualsVisitor(ValueVector left, ValueVector right) {
    this (left, right, DEFAULT_TYPE_COMPARATOR);
  }

  /**
   * Constructs a new instance.
   *
   * @param left left vector
   * @param right right vector
   * @param typeComparator type comparator to compare vector type.
   */
  public RangeEqualsVisitor(
      ValueVector left,
      ValueVector right,
      BiFunction<ValueVector, ValueVector, Boolean> typeComparator) {
    this.left = left;
    this.right = right;
    this.typeComparator = typeComparator;

    Preconditions.checkArgument(left != null,
        "left vector cannot be null");
    Preconditions.checkArgument(right != null,
        "right vector cannot be null");

    // type usually checks only once unless the left vector is changed.
    checkType();
  }

  private void checkType() {
    if (typeComparator == null || left == right) {
      typeCompareResult = true;
    } else {
      typeCompareResult = typeComparator.apply(left, right);
    }
  }

  /**
   * Validate the passed left vector, if it is changed, reset and check type.
   */
  protected boolean validate(ValueVector left) {
    if (left != this.left) {
      this.left = left;
      checkType();
    }
    return typeCompareResult;
  }

  /**
   * Check range equals.
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

  @Override
  public Boolean visit(BaseFixedWidthVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareBaseFixedWidthVectors(range);
  }

  @Override
  public Boolean visit(BaseVariableWidthVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareBaseVariableWidthVectors(range);
  }

  @Override
  public Boolean visit(BaseLargeVariableWidthVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareBaseLargeVariableWidthVectors(range);
  }

  @Override
  public Boolean visit(ListVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareListVectors(range);
  }

  @Override
  public Boolean visit(FixedSizeListVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareFixedSizeListVectors(range);
  }

  @Override
  public Boolean visit(LargeListVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareLargeListVectors(range);
  }

  @Override
  public Boolean visit(NonNullableStructVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareStructVectors(range);
  }

  @Override
  public Boolean visit(UnionVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareUnionVectors(range);
  }

  @Override
  public Boolean visit(DenseUnionVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return compareDenseUnionVectors(range);
  }

  @Override
  public Boolean visit(NullVector left, Range range) {
    if (!validate(left)) {
      return false;
    }
    return true;
  }

  @Override
  public Boolean visit(ExtensionTypeVector<?> left, Range range) {
    if (!(right instanceof ExtensionTypeVector<?>) || !validate(left)) {
      return false;
    }
    ValueVector rightUnderlying = ((ExtensionTypeVector<?>) right).getUnderlyingVector();
    TypeEqualsVisitor typeVisitor = new TypeEqualsVisitor(rightUnderlying);
    RangeEqualsVisitor underlyingVisitor =
            createInnerVisitor(left.getUnderlyingVector(), rightUnderlying, (l, r) -> typeVisitor.equals(l));
    return underlyingVisitor.rangeEquals(range);
  }

  protected RangeEqualsVisitor createInnerVisitor(
          ValueVector leftInner, ValueVector rightInner,
          BiFunction<ValueVector, ValueVector, Boolean> typeComparator) {
    return new RangeEqualsVisitor(leftInner, rightInner, typeComparator);
  }

  protected boolean compareUnionVectors(Range range) {
    UnionVector leftVector = (UnionVector) left;
    UnionVector rightVector = (UnionVector) right;

    Range subRange = new Range(0, 0, 1);
    for (int i = 0; i < range.getLength(); i++) {
      subRange.setLeftStart(range.getLeftStart() + i).setRightStart(range.getRightStart() + i);
      ValueVector leftSubVector = leftVector.getVector(range.getLeftStart() + i);
      ValueVector rightSubVector = rightVector.getVector(range.getRightStart() + i);

      if (leftSubVector == null || rightSubVector == null) {
        if (leftSubVector == rightSubVector) {
          continue;
        } else {
          return false;
        }
      }
      TypeEqualsVisitor typeVisitor = new TypeEqualsVisitor(rightSubVector);
      RangeEqualsVisitor visitor =
          createInnerVisitor(leftSubVector, rightSubVector, (left, right) -> typeVisitor.equals(left));
      if (!visitor.rangeEquals(subRange)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareDenseUnionVectors(Range range) {
    DenseUnionVector leftVector = (DenseUnionVector) left;
    DenseUnionVector rightVector = (DenseUnionVector) right;

    Range subRange = new Range(0, 0, 1);
    for (int i = 0; i < range.getLength(); i++) {
      boolean isLeftNull = leftVector.isNull(range.getLeftStart() + i);
      boolean isRightNull = rightVector.isNull(range.getRightStart() + i);

      // compare nullabilities
      if (isLeftNull || isRightNull) {
        if (isLeftNull != isRightNull) {
          // exactly one slot is null, unequal
          return false;
        } else {
          // both slots are null, pass this iteration
          continue;
        }
      }

      // compare type ids
      byte leftTypeId = leftVector.getTypeId(range.getLeftStart() + i);
      byte rightTypeId = rightVector.getTypeId(range.getRightStart() + i);

      if (leftTypeId != rightTypeId) {
        return false;
      }

      ValueVector leftSubVector = leftVector.getVectorByType(leftTypeId);
      ValueVector rightSubVector = rightVector.getVectorByType(rightTypeId);

      if (leftSubVector == null || rightSubVector == null) {
        if (leftSubVector != rightSubVector) {
          // exactly one of the sub-vectors is null, unequal
          return false;
        } else {
          // both sub-vectors are null, pass this iteration
          continue;
        }
      }

      // compare values
      int leftOffset = leftVector.getOffset(range.getLeftStart() + i);
      int rightOffset = rightVector.getOffset(range.getRightStart() + i);
      subRange.setLeftStart(leftOffset).setRightStart(rightOffset);
      TypeEqualsVisitor typeVisitor = new TypeEqualsVisitor(rightSubVector);
      RangeEqualsVisitor visitor =
          createInnerVisitor(leftSubVector, rightSubVector, (left, right) -> typeVisitor.equals(left));
      if (!visitor.rangeEquals(subRange)) {
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
      RangeEqualsVisitor visitor =
          createInnerVisitor(leftVector.getChild(name), rightVector.getChild(name), /*type comparator*/ null);
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
        if (!(leftVector instanceof BitVector)) {
          int startIndexLeft = typeWidth * leftIndex;
          int endIndexLeft = typeWidth * (leftIndex + 1);

          int startIndexRight = typeWidth * rightIndex;
          int endIndexRight = typeWidth * (rightIndex + 1);

          int ret = ByteFunctionHelpers.equal(leftVector.getDataBuffer(), startIndexLeft, endIndexLeft,
              rightVector.getDataBuffer(), startIndexRight, endIndexRight);

          if (ret == 0) {
            return false;
          }
        } else {
          boolean ret = ((BitVector) leftVector).get(leftIndex) == ((BitVector) rightVector).get(rightIndex);
          if (!ret) {
            return false;
          }
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

  protected boolean compareBaseLargeVariableWidthVectors(Range range) {
    BaseLargeVariableWidthVector leftVector = (BaseLargeVariableWidthVector) left;
    BaseLargeVariableWidthVector rightVector = (BaseLargeVariableWidthVector) right;

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      int offsetWidth = BaseLargeVariableWidthVector.OFFSET_WIDTH;

      if (!isNull) {
        final long startIndexLeft = leftVector.getOffsetBuffer().getLong((long) leftIndex * offsetWidth);
        final long endIndexLeft = leftVector.getOffsetBuffer().getLong((long) (leftIndex + 1) * offsetWidth);

        final long startIndexRight = rightVector.getOffsetBuffer().getLong((long) rightIndex * offsetWidth);
        final long endIndexRight = rightVector.getOffsetBuffer().getLong((long) (rightIndex + 1) * offsetWidth);

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

    RangeEqualsVisitor innerVisitor =
        createInnerVisitor(leftVector.getDataVector(), rightVector.getDataVector(), /*type comparator*/ null);
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
    RangeEqualsVisitor innerVisitor =
        createInnerVisitor(leftVector.getDataVector(), rightVector.getDataVector(), /*type comparator*/ null);
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

  protected boolean compareLargeListVectors(Range range) {
    LargeListVector leftVector = (LargeListVector) left;
    LargeListVector rightVector = (LargeListVector) right;

    RangeEqualsVisitor innerVisitor =
        createInnerVisitor(leftVector.getDataVector(), rightVector.getDataVector(), /*type comparator*/ null);
    Range innerRange = new Range();

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      long offsetWidth = LargeListVector.OFFSET_WIDTH;

      if (!isNull) {
        final long startIndexLeft = leftVector.getOffsetBuffer().getLong((long) leftIndex * offsetWidth);
        final long endIndexLeft = leftVector.getOffsetBuffer().getLong((long) (leftIndex + 1) * offsetWidth);

        final long startIndexRight = rightVector.getOffsetBuffer().getLong((long) rightIndex * offsetWidth);
        final long endIndexRight = rightVector.getOffsetBuffer().getLong((long) (rightIndex + 1) * offsetWidth);

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        innerRange = innerRange // TODO revisit these casts when long indexing is finished
            .setRightStart(checkedCastToInt(startIndexRight))
            .setLeftStart(checkedCastToInt(startIndexLeft))
            .setLength(checkedCastToInt(endIndexLeft - startIndexLeft));
        if (!innerVisitor.rangeEquals(innerRange)) {
          return false;
        }
      }
    }
    return true;
  }
}
