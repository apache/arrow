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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Visitor to compare floating point.
 */
public class ApproxEqualsVisitor extends RangeEqualsVisitor {

  /**
   * The float/double values are treated as equal as long as the delta is <= epsilon.
   */
  private final float epsilon;

  public ApproxEqualsVisitor(ValueVector right, float epsilon) {
    this (right, epsilon, true);
  }

  public ApproxEqualsVisitor(ValueVector right, float epsilon, boolean typeCheckNeeded) {
    this (right, epsilon, typeCheckNeeded, 0, 0, right.getValueCount());
  }

  public ApproxEqualsVisitor(ValueVector right, float epsilon, boolean typeCheckNeeded,
      int leftStart, int rightStart, int length) {
    super(right, rightStart, leftStart, length, typeCheckNeeded);
    this.epsilon = epsilon;
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, Void value) {
    if (left instanceof Float4Vector) {
      return validate(left) && float4ApproxEquals((Float4Vector) left);
    } else if (left instanceof Float8Vector) {
      return validate(left) && float8ApproxEquals((Float8Vector) left);
    } else {
      return super.visit(left, value);
    }
  }

  @Override
  protected boolean compareUnionVectors(UnionVector left) {

    UnionVector rightVector = (UnionVector) right;

    List<FieldVector> leftChildren = left.getChildrenFromFields();
    List<FieldVector> rightChildren = rightVector.getChildrenFromFields();

    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    for (int k = 0; k < leftChildren.size(); k++) {
      ApproxEqualsVisitor visitor = new ApproxEqualsVisitor(rightChildren.get(k),
          epsilon);
      if (!leftChildren.get(k).accept(visitor, null)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean compareStructVectors(NonNullableStructVector left) {

    NonNullableStructVector rightVector = (NonNullableStructVector) right;

    if (!left.getChildFieldNames().equals(rightVector.getChildFieldNames())) {
      return false;
    }

    for (String name : left.getChildFieldNames()) {
      ApproxEqualsVisitor visitor = new ApproxEqualsVisitor(rightVector.getChild(name),
          epsilon);
      if (!left.getChild(name).accept(visitor, null)) {
        return false;
      }
    }

    return true;
  }

  @Override
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

        if (!leftDataVector.accept(new ApproxEqualsVisitor(rightDataVector, epsilon, typeCheckNeeded,
            startIndexLeft, startIndexRight, (endIndexLeft - startIndexLeft)), null)) {
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

        if (!leftDataVector.accept(new ApproxEqualsVisitor(rightDataVector, epsilon, typeCheckNeeded,
            startIndexLeft, startIndexRight, (endIndexLeft - startIndexLeft)), null)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean float4ApproxEquals(Float4Vector left) {

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);

      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {

        float leftValue = left.get(leftIndex);
        float rightValue = ((Float4Vector)right).get(rightIndex);
        if (Math.abs(leftValue - rightValue) > epsilon) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean float8ApproxEquals(Float8Vector left) {
    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);

      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {

        double leftValue = left.get(leftIndex);
        double rightValue = ((Float8Vector)right).get(rightIndex);
        if (Math.abs(leftValue - rightValue) > epsilon) {
          return false;
        }
      }
    }
    return true;
  }
}
