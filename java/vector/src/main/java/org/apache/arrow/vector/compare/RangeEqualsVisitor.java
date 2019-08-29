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
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Visitor to compare a range of values for vectors.
 */
public class RangeEqualsVisitor implements VectorVisitor<Boolean, RangeEqualsParameter> {

  /**
   * Check range equals without passing IN param in VectorVisitor.
   */
  public boolean rangeEquals(RangeEqualsParameter parameter) {
    return parameter.getLeft().accept(this, parameter);
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareBaseFixedWidthVectors(parameter);
  }

  @Override
  public Boolean visit(BaseVariableWidthVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareBaseVariableWidthVectors(parameter);
  }

  @Override
  public Boolean visit(ListVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareListVectors(parameter);
  }

  @Override
  public Boolean visit(FixedSizeListVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareFixedSizeListVectors(parameter);
  }

  @Override
  public Boolean visit(NonNullableStructVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareStructVectors(parameter);
  }

  @Override
  public Boolean visit(UnionVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate() && compareUnionVectors(parameter);
  }

  @Override
  public Boolean visit(ZeroVector left, RangeEqualsParameter parameter) {
    parameter.setLeft(left);
    return parameter.validate();
  }

  /**
   * Creates a visitor to visit child vectors.
   * It is used for complex vector types.
   * @return the visitor for child vecors.
   */
  protected RangeEqualsVisitor createInnerVisitor() {
    return new RangeEqualsVisitor();
  }

  protected boolean compareUnionVectors(RangeEqualsParameter parameter) {
    UnionVector leftVector = (UnionVector) parameter.getLeft();
    UnionVector rightVector = (UnionVector) parameter.getRight();

    List<FieldVector> leftChildren = leftVector.getChildrenFromFields();
    List<FieldVector> rightChildren = rightVector.getChildrenFromFields();

    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    RangeEqualsVisitor visitor = createInnerVisitor();
    RangeEqualsParameter innerParam = new RangeEqualsParameter()
            .setLeftStart(parameter.getLeftStart())
            .setRightStart(parameter.getRightStart())
            .setLength(parameter.getLength())
            .setTypeCheckNeeded(parameter.isTypeCheckNeeded());

    for (int k = 0; k < leftChildren.size(); k++) {
      innerParam.setRight(rightChildren.get(k));
      if (!leftChildren.get(k).accept(visitor, innerParam)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareStructVectors(RangeEqualsParameter parameter) {
    NonNullableStructVector leftVector = (NonNullableStructVector) parameter.getLeft();
    NonNullableStructVector rightVector = (NonNullableStructVector) parameter.getRight();

    List<String> leftChildNames = leftVector.getChildFieldNames();
    if (!leftChildNames.equals(rightVector.getChildFieldNames())) {
      return false;
    }

    RangeEqualsVisitor visitor = createInnerVisitor();
    RangeEqualsParameter innerParam = new RangeEqualsParameter()
            .setLeftStart(parameter.getLeftStart())
            .setRightStart(parameter.getRightStart())
            .setLength(parameter.getLength())
            .setTypeCheckNeeded(parameter.isTypeCheckNeeded());

    for (String name : leftChildNames) {
      innerParam.setRight(rightVector.getChild(name));
      if (!leftVector.getChild(name).accept(visitor, innerParam)) {
        return false;
      }
    }

    return true;
  }

  protected boolean compareBaseFixedWidthVectors(RangeEqualsParameter parameter) {
    BaseFixedWidthVector leftVector = (BaseFixedWidthVector) parameter.getLeft();
    BaseFixedWidthVector rightVector = (BaseFixedWidthVector) parameter.getRight();

    for (int i = 0; i < parameter.getLength(); i++) {
      int leftIndex = parameter.getLeftStart() + i;
      int rightIndex = parameter.getRightStart() + i;

      boolean isNull = parameter.getLeft().isNull(leftIndex);

      if (isNull != parameter.getRight().isNull(rightIndex)) {
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

  protected boolean compareBaseVariableWidthVectors(RangeEqualsParameter parameter) {
    BaseVariableWidthVector leftVector = (BaseVariableWidthVector) parameter.getLeft();
    BaseVariableWidthVector rightVector = (BaseVariableWidthVector) parameter.getRight();

    for (int i = 0; i < parameter.getLength(); i++) {
      int leftIndex = parameter.getLeftStart() + i;
      int rightIndex = parameter.getRightStart() + i;

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

  protected boolean compareListVectors(RangeEqualsParameter parameter) {
    ListVector leftVector = (ListVector) parameter.getLeft();
    ListVector rightVector = (ListVector) parameter.getRight();

    RangeEqualsVisitor visitor = createInnerVisitor();
    RangeEqualsParameter innerParam = new RangeEqualsParameter()
            .setLeft(leftVector.getDataVector())
            .setRight(rightVector.getDataVector())
            .setTypeCheckNeeded(parameter.isTypeCheckNeeded());

    for (int i = 0; i < parameter.getLength(); i++) {
      int leftIndex = parameter.getLeftStart() + i;
      int rightIndex = parameter.getRightStart() + i;

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

        innerParam.setRightStart(startIndexRight)
                .setLeftStart(startIndexLeft)
                .setLength(endIndexLeft - startIndexLeft);
        if (!leftVector.getDataVector().accept(visitor, innerParam)) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareFixedSizeListVectors(RangeEqualsParameter parameter) {
    FixedSizeListVector leftVector = (FixedSizeListVector) parameter.getLeft();
    FixedSizeListVector rightVector = (FixedSizeListVector) parameter.getRight();

    if (leftVector.getListSize() != rightVector.getListSize()) {
      return false;
    }

    RangeEqualsVisitor visitor = createInnerVisitor();
    RangeEqualsParameter innerParam = new RangeEqualsParameter()
            .setLeft(leftVector.getDataVector())
            .setRight(rightVector.getDataVector())
            .setTypeCheckNeeded(parameter.isTypeCheckNeeded());

    for (int i = 0; i < parameter.getLength(); i++) {
      int leftIndex = parameter.getLeftStart() + i;
      int rightIndex = parameter.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);
      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      int listSize = leftVector.getListSize();

      if (!isNull) {
        final int startIndexLeft = leftIndex * listSize;
        final int endIndexLeft = (leftIndex + 1) * listSize;

        final int startIndexRight = rightIndex * listSize;
        final int endIndexRight = (rightIndex + 1) * listSize;

        if ((endIndexLeft - startIndexLeft) != (endIndexRight - startIndexRight)) {
          return false;
        }

        innerParam.setRightStart(startIndexRight)
                .setLeftStart(startIndexLeft)
                .setLength(endIndexLeft - startIndexLeft);
        if (!leftVector.getDataVector().accept(visitor, innerParam)) {
          return false;
        }
      }
    }
    return true;
  }
}
