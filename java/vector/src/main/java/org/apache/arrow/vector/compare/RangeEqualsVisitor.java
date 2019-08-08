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

import java.util.ArrayList;
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
    return false;
  }

  protected boolean compareUnionVectors(UnionVector left) {
    if (!left.getField().getType().equals(right.getField().getType())) {
      return false;
    }

    UnionVector rightVector = (UnionVector) right;

    List<FieldVector> leftChildrens = left.getChildrenFromFields();
    List<FieldVector> rightChildrens = rightVector.getChildrenFromFields();

    if (leftChildrens.size() != rightChildrens.size()) {
      return false;
    }

    for (int k = 0; k < leftChildrens.size(); k++) {
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(rightChildrens.get(k),
          leftStart, rightStart, length);
      if (!leftChildrens.get(k).accept(visitor)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareStructVectors(NonNullableStructVector left) {
    if (!left.getField().getType().equals(right.getField().getType())) {
      return false;
    }

    NonNullableStructVector rightVector = (NonNullableStructVector) right;

    if (!left.getChildFieldNames().equals(rightVector.getChildFieldNames())) {
      return false;
    }

    List<ValueVector> leftChildrens = new ArrayList<>();
    List<ValueVector> rightChildrens = new ArrayList<>();

    for (String child : left.getChildFieldNames()) {
      ValueVector v = left.getChild(child);
      if (v != null) {
        leftChildrens.add(v);
      }
    }

    for (String child : rightVector.getChildFieldNames()) {
      ValueVector v = rightVector.getChild(child);
      if (v != null) {
        rightChildrens.add(v);
      }
    }

    if (leftChildrens.size() != rightChildrens.size()) {
      return false;
    }

    for (int k = 0; k < leftChildrens.size(); k++) {
      RangeEqualsVisitor visitor = new RangeEqualsVisitor(rightChildrens.get(k),
          leftStart, rightStart, length);
      if (!leftChildrens.get(k).accept(visitor)) {
        return false;
      }
    }
    return true;
  }

  protected boolean compareBaseFixedWidthVectors(BaseFixedWidthVector left) {

    if (!left.getField().getType().equals(right.getField().getType())) {
      return false;
    }

    int typeWidth = left.getTypeWidth();

    for (int i = 0; i < length; i++) {
      int leftIndex = leftStart + i;
      int rightIndex = rightStart + i;

      boolean isNull = left.isNull(leftIndex);
      if (isNull != right.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {
        int start1 = typeWidth * leftIndex;
        int end1 = typeWidth * (leftIndex + 1);

        int start2 = typeWidth * rightIndex;
        int end2 = typeWidth * (rightIndex + 1);

        int ret = ByteFunctionHelpers.equal(left.getDataBuffer(), start1, end1,
            right.getDataBuffer(), start2, end2);

        if (ret == 0) {
          return false;
        }

      }
    }

    return true;
  }

  protected boolean compareBaseVariableWidthVectors(BaseVariableWidthVector left) {
    if (!left.getField().getType().equals(right.getField().getType())) {
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
    }
    return true;
  }

  protected boolean compareListVectors(ListVector left) {
    if (!left.getField().getType().equals(right.getField().getType())) {
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
        final int start1 = left.getOffsetBuffer().getInt(leftIndex * offsetWidth);
        final int end1 = left.getOffsetBuffer().getInt((leftIndex + 1) * offsetWidth);

        final int start2 = right.getOffsetBuffer().getInt(rightIndex * offsetWidth);
        final int end2 = right.getOffsetBuffer().getInt((rightIndex + 1) * offsetWidth);

        if ((end1 - start1) != (end2 - start2)) {
          return false;
        }

        ValueVector dataVector1 = left.getDataVector();
        ValueVector dataVector2 = ((ListVector)right).getDataVector();

        if (!dataVector1.accept(new RangeEqualsVisitor(dataVector2, start1, start2, (end1 - start1)))) {
          return false;
        }
      }
    }
    return true;
  }

  protected boolean compareFixedSizeListVectors(FixedSizeListVector left) {
    if (!left.getField().getType().equals(right.getField().getType())) {
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
        final int start1 = leftIndex * listSize;
        final int end1 = (leftIndex + 1) * listSize;

        final int start2 = rightIndex * listSize;
        final int end2 = (rightIndex + 1) * listSize;

        if ((end1 - start1) != (end2 - start2)) {
          return false;
        }

        ValueVector dataVector1 = left.getDataVector();
        ValueVector dataVector2 = ((FixedSizeListVector)right).getDataVector();

        if (!dataVector1.accept(new RangeEqualsVisitor(dataVector2, start1, start2, (end1 - start1)))) {
          return false;
        }
      }
    }
    return true;
  }

}
