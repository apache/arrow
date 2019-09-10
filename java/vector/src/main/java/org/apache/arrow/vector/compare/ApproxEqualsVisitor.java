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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;

/**
 * Visitor to compare floating point.
 */
public class ApproxEqualsVisitor extends RangeEqualsVisitor {

  /**
   * The float/double values are treated as equal as long as the difference calculated by function is <= epsilon.
   */
  private final float floatEpsilon;
  private final double doubleEpsilon;

  /**
   * Functions to calculate difference between float/double values.
   */
  private DiffFunction<Float> floatDiffFunction =
      (Float value1, Float value2) -> Math.abs(value1 - value2);
  private DiffFunction<Double> doubleDiffFunction =
      (Double value1, Double value2) -> Math.abs(value1 - value2);

  /**
   * Constructs a new instance.
   *
   * @param left left vector
   * @param right right vector
   * @param floatEpsilon difference for float values
   * @param doubleEpsilon difference for double values
   */
  public ApproxEqualsVisitor(ValueVector left, ValueVector right, float floatEpsilon, double doubleEpsilon) {
    super(left, right, true);
    this.floatEpsilon = floatEpsilon;
    this.doubleEpsilon = doubleEpsilon;
  }

  public void setFloatDiffFunction(DiffFunction<Float> floatDiffFunction) {
    this.floatDiffFunction = floatDiffFunction;
  }

  public void setDoubleDiffFunction(DiffFunction<Double> doubleDiffFunction) {
    this.doubleDiffFunction = doubleDiffFunction;
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, Range range) {
    if (left instanceof Float4Vector) {
      return float4ApproxEquals(range);
    } else if (left instanceof Float8Vector) {
      return float8ApproxEquals(range);
    } else {
      return super.visit(left, range);
    }
  }

  @Override
  protected ApproxEqualsVisitor createInnerVisitor(ValueVector left, ValueVector right) {
    return new ApproxEqualsVisitor(left, right, floatEpsilon, doubleEpsilon);
  }

  private boolean float4ApproxEquals(Range range) {
    Float4Vector leftVector  = (Float4Vector) getLeft();
    Float4Vector rightVector  = (Float4Vector) getRight();

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);

      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {
        Float leftValue = leftVector.get(leftIndex);
        Float rightValue = rightVector.get(rightIndex);
        if (leftValue.isNaN()) {
          return rightValue.isNaN();
        }
        if (leftValue.isInfinite()) {
          return rightValue.isInfinite() && Math.signum(leftValue) == Math.signum(rightValue);
        }
        if (floatDiffFunction.apply(leftValue, rightValue) > floatEpsilon) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean float8ApproxEquals(Range range) {
    Float8Vector leftVector  = (Float8Vector) getLeft();
    Float8Vector rightVector  = (Float8Vector) getRight();

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      boolean isNull = leftVector.isNull(leftIndex);

      if (isNull != rightVector.isNull(rightIndex)) {
        return false;
      }

      if (!isNull) {

        Double leftValue = leftVector.get(leftIndex);
        Double rightValue = rightVector.get(rightIndex);
        if (leftValue.isNaN()) {
          return rightValue.isNaN();
        }
        if (leftValue.isInfinite()) {
          return rightValue.isInfinite() && Math.signum(leftValue) == Math.signum(rightValue);
        }
        if (doubleDiffFunction.apply(leftValue, rightValue) > doubleEpsilon) {
          return false;
        }
      }
    }
    return true;
  }
}


