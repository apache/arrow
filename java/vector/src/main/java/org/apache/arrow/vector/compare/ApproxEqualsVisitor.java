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
 * Visitor to compare floating point vectors approximately.
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
  private DiffFunctionFloat floatDiffFunction;
  private DiffFunctionDouble doubleDiffFunction;

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

    floatDiffFunction =
        (float value1, float value2) -> Math.abs(value1 - value2) <= floatEpsilon;
    doubleDiffFunction =
        (double value1, double value2) -> Math.abs(value1 - value2) <= doubleEpsilon;
  }

  public void setFloatDiffFunction(DiffFunctionFloat floatDiffFunction) {
    this.floatDiffFunction = floatDiffFunction;
  }

  public void setDoubleDiffFunction(DiffFunctionDouble doubleDiffFunction) {
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
        float leftValue = leftVector.get(leftIndex);
        float rightValue = rightVector.get(rightIndex);
        if (Float.isNaN(leftValue)) {
          return Float.isNaN(rightValue);
        }
        if (Float.isInfinite(leftValue)) {
          return Float.isInfinite(rightValue) && Math.signum(leftValue) == Math.signum(rightValue);
        }
        if (!floatDiffFunction.approxEquals(leftValue, rightValue)) {
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

        double leftValue = leftVector.get(leftIndex);
        double rightValue = rightVector.get(rightIndex);
        if (Double.isNaN(leftValue)) {
          return Double.isNaN(rightValue);
        }
        if (Double.isInfinite(leftValue)) {
          return Double.isInfinite(rightValue) && Math.signum(leftValue) == Math.signum(rightValue);
        }
        if (!doubleDiffFunction.approxEquals(leftValue, rightValue)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Difference function for float values.
   */
  public interface DiffFunctionFloat {
    boolean approxEquals(float v1, float v2);
  }

  /**
   * Difference function for double values.
   */
  public interface DiffFunctionDouble {
    boolean approxEquals(double v1, double v2);
  }
}
