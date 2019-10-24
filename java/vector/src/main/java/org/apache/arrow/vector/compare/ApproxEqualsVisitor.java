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
import org.apache.arrow.vector.compare.util.ValueEpsilonEqualizers;

/**
 * Visitor to compare floating point vectors approximately.
 */
public class ApproxEqualsVisitor extends RangeEqualsVisitor {

  /**
   * Functions to calculate difference between float/double values.
   */
  private final VectorValueEqualizer<Float4Vector> floatDiffFunction;
  private final VectorValueEqualizer<Float8Vector> doubleDiffFunction;

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

    floatDiffFunction = new ValueEpsilonEqualizers.Float4EpsilonEqualizer(floatEpsilon);
    doubleDiffFunction = new ValueEpsilonEqualizers.Float8EpsilonEqualizer(doubleEpsilon);
  }

  /**
   * Constructs a new instance.
   * @param left the left vector.
   * @param right the right vector.
   * @param floatDiffFunction the equalizer for float values.
   * @param doubleDiffFunction the equalizer for double values.
   */
  public ApproxEqualsVisitor(ValueVector left, ValueVector right,
                             VectorValueEqualizer<Float4Vector> floatDiffFunction,
                             VectorValueEqualizer<Float8Vector> doubleDiffFunction) {
    super(left, right, true);
    this.floatDiffFunction = floatDiffFunction;
    this.doubleDiffFunction = doubleDiffFunction;
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, Range range) {
    if (left instanceof Float4Vector) {
      if (!validate(left)) {
        return false;
      }
      return float4ApproxEquals(range);
    } else if (left instanceof Float8Vector) {
      if (!validate(left)) {
        return false;
      }
      return float8ApproxEquals(range);
    } else {
      return super.visit(left, range);
    }
  }

  @Override
  protected ApproxEqualsVisitor createInnerVisitor(ValueVector left, ValueVector right) {
    return new ApproxEqualsVisitor(left, right, floatDiffFunction.clone(), doubleDiffFunction.clone());
  }

  private boolean float4ApproxEquals(Range range) {
    Float4Vector leftVector  = (Float4Vector) getLeft();
    Float4Vector rightVector  = (Float4Vector) getRight();

    for (int i = 0; i < range.getLength(); i++) {
      int leftIndex = range.getLeftStart() + i;
      int rightIndex = range.getRightStart() + i;

      if (!floatDiffFunction.valuesEqual(leftVector, leftIndex, rightVector, rightIndex)) {
        return false;
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

      if (!doubleDiffFunction.valuesEqual(leftVector, leftIndex, rightVector, rightIndex)) {
        return false;
      }
    }
    return true;
  }
}
