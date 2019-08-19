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
public class ApproxEqualsVisitor extends VectorEqualsVisitor {

  private final float epsilon;

  public ApproxEqualsVisitor(ValueVector right, float epsilon) {
    super(right);
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
        float rightValue = ((Float4Vector)right).get(leftIndex);
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
        double rightValue = ((Float8Vector)right).get(leftIndex);
        if (Math.abs(leftValue - rightValue) > epsilon) {
          return false;
        }
      }
    }
    return true;
  }
}
