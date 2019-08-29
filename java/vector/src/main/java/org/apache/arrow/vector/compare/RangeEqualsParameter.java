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

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ValueVector;

/**
 * Wrapper for the parameters of comparing a range of values in two vectors.
 */
public class RangeEqualsParameter {

  /**
   * The left vector.
   */
  private ValueVector left;

  /**
   * The right vector.
   */
  private ValueVector right;

  /**
   * Start position in the left vector.
   */
  private int leftStart = -1;

  /**
   * Start position in the right vector.
   */
  private int rightStart = -1;

  /**
   * Length of the range.
   */
  private int length = -1;

  /**
   * If type check is required.
   */
  private boolean typeCheckNeeded;

  /**
   * Checks the type of the left and right vectors.
   * @return true if left and right vectors have the same type, and false otherwise.
   */
  public boolean checkType() {
    if (!typeCheckNeeded) {
      return true;
    }
    return left.getField().getType().equals(right.getField().getType());
  }

  /**
   * Do some validation work, like type check and indices check.
   */
  public boolean validate() {
    if (!checkType()) {
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
    Preconditions.checkArgument(left != null,
            "left vector cannot be null");
    Preconditions.checkArgument(right != null,
            "right vector cannot be null");

    return true;
  }

  public ValueVector getLeft() {
    return left;
  }

  public ValueVector getRight() {
    return right;
  }

  public int getLeftStart() {
    return leftStart;
  }

  public int getRightStart() {
    return rightStart;
  }

  public int getLength() {
    return length;
  }

  public boolean isTypeCheckNeeded() {
    return typeCheckNeeded;
  }

  public RangeEqualsParameter setLeft(ValueVector left) {
    this.left = left;
    return this;
  }

  public RangeEqualsParameter setRight(ValueVector right) {
    this.right = right;
    return this;
  }

  public RangeEqualsParameter setLeftStart(int leftStart) {
    this.leftStart = leftStart;
    return this;
  }

  public RangeEqualsParameter setRightStart(int rightStart) {
    this.rightStart = rightStart;
    return this;
  }

  public RangeEqualsParameter setLength(int length) {
    this.length = length;
    return this;
  }

  public RangeEqualsParameter setTypeCheckNeeded(boolean typeCheckNeeded) {
    this.typeCheckNeeded = typeCheckNeeded;
    return this;
  }
}
