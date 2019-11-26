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

import org.apache.arrow.vector.ValueVector;

/**
 * Visitor to compare vectors equal.
 */
public class VectorEqualsVisitor {

  /**
   * Checks if two vectors are equals.
   * @param left the left vector to compare.
   * @param right the right vector to compare.
   * @return true if the vectors are equal, and false otherwise.
   */
  public static boolean vectorEquals(ValueVector left, ValueVector right) {
    return vectorEquals(left, right, true);
  }

  /**
   * Checks if two vectors are equals.
   */
  public static boolean vectorEquals(ValueVector left, ValueVector right, boolean checkType) {
    return vectorEquals(left, right, checkType, new TypeEqualsVisitor(right));
  }

  /**
   * Checks if two vectors are equals.
   * @param left the left vector to compare.
   * @param right the right vector to compare.
   * @param checkType whether need check equality of types
   * @param typeVisitor type visitor to compare vector fields
   * @return true if the vectors are equal, and false otherwise.
   */
  public static boolean vectorEquals(
      ValueVector left,
      ValueVector right,
      boolean checkType,
      TypeEqualsVisitor typeVisitor) {

    if (left.getValueCount() != right.getValueCount()) {
      return false;
    }

    RangeEqualsVisitor visitor = new RangeEqualsVisitor(left, right, checkType, typeVisitor);
    return visitor.rangeEquals(new Range(0, 0, left.getValueCount()));
  }
}
