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
import java.util.Objects;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Visitor to compare type equals for vectors.
 */
public class TypeEqualsVisitor implements VectorVisitor<Boolean, Void> {

  private final ValueVector right;

  private final boolean checkName;
  private final boolean checkMetadata;

  /**
   * Construct an instance.
   */
  public TypeEqualsVisitor(ValueVector right) {
    this (right, true, true);
  }

  /**
   * Construct an instance.
   * @param right right vector
   * @param checkName whether checks names
   * @param checkMetadata whether checks metadata
   */
  public TypeEqualsVisitor(ValueVector right, boolean checkName, boolean checkMetadata) {
    this.right = right;
    this.checkName = checkName;
    this.checkMetadata = checkMetadata;
  }

  /**
   * Check type equals without passing IN param in VectorVisitor.
   */
  public boolean equals(ValueVector left) {
    return left.accept(this, null);
  }

  @Override
  public Boolean visit(BaseFixedWidthVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(BaseVariableWidthVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(ListVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(FixedSizeListVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(NonNullableStructVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(UnionVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  @Override
  public Boolean visit(ZeroVector left, Void value) {
    return compareField(left.getField(), right.getField());
  }

  private boolean compareField(Field leftField, Field rightField) {

    if (leftField == rightField) {
      return true;
    }

    return (!checkName || Objects.equals(leftField.getName(), rightField.getName())) &&
        Objects.equals(leftField.isNullable(), rightField.isNullable()) &&
        Objects.equals(leftField.getType(), rightField.getType()) &&
        Objects.equals(leftField.getDictionary(), rightField.getDictionary()) &&
        (!checkMetadata || Objects.equals(leftField.getMetadata(), rightField.getMetadata())) &&
        compareChildren(leftField.getChildren(), rightField.getChildren());
  }

  private boolean compareChildren(List<Field> leftChildren, List<Field> rightChildren) {
    if (leftChildren.size() != rightChildren.size()) {
      return false;
    }

    for (int i = 0; i < leftChildren.size(); i++) {
      if (!compareField(leftChildren.get(i), rightChildren.get(i))) {
        return false;
      }
    }
    return true;
  }
}
