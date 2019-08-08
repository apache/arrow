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
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Visitor to compare vectors equal.
 */
public class VectorEqualsVisitor extends RangeEqualsVisitor {

  public VectorEqualsVisitor(ValueVector right) {
    super(right, 0, 0, right.getValueCount());
  }

  @Override
  protected boolean compareBaseFixedWidthVectors(BaseFixedWidthVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareBaseFixedWidthVectors(left);
  }

  @Override
  protected boolean compareBaseVariableWidthVectors(BaseVariableWidthVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareBaseVariableWidthVectors(left);
  }

  @Override
  protected boolean compareListVectors(ListVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareListVectors(left);
  }

  @Override
  protected boolean compareFixedSizeListVectors(FixedSizeListVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareFixedSizeListVectors(left);
  }

  @Override
  protected boolean compareStructVectors(NonNullableStructVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareStructVectors(left);
  }

  @Override
  protected boolean compareUnionVectors(UnionVector left) {
    if (left.getValueCount() != right.getValueCount()) {
      return  false;
    }
    return super.compareUnionVectors(left);
  }
}
