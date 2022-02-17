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

package org.apache.arrow.vector.validate;

import static org.apache.arrow.vector.validate.ValidateUtil.validateOrThrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Utility for validating vector data.
 */
public class ValidateVectorDataVisitor implements VectorVisitor<Void, Void> {

  private void validateOffsetBuffer(ValueVector vector, int valueCount) {
    if (valueCount == 0) {
      return;
    }
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();

    // verify that the values in the offset buffer is non-decreasing
    int prevValue = offsetBuffer.getInt(0);
    for (int i = 1; i <= valueCount; i++) {
      int curValue = offsetBuffer.getInt(i * 4);
      validateOrThrow(curValue >= 0, "The value at position %s of the offset buffer is negative: %s.", i, curValue);
      validateOrThrow(curValue >= prevValue,
          "The values in positions %s and %s of the offset buffer are decreasing: %s, %s.",
          i - 1, i, prevValue, curValue);
      prevValue = curValue;
    }
  }

  private void validateLargeOffsetBuffer(ValueVector vector, int valueCount) {
    if (valueCount == 0) {
      return;
    }
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();

    // verify that the values in the large offset buffer is non-decreasing
    long prevValue = offsetBuffer.getLong(0);
    for (int i = 1; i <= valueCount; i++) {
      long curValue = offsetBuffer.getLong((long) i * 8);
      validateOrThrow(curValue >= 0L, "The value at position %s of the large offset buffer is negative: %s.",
          i, curValue);
      validateOrThrow(curValue >= prevValue,
          "The values in positions %s and %s of the large offset buffer are decreasing: %s, %s.",
          i - 1, i, prevValue, curValue);
      prevValue = curValue;
    }
  }

  private void validateTypeBuffer(ArrowBuf typeBuf, int valueCount) {
    for (int i = 0; i < valueCount; i++) {
      validateOrThrow(typeBuf.getByte(i) >= 0, "The type id at position %s is negative: %s.",
          i, typeBuf.getByte(i));
    }
  }

  @Override
  public Void visit(BaseFixedWidthVector vector, Void value) {
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Void value) {
    validateOffsetBuffer(vector, vector.getValueCount());
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector vector, Void value) {
    validateLargeOffsetBuffer(vector, vector.getValueCount());
    return null;
  }

  @Override
  public Void visit(ListVector vector, Void value) {
    validateOffsetBuffer(vector, vector.getValueCount());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(FixedSizeListVector vector, Void value) {
    validateOffsetBuffer(vector, vector.getValueCount());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(LargeListVector vector, Void value) {
    validateLargeOffsetBuffer(vector, vector.getValueCount());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NonNullableStructVector vector, Void value) {
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(UnionVector vector, Void value) {
    validateTypeBuffer(vector.getTypeBuffer(), vector.getValueCount());
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(DenseUnionVector vector, Void value) {
    validateTypeBuffer(vector.getTypeBuffer(), vector.getValueCount());

    // validate offset buffer
    for (int i = 0; i < vector.getValueCount(); i++) {
      int offset = vector.getOffset(i);
      byte typeId = vector.getTypeId(i);
      ValueVector subVector = vector.getVectorByType(typeId);
      validateOrThrow(offset < subVector.getValueCount(),
          "Dense union vector offset exceeds sub-vector boundary. Vector offset %s, sub vector size %s",
          offset, subVector.getValueCount());
    }

    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NullVector vector, Void value) {
    return null;
  }

  @Override
  public Void visit(ExtensionTypeVector<?> vector, Void value) {
    vector.getUnderlyingVector().accept(this, value);
    return null;
  }
}
