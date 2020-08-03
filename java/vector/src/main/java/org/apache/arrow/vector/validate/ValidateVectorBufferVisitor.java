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

import static org.apache.arrow.vector.validate.ValidateUtility.validateOrThrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TypeLayout;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Visitor to validate vector buffers.
 */
public class ValidateVectorBufferVisitor implements VectorVisitor<Void, Void> {

  private void validateVectorCommon(ValueVector vector) {
    ArrowType arrowType = vector.getField().getType();
    validateOrThrow(vector.getValueCount() >= 0, "vector valueCount is negative");

    if (vector instanceof FieldVector) {
      FieldVector fieldVector = (FieldVector) vector;
      int typeBufferCount = TypeLayout.getTypeBufferCount(arrowType);
      validateOrThrow(fieldVector.getFieldBuffers().size() == typeBufferCount,
          String.format("Expected %s buffers in vector of type %s, got %s",
              typeBufferCount, vector.getField().getType().toString(), fieldVector.getFieldBuffers().size()));
    }
  }

  private void validateValidityBuffer(ValueVector vector, int valueCount) {
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    validateOrThrow(validityBuffer != null, "The validity buffer is null.");
    validateOrThrow(validityBuffer.capacity() * 8 >= valueCount, "No enough capacity for the validity buffer.");
  }

  private void validateOffsetBuffer(ValueVector vector, int valueCount) {
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();
    validateOrThrow(offsetBuffer != null, "The offset buffer is null.");
    if (valueCount > 0) {
      validateOrThrow(offsetBuffer.capacity() >= (valueCount + 1) * 4, "No enough capacity for the offset buffer.");
    }
  }

  private void validateLargeOffsetBuffer(ValueVector vector, int valueCount) {
    ArrowBuf offsetBuffer = vector.getOffsetBuffer();
    validateOrThrow(offsetBuffer != null, "The large offset buffer is null.");
    if (valueCount > 0) {
      validateOrThrow(offsetBuffer.capacity() >= (valueCount + 1) * 8,
          "No enough capacity for the large offset buffer.");
    }
  }

  private void validateFixedWidthDataBuffer(ValueVector vector, int valueCount, int bitWidth) {
    ArrowBuf dataBuffer = vector.getDataBuffer();
    validateOrThrow(dataBuffer != null, "The fixed width data buffer is null.");
    validateOrThrow((long) bitWidth * valueCount <= dataBuffer.capacity() * 8L,
        "No enough capacity for fixed width data buffer");
  }

  private void validateDataBuffer(ValueVector vector, long minCapacity) {
    ArrowBuf dataBuffer = vector.getDataBuffer();
    validateOrThrow(dataBuffer != null, "The data buffer is null.");
    validateOrThrow(dataBuffer.capacity() >= minCapacity, "No enough capacity for data buffer");
  }

  @Override
  public Void visit(BaseFixedWidthVector vector, Void value) {
    int bitWidth = (vector instanceof BitVector) ? 1 : vector.getTypeWidth() * 8;
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    validateFixedWidthDataBuffer(vector, valueCount, bitWidth);
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    validateOffsetBuffer(vector, valueCount);
    int lastOffset = valueCount == 0 ? 0 :
        vector.getOffsetBuffer().getInt(valueCount * BaseVariableWidthVector.OFFSET_WIDTH);
    validateDataBuffer(vector, lastOffset);
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    validateLargeOffsetBuffer(vector, valueCount);
    long lastOffset = valueCount == 0 ? 0L :
        vector.getOffsetBuffer().getLong((long) valueCount * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    validateDataBuffer(vector, lastOffset);
    return null;
  }

  @Override
  public Void visit(ListVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    validateOffsetBuffer(vector, valueCount);

    FieldVector dataVector = vector.getDataVector();
    int lastOffset = valueCount == 0 ? 0 :
        vector.getOffsetBuffer().getInt(valueCount * BaseVariableWidthVector.OFFSET_WIDTH);
    int dataVectorLength = dataVector == null ? 0 : dataVector.getValueCount();
    validateOrThrow(dataVectorLength >= lastOffset,
        "Inner vector does not contain enough elements.");

    if (dataVector != null) {
      dataVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(FixedSizeListVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    FieldVector dataVector = vector.getDataVector();
    int dataVectorLength = dataVector == null ? 0 : dataVector.getValueCount();
    validateOrThrow(dataVectorLength >= valueCount * vector.getListSize(),
        "Inner vector does not contain enough elements.");
    if (dataVector != null) {
      dataVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(LargeListVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    validateLargeOffsetBuffer(vector, valueCount);

    FieldVector dataVector = vector.getDataVector();
    long lastOffset = valueCount == 0 ? 0 :
        vector.getOffsetBuffer().getLong(valueCount * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    int dataVectorLength = dataVector == null ? 0 : dataVector.getValueCount();
    validateOrThrow(dataVectorLength >= lastOffset,
        "Inner vector does not contain enough elements.");

    if (dataVector != null) {
      dataVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NonNullableStructVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    validateValidityBuffer(vector, valueCount);
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      validateOrThrow(valueCount == subVector.getValueCount(),
          "Struct vector length not equal to child vector length");
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(UnionVector vector, Void value) {
    int valueCount = vector.getValueCount();
    validateVectorCommon(vector);
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      validateOrThrow(valueCount == subVector.getValueCount(),
          "Union vector length not equal to child vector length");
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(DenseUnionVector vector, Void value) {
    validateVectorCommon(vector);
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NullVector vector, Void value) {
    return null;
  }
}
