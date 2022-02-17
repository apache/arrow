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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * Visitor to validate vector (without validating data).
 * This visitor could be used for {@link ValueVector#accept(VectorVisitor, Object)} API,
 * and also users could simply use {@link ValueVectorUtility#validate(ValueVector)}.
 */
public class ValidateVectorVisitor implements VectorVisitor<Void, Void> {

  @Override
  public Void visit(BaseFixedWidthVector vector, Void value) {
    if (vector.getValueCount() > 0) {
      if (vector.getDataBuffer() == null || vector.getDataBuffer().capacity() == 0) {
        throw new IllegalArgumentException("valueBuffer is null or capacity is 0");
      }
    }
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Void value) {

    if (vector.getValueCount() > 0) {
      if (vector.getDataBuffer() == null || vector.getDataBuffer().capacity() == 0) {
        throw new IllegalArgumentException("valueBuffer is null or capacity is 0");
      }

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      int minBufferSize = (vector.getValueCount() + 1) * BaseVariableWidthVector.OFFSET_WIDTH;

      if (offsetBuf.capacity() < minBufferSize) {
        throw new IllegalArgumentException(String.format("offsetBuffer too small in vector of type %s" +
                " and valueCount %s : expected at least %s byte(s), got %s",
            vector.getField().getType().toString(),
            vector.getValueCount(), minBufferSize, offsetBuf.capacity()));
      }

      int firstOffset = vector.getOffsetBuffer().getInt(0);
      int lastOffset = vector.getOffsetBuffer().getInt(vector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);

      if (firstOffset < 0 || lastOffset < 0) {
        throw new IllegalArgumentException("Negative offsets in vector");
      }

      int dataExtent = lastOffset - firstOffset;

      if (dataExtent > 0 && (vector.getDataBuffer().capacity() == 0)) {
        throw new IllegalArgumentException("dataBuffer capacity is 0");
      }

      if (dataExtent > vector.getDataBuffer().capacity()) {
        throw new IllegalArgumentException(String.format("Length spanned by offsets %s larger than" +
            " dataBuffer capacity %s", dataExtent, vector.getValueCount()));
      }
    }
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector left, Void value) {
    return null;
  }

  @Override
  public Void visit(ListVector vector, Void value) {

    FieldVector dataVector = vector.getDataVector();

    if (vector.getValueCount() > 0) {

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      int minBufferSize = (vector.getValueCount() + 1) * BaseVariableWidthVector.OFFSET_WIDTH;

      if (offsetBuf.capacity() < minBufferSize) {
        throw new IllegalArgumentException(String.format("offsetBuffer too small in vector of type %s" +
                " and valueCount %s : expected at least %s byte(s), got %s",
            vector.getField().getType().toString(),
            vector.getValueCount(), minBufferSize, offsetBuf.capacity()));
      }

      int firstOffset = vector.getOffsetBuffer().getInt(0);
      int lastOffset = vector.getOffsetBuffer().getInt(vector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);

      if (firstOffset < 0 || lastOffset < 0) {
        throw new IllegalArgumentException("Negative offsets in list vector");
      }

      int dataExtent = lastOffset - firstOffset;

      if (dataExtent > 0 && (dataVector.getDataBuffer() == null || dataVector.getDataBuffer().capacity() == 0)) {
        throw new IllegalArgumentException("valueBuffer is null or capacity is 0");
      }

      if (dataExtent > dataVector.getValueCount()) {
        throw new IllegalArgumentException(String.format("Length spanned by list offsets (%s) larger than" +
            " data vector valueCount (length %s)", dataExtent, dataVector.getValueCount()));
      }
    }

    return dataVector.accept(this, null);
  }

  @Override
  public Void visit(LargeListVector vector, Void value) {

    FieldVector dataVector = vector.getDataVector();

    if (vector.getValueCount() > 0) {

      ArrowBuf offsetBuf = vector.getOffsetBuffer();
      long minBufferSize = (vector.getValueCount() + 1) * LargeListVector.OFFSET_WIDTH;

      if (offsetBuf.capacity() < minBufferSize) {
        throw new IllegalArgumentException(String.format("offsetBuffer too small in vector of type %s" +
                " and valueCount %s : expected at least %s byte(s), got %s",
            vector.getField().getType().toString(),
            vector.getValueCount(), minBufferSize, offsetBuf.capacity()));
      }

      long firstOffset = vector.getOffsetBuffer().getLong(0);
      long lastOffset = vector.getOffsetBuffer().getLong(vector.getValueCount() * LargeListVector.OFFSET_WIDTH);

      if (firstOffset < 0 || lastOffset < 0) {
        throw new IllegalArgumentException("Negative offsets in list vector");
      }

      long dataExtent = lastOffset - firstOffset;

      if (dataExtent > 0 && (dataVector.getDataBuffer() == null || dataVector.getDataBuffer().capacity() == 0)) {
        throw new IllegalArgumentException("valueBuffer is null or capacity is 0");
      }

      if (dataExtent > dataVector.getValueCount()) {
        throw new IllegalArgumentException(String.format("Length spanned by list offsets (%s) larger than" +
            " data vector valueCount (length %s)", dataExtent, dataVector.getValueCount()));
      }
    }

    return dataVector.accept(this, null);
  }

  @Override
  public Void visit(FixedSizeListVector vector, Void value) {

    FieldVector dataVector = vector.getDataVector();
    int valueCount = vector.getValueCount();
    int listSize = vector.getListSize();

    if (valueCount > 0 && (dataVector.getDataBuffer() == null || dataVector.getDataBuffer().capacity() == 0)) {
      throw new IllegalArgumentException("valueBuffer is null or capacity is 0");
    }

    if (valueCount * listSize != dataVector.getValueCount()) {
      throw new IllegalArgumentException(String.format("data vector valueCount invalid, expect %s, " +
          "actual is: %s", valueCount * listSize, dataVector.getValueCount()));
    }

    return null;
  }

  @Override
  public Void visit(NonNullableStructVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();
    final int valueCount = vector.getValueCount();

    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (child.getValueCount() != valueCount) {
        throw new IllegalArgumentException(String.format("struct child vector #%s valueCount is not equals with " +
            "struct vector, expect %s, actual %s", i, vector.getValueCount(), child.getValueCount()));
      }

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        throw new IllegalArgumentException(String.format("struct child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      child.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(UnionVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();
    final int valueCount = vector.getValueCount();

    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (child.getValueCount() != valueCount) {
        throw new IllegalArgumentException(String.format("union child vector #%s valueCount is not equals with union" +
            " vector, expect %s, actual %s", i, vector.getValueCount(), child.getValueCount()));
      }

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        throw new IllegalArgumentException(String.format("union child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      child.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(DenseUnionVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();
    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        throw new IllegalArgumentException(String.format("union child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      child.accept(this, null);
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
