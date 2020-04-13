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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * visitor to validate vector (not include data).
 */
public class ValidateVectorVisitor implements VectorVisitor<Status, Void> {

  @Override
  public Status visit(BaseFixedWidthVector vector, Void value) {

    if (vector.getValueCount() > 0) {
      if (vector.getDataBuffer() == null || vector.getDataBuffer().capacity() == 0) {
        return Status.invalid("valueBuffer is null or capacity is 0");
      }
    }
    return Status.success();
  }

  @Override
  public Status visit(BaseVariableWidthVector vector, Void value) {

    if (vector.getValueCount() > 0) {
      if (vector.getDataBuffer() == null || vector.getDataBuffer().capacity() == 0) {
        return Status.invalid("valueBuffer is null or capacity is 0");
      }
    }
    return Status.success();
  }

  @Override
  public Status visit(ListVector vector, Void value) {

    FieldVector dataVector = vector.getDataVector();

    if (vector.getValueCount() > 0) {
      int firstOffset = vector.getOffsetBuffer().getInt(0);
      int lastOffset = vector.getOffsetBuffer().getInt(vector.getValueCount() * 4);

      if (firstOffset < 0 || lastOffset < 0) {
        return Status.invalid("Negative offsets in list vector");
      }

      int dataExtent = lastOffset - firstOffset;

      if (dataExtent > 0 && (dataVector.getDataBuffer() == null || dataVector.getDataBuffer().capacity() == 0)) {
        return Status.invalid("values is null");
      }

      if (dataExtent > dataVector.getValueCount()) {
        return Status.invalid(String.format("Length spanned by list offsets (%s) larger than" +
            " data vector valueCount (length %s)", dataExtent, dataVector.getValueCount()));
      }
    }

    Status status = dataVector.accept(this, null);
    if (!status.isSuccess()) {
      return Status.invalid("list data vector invalid:" + status.toString());
    }
    return Status.success();
  }

  @Override
  public Status visit(FixedSizeListVector vector, Void value) {

    FieldVector dataVector = vector.getDataVector();
    int valueCount = vector.getValueCount();
    int listSize = vector.getListSize();

    if (valueCount > 0 && (dataVector.getDataBuffer() == null || dataVector.getDataBuffer().capacity() == 0)) {
      return Status.invalid("values is null");
    }

    if (valueCount * listSize != dataVector.getValueCount()) {
      return Status.invalid(String.format("data vector valueCount invalid, expect %s, actual is: %s",
          valueCount * listSize, dataVector.getValueCount()));
    }

    return Status.success();
  }

  @Override
  public Status visit(NonNullableStructVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();
    final int valueCount = vector.getValueCount();

    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (child.getValueCount() != valueCount) {
        return Status.invalid(String.format("struct child vector #%s valueCount is not equals with " +
            "struct vector, expect %s, actual %s", i, vector.getValueCount(), child.getValueCount()));
      }

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        return Status.invalid(String.format("struct child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      Status status = child.accept(this, null);

      if (!status.isSuccess()) {
        return Status.invalid("struct child vector invalid:" + status.toString());
      }
    }
    return Status.success();
  }

  @Override
  public Status visit(UnionVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();
    final int valueCount = vector.getValueCount();

    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (child.getValueCount() != valueCount) {
        return Status.invalid(String.format("union child vector #%s valueCount is not equals with union" +
            " vector, expect %s, actual %s", i, vector.getValueCount(), child.getValueCount()));
      }

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        return Status.invalid(String.format("union child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      Status status = child.accept(this, null);

      if (!status.isSuccess()) {
        return Status.invalid("union child vector invalid:" + status.toString());
      }
    }
    return Status.success();
  }

  @Override
  public Status visit(DenseUnionVector vector, Void value) {

    List<Field> childFields = vector.getField().getChildren();

    for (int i = 0; i < childFields.size(); i++) {
      FieldVector child = vector.getChildrenFromFields().get(i);

      if (!childFields.get(i).getType().equals(child.getField().getType())) {
        return Status.invalid(String.format("union child vector #%s does not match type: %s vs %s",
            i, childFields.get(i).getType().toString(), child.getField().getType().toString()));
      }

      Status status = child.accept(this, null);

      if (!status.isSuccess()) {
        return Status.invalid("union child vector invalid:" + status.toString());
      }
    }
    return Status.success();
  }

  @Override
  public Status visit(NullVector vector, Void value) {
    return Status.success();
  }

}
