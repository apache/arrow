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

package org.apache.arrow.vector.util;

import java.util.List;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ArrowBufPointerNode;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Given a vector and an index, generates a tree representation of the underlying values.
 */
public class ArrowBufPointerTreeGenerator implements VectorVisitor<ArrowBufPointerNode, Integer> {

  @Override
  public ArrowBufPointerNode visit(BaseFixedWidthVector vector, Integer index) {
    return new ArrowBufPointerNode(vector.getDataPointer(index));
  }

  @Override
  public ArrowBufPointerNode visit(BaseVariableWidthVector vector, Integer index) {
    return new ArrowBufPointerNode(vector.getDataPointer(index));
  }

  @Override
  public ArrowBufPointerNode visit(ListVector vector, Integer index) {
    if (vector.isNull(index)) {
      return new ArrowBufPointerNode(new ArrowBufPointer());
    }

    int startIdx = vector.getOffsetBuffer().getInt(index * ListVector.OFFSET_WIDTH);
    int endIdx = vector.getOffsetBuffer().getInt((index + 1) * ListVector.OFFSET_WIDTH);

    ArrowBufPointerNode[] children = new ArrowBufPointerNode[endIdx - startIdx];
    FieldVector childVector = vector.getDataVector();
    for (int i = startIdx; i < endIdx; i++) {
      children[i - startIdx] = childVector.accept(this, i);
    }
    return new ArrowBufPointerNode(children);
  }

  @Override
  public ArrowBufPointerNode visit(FixedSizeListVector vector, Integer index) {
    if (vector.isNull(index)) {
      return new ArrowBufPointerNode(new ArrowBufPointer());
    }

    int listLength = vector.getListSize();

    int startIdx = index * listLength;
    int endIdx = (index + 1) * listLength;

    ArrowBufPointerNode[] children = new ArrowBufPointerNode[endIdx - startIdx];
    FieldVector childVector = vector.getDataVector();
    for (int i = startIdx; i < endIdx; i++) {
      children[i - startIdx] = childVector.accept(this, i);
    }
    return new ArrowBufPointerNode(children);
  }

  @Override
  public ArrowBufPointerNode visit(NonNullableStructVector vector, Integer index) {
    if (vector.isNull(index)) {
      return new ArrowBufPointerNode(new ArrowBufPointer());
    }

    List<FieldVector> childVectors = vector.getChildrenFromFields();
    ArrowBufPointerNode[] children = new ArrowBufPointerNode[childVectors.size()];

    for (int i = 0; i < children.length; i++) {
      children[i] = childVectors.get(i).accept(this, index);
    }
    return new ArrowBufPointerNode(children);
  }

  @Override
  public ArrowBufPointerNode visit(UnionVector vector, Integer index) {
    if (vector.isNull(index)) {
      return new ArrowBufPointerNode(new ArrowBufPointer());
    }

    return vector.getVector(index).accept(this, index);
  }

  @Override
  public ArrowBufPointerNode visit(NullVector vector, Integer index) {
    return new ArrowBufPointerNode(new ArrowBufPointer());
  }
}
