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

package org.apache.arrow.algorithm.pointer;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Vector visitor for generating {@link ArrowBufPointer}s, given a vector and index.
 * This only works for {@link org.apache.arrow.vector.ElementAddressableVector}s.
 */
public class ArrowBufPointerGenerator implements VectorVisitor<ArrowBufPointer, Integer> {

  @Override
  public ArrowBufPointer visit(BaseFixedWidthVector vector, Integer index) {
    if (vector instanceof BitVector) {
      throw new UnsupportedOperationException();
    }
    if (vector.isNull(index)) {
      return ArrowBufPointer.NULL_POINTER;
    }
    return new ArrowBufPointer(vector.getDataBuffer(), (long) index * vector.getTypeWidth(), vector.getTypeWidth());
  }

  @Override
  public ArrowBufPointer visit(BaseVariableWidthVector vector, Integer index) {
    if (vector.isNull(index)) {
      return ArrowBufPointer.NULL_POINTER;
    }

    int startIdx = vector.getOffsetBuffer().getInt(index * BaseVariableWidthVector.OFFSET_WIDTH);
    int endIdx = vector.getOffsetBuffer().getInt((index + 1) * BaseVariableWidthVector.OFFSET_WIDTH);
    return new ArrowBufPointer(vector.getDataBuffer(), startIdx, endIdx - startIdx);
  }

  @Override
  public ArrowBufPointer visit(BaseLargeVariableWidthVector vector, Integer index) {
    if (vector.isNull(index)) {
      return ArrowBufPointer.NULL_POINTER;
    }

    long startIdx = vector.getOffsetBuffer().getLong((long) index * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    long endIdx = vector.getOffsetBuffer().getLong((long) (index + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    return new ArrowBufPointer(vector.getDataBuffer(), startIdx, endIdx - startIdx);
  }

  @Override
  public ArrowBufPointer visit(ListVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(FixedSizeListVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(LargeListVector left, Integer value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(NonNullableStructVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(UnionVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(DenseUnionVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer visit(NullVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }
}
