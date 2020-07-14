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
 * Vector visitor for populating {@link ArrowBufPointer}s, given a vector and index.
 * This only works for {@link org.apache.arrow.vector.ElementAddressableVector}s.
 */
public class ArrowBufPointerPopulator implements VectorVisitor<Void, Integer> {

  private ArrowBufPointer pointer;

  /**
   * Constructs a new instance.
   * @param pointer the {@link ArrowBufPointer} to populate.
   */
  public ArrowBufPointerPopulator(ArrowBufPointer pointer) {
    this.pointer = pointer;
  }

  /**
   * Gets the underlying pointer to populate.
   */
  public ArrowBufPointer getPointer() {
    return pointer;
  }

  /**
   * Sets the pointer to populate.
   */
  public void setPointer(ArrowBufPointer pointer) {
    this.pointer = pointer;
  }

  @Override
  public Void visit(BaseFixedWidthVector vector, Integer index) {
    if (vector instanceof BitVector) {
      throw new UnsupportedOperationException();
    }

    if (vector.isNull(index)) {
      pointer.buf = null;
      return null;
    }

    pointer.buf = vector.getDataBuffer();
    pointer.offset = (long) index * vector.getTypeWidth();
    pointer.length = vector.getTypeWidth();
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Integer index) {
    if (vector.isNull(index)) {
      pointer.buf = null;
      return null;
    }

    int startIdx = vector.getOffsetBuffer().getInt(index * BaseVariableWidthVector.OFFSET_WIDTH);
    int endIdx = vector.getOffsetBuffer().getInt((index + 1) * BaseVariableWidthVector.OFFSET_WIDTH);

    pointer.buf = vector.getDataBuffer();
    pointer.offset = startIdx;
    pointer.length = endIdx - startIdx;
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector vector, Integer index) {
    if (vector.isNull(index)) {
      pointer.buf = null;
      return null;
    }

    long startIdx = vector.getOffsetBuffer().getLong((long) index * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    long endIdx = vector.getOffsetBuffer().getLong((long) (index + 1) * BaseLargeVariableWidthVector.OFFSET_WIDTH);

    pointer.buf = vector.getDataBuffer();
    pointer.offset = startIdx;
    pointer.length = endIdx - startIdx;
    return null;
  }

  @Override
  public Void visit(ListVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(FixedSizeListVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(LargeListVector left, Integer value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(NonNullableStructVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(UnionVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(DenseUnionVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(NullVector vector, Integer index) {
    throw new UnsupportedOperationException();
  }
}
