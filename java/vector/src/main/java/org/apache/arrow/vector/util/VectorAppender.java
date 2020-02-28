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

import java.util.HashSet;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

import io.netty.util.internal.PlatformDependent;

/**
 * Utility to append two vectors together.
 */
class VectorAppender implements VectorVisitor<ValueVector, Void> {

  /**
   * The targetVector to be appended.
   */
  private final ValueVector targetVector;

  private final TypeEqualsVisitor typeVisitor;

  /**
   * Constructs a new targetVector appender, with the given targetVector.
   * @param targetVector the targetVector to be appended.
   */
  VectorAppender(ValueVector targetVector) {
    this.targetVector = targetVector;
    typeVisitor = new TypeEqualsVisitor(targetVector, false, true);
  }

  @Override
  public ValueVector visit(BaseFixedWidthVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append data buffer
    PlatformDependent.copyMemory(deltaVector.getDataBuffer().memoryAddress(),
            targetVector.getDataBuffer().memoryAddress() + deltaVector.getTypeWidth() * targetVector.getValueCount(),
            deltaVector.getTypeWidth() * deltaVector.getValueCount());
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(BaseVariableWidthVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetDataSize = targetVector.getOffsetBuffer().getInt(
            targetVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
    int deltaDataSize = deltaVector.getOffsetBuffer().getInt(
            deltaVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
    int newValueCapacity = targetDataSize + deltaDataSize;

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }
    while (targetVector.getDataBuffer().capacity() < newValueCapacity) {
      ((BaseVariableWidthVector) targetVector).reallocDataBuffer();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append data buffer
    PlatformDependent.copyMemory(deltaVector.getDataBuffer().memoryAddress(),
            targetVector.getDataBuffer().memoryAddress() + targetDataSize, deltaDataSize);

    // copy offset buffer
    PlatformDependent.copyMemory(
            deltaVector.getOffsetBuffer().memoryAddress() + BaseVariableWidthVector.OFFSET_WIDTH,
            targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
                    BaseVariableWidthVector.OFFSET_WIDTH,
            deltaVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      int oldOffset = targetVector.getOffsetBuffer().getInt((targetVector.getValueCount() + 1 + i) *
              BaseVariableWidthVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setInt(
              (targetVector.getValueCount() + 1 + i) *
                      BaseVariableWidthVector.OFFSET_WIDTH, oldOffset + targetDataSize);
    }
    ((BaseVariableWidthVector) targetVector).setLastSet(newValueCount - 1);
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(ListVector deltaVector, Void value) {
    Preconditions.checkArgument(typeVisitor.equals(deltaVector),
            "The targetVector to append must have the same type as the targetVector being appended");

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetListSize = targetVector.getOffsetBuffer().getInt(
            targetVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
    int deltaListSize = deltaVector.getOffsetBuffer().getInt(
            deltaVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);

    ListVector targetListVector = (ListVector) targetVector;

    // make sure the underlying vector has value count set
    targetListVector.getDataVector().setValueCount(targetListSize);
    deltaVector.getDataVector().setValueCount(deltaListSize);

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append offset buffer
    PlatformDependent.copyMemory(deltaVector.getOffsetBuffer().memoryAddress() + ListVector.OFFSET_WIDTH,
            targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
                    ListVector.OFFSET_WIDTH,
            deltaVector.getValueCount() * ListVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      int oldOffset =
              targetVector.getOffsetBuffer().getInt((targetVector.getValueCount() + 1 + i) * ListVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setInt((targetVector.getValueCount() + 1 + i) * ListVector.OFFSET_WIDTH,
              oldOffset + targetListSize);
    }
    targetListVector.setLastSet(newValueCount - 1);

    // append underlying vectors
    VectorAppender innerAppender = new VectorAppender(targetListVector.getDataVector());
    deltaVector.getDataVector().accept(innerAppender, null);

    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(FixedSizeListVector deltaVector, Void value) {
    Preconditions.checkArgument(typeVisitor.equals(deltaVector),
            "The vector to append must have the same type as the targetVector being appended");

    FixedSizeListVector targetListVector = (FixedSizeListVector) targetVector;

    Preconditions.checkArgument(targetListVector.getListSize() == deltaVector.getListSize(),
            "FixedSizeListVector must have the same list size to append");

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetListSize = targetListVector.getValueCount() * targetListVector.getListSize();
    int deltaListSize = deltaVector.getValueCount() * deltaVector.getListSize();

    // make sure the underlying vector has value count set
    targetListVector.getDataVector().setValueCount(targetListSize);
    deltaVector.getDataVector().setValueCount(deltaListSize);

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append underlying vectors
    VectorAppender innerAppender = new VectorAppender(targetListVector.getDataVector());
    deltaVector.getDataVector().accept(innerAppender, null);

    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(NonNullableStructVector deltaVector, Void value) {
    Preconditions.checkArgument(typeVisitor.equals(deltaVector),
            "The vector to append must have the same type as the targetVector being appended");

    NonNullableStructVector targetStructVector = (NonNullableStructVector) targetVector;
    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append child vectors
    for (int i = 0; i < targetStructVector.getChildrenFromFields().size(); i++) {
      ValueVector targetChild = targetStructVector.getVectorById(i);
      ValueVector deltaChild = deltaVector.getVectorById(i);

      targetChild.setValueCount(targetStructVector.getValueCount());
      deltaChild.setValueCount(deltaVector.getValueCount());

      VectorAppender innerAppender = new VectorAppender(targetChild);
      deltaChild.accept(innerAppender, null);
    }

    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(UnionVector deltaVector, Void value) {
    // we only make sure that both vectors are union vectors.
    Preconditions.checkArgument(targetVector.getMinorType() == deltaVector.getMinorType(),
            "The vector to append must have the same type as the targetVector being appended");

    UnionVector targetUnionVector = (UnionVector) targetVector;
    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    // make sure there is enough capacity
    while (targetUnionVector.getValueCapacity() < newValueCount) {
      targetUnionVector.reAlloc();
    }

    // append type buffers
    PlatformDependent.copyMemory(deltaVector.getValidityBufferAddress(),
            targetUnionVector.getValidityBufferAddress() + targetVector.getValueCount(),
            deltaVector.getValueCount());

    // build the hash set for all types
    HashSet<Integer> targetTypes = new HashSet<>();
    for (int i = 0; i < targetUnionVector.getValueCount(); i++) {
      targetTypes.add((int) targetUnionVector.getValidityBuffer().getByte(i));
    }
    HashSet<Integer> deltaTypes = new HashSet<>();
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      deltaTypes.add((int) deltaVector.getValidityBuffer().getByte(i));
    }

    // append child vectors
    for (int i = 0; i < Byte.MAX_VALUE; i++) {
      if (targetTypes.contains(i) || deltaTypes.contains(i)) {
        ValueVector targetChild = targetUnionVector.getVectorByType(i);
        if (!targetTypes.contains(i)) {
          // if the vector type does not exist in the target, it must be newly created
          // and we must make sure it has enough capacity.
          while (targetChild.getValueCapacity() < newValueCount) {
            targetChild.reAlloc();
          }
        }

        if (deltaTypes.contains(i)) {
          // append child vectors
          ValueVector deltaChild = deltaVector.getVectorByType(i);

          targetChild.setValueCount(targetUnionVector.getValueCount());
          deltaChild.setValueCount(deltaVector.getValueCount());

          VectorAppender innerAppender = new VectorAppender(targetChild);
          deltaChild.accept(innerAppender, null);
        }
        targetChild.setValueCount(newValueCount);
      }
    }

    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(DenseUnionVector left, Void value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueVector visit(NullVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");
    return targetVector;
  }
}
