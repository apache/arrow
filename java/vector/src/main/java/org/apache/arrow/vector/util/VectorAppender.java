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

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.util.HashSet;

import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

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

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // optimization, nothing to append, return
    }

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
    if (targetVector instanceof BitVector) {
      // special processing for bit vector, as its type width is 0
      BitVectorHelper.concatBits(targetVector.getDataBuffer(), targetVector.getValueCount(),
              deltaVector.getDataBuffer(), deltaVector.getValueCount(), targetVector.getDataBuffer());

    } else {
      MemoryUtil.UNSAFE.copyMemory(deltaVector.getDataBuffer().memoryAddress(),
              targetVector.getDataBuffer().memoryAddress() + deltaVector.getTypeWidth() * targetVector.getValueCount(),
              deltaVector.getTypeWidth() * deltaVector.getValueCount());
    }
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(BaseVariableWidthVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // nothing to append, return
    }

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetDataSize = targetVector.getOffsetBuffer().getInt(
            (long) targetVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
    int deltaDataSize = deltaVector.getOffsetBuffer().getInt(
            (long) deltaVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);
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
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getDataBuffer().memoryAddress(),
            targetVector.getDataBuffer().memoryAddress() + targetDataSize, deltaDataSize);

    // copy offset buffer
    MemoryUtil.UNSAFE.copyMemory(
            deltaVector.getOffsetBuffer().memoryAddress() + BaseVariableWidthVector.OFFSET_WIDTH,
            targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
                    BaseVariableWidthVector.OFFSET_WIDTH,
            deltaVector.getValueCount() * BaseVariableWidthVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      int oldOffset = targetVector.getOffsetBuffer().getInt((long) (targetVector.getValueCount() + 1 + i) *
              BaseVariableWidthVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setInt(
              (long) (targetVector.getValueCount() + 1 + i) *
                      BaseVariableWidthVector.OFFSET_WIDTH, oldOffset + targetDataSize);
    }
    ((BaseVariableWidthVector) targetVector).setLastSet(newValueCount - 1);
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(BaseLargeVariableWidthVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // nothing to append, return
    }

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    long targetDataSize = targetVector.getOffsetBuffer().getLong(
            (long) targetVector.getValueCount() * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    long deltaDataSize = deltaVector.getOffsetBuffer().getLong(
            (long) deltaVector.getValueCount() * BaseLargeVariableWidthVector.OFFSET_WIDTH);
    long newValueCapacity = targetDataSize + deltaDataSize;

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }
    while (targetVector.getDataBuffer().capacity() < newValueCapacity) {
      ((BaseLargeVariableWidthVector) targetVector).reallocDataBuffer();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append data buffer
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getDataBuffer().memoryAddress(),
            targetVector.getDataBuffer().memoryAddress() + targetDataSize, deltaDataSize);

    // copy offset buffer
    MemoryUtil.UNSAFE.copyMemory(
            deltaVector.getOffsetBuffer().memoryAddress() + BaseLargeVariableWidthVector.OFFSET_WIDTH,
            targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
                    BaseLargeVariableWidthVector.OFFSET_WIDTH,
            deltaVector.getValueCount() * BaseLargeVariableWidthVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      long oldOffset = targetVector.getOffsetBuffer().getLong((long) (targetVector.getValueCount() + 1 + i) *
              BaseLargeVariableWidthVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setLong(
              (long) (targetVector.getValueCount() + 1 + i) *
                      BaseLargeVariableWidthVector.OFFSET_WIDTH, oldOffset + targetDataSize);
    }
    ((BaseLargeVariableWidthVector) targetVector).setLastSet(newValueCount - 1);
    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(ListVector deltaVector, Void value) {
    Preconditions.checkArgument(typeVisitor.equals(deltaVector),
          "The targetVector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // nothing to append, return
    }

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    int targetListSize = targetVector.getOffsetBuffer().getInt(
          (long) targetVector.getValueCount() * ListVector.OFFSET_WIDTH);
    int deltaListSize = deltaVector.getOffsetBuffer().getInt(
          (long) deltaVector.getValueCount() * ListVector.OFFSET_WIDTH);

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
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getOffsetBuffer().memoryAddress() + ListVector.OFFSET_WIDTH,
          targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
              ListVector.OFFSET_WIDTH,
          (long) deltaVector.getValueCount() * ListVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      int oldOffset = targetVector.getOffsetBuffer().getInt(
          (long) (targetVector.getValueCount() + 1 + i) * ListVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setInt((long) (targetVector.getValueCount() + 1 + i) * ListVector.OFFSET_WIDTH,
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
  public ValueVector visit(LargeListVector deltaVector, Void value) {
    Preconditions.checkArgument(typeVisitor.equals(deltaVector),
            "The targetVector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // nothing to append, return
    }

    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    long targetListSize = targetVector.getOffsetBuffer().getLong(
            (long) targetVector.getValueCount() * LargeListVector.OFFSET_WIDTH);
    long deltaListSize = deltaVector.getOffsetBuffer().getLong(
            (long) deltaVector.getValueCount() * LargeListVector.OFFSET_WIDTH);

    ListVector targetListVector = (ListVector) targetVector;

    // make sure the underlying vector has value count set
    // todo recheck these casts when int64 vectors are supported
    targetListVector.getDataVector().setValueCount(checkedCastToInt(targetListSize));
    deltaVector.getDataVector().setValueCount(checkedCastToInt(deltaListSize));

    // make sure there is enough capacity
    while (targetVector.getValueCapacity() < newValueCount) {
      targetVector.reAlloc();
    }

    // append validity buffer
    BitVectorHelper.concatBits(
            targetVector.getValidityBuffer(), targetVector.getValueCount(),
            deltaVector.getValidityBuffer(), deltaVector.getValueCount(), targetVector.getValidityBuffer());

    // append offset buffer
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getOffsetBuffer().memoryAddress() + ListVector.OFFSET_WIDTH,
            targetVector.getOffsetBuffer().memoryAddress() + (targetVector.getValueCount() + 1) *
                    LargeListVector.OFFSET_WIDTH,
            (long) deltaVector.getValueCount() * ListVector.OFFSET_WIDTH);

    // increase each offset from the second buffer
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      long oldOffset = targetVector.getOffsetBuffer().getLong(
          (long) (targetVector.getValueCount() + 1 + i) * LargeListVector.OFFSET_WIDTH);
      targetVector.getOffsetBuffer().setLong((long) (targetVector.getValueCount() + 1 + i) *
          LargeListVector.OFFSET_WIDTH, oldOffset + targetListSize);
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

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // optimization, nothing to append, return
    }

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

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // optimization, nothing to append, return
    }

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

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // optimization, nothing to append, return
    }

    UnionVector targetUnionVector = (UnionVector) targetVector;
    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    // make sure there is enough capacity
    while (targetUnionVector.getValueCapacity() < newValueCount) {
      targetUnionVector.reAlloc();
    }

    // append type buffers
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getTypeBufferAddress(),
            targetUnionVector.getTypeBufferAddress() + targetVector.getValueCount(),
            deltaVector.getValueCount());

    // build the hash set for all types
    HashSet<Integer> targetTypes = new HashSet<>();
    for (int i = 0; i < targetUnionVector.getValueCount(); i++) {
      targetTypes.add(targetUnionVector.getTypeValue(i));
    }
    HashSet<Integer> deltaTypes = new HashSet<>();
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      deltaTypes.add(deltaVector.getTypeValue(i));
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
  public ValueVector visit(DenseUnionVector deltaVector, Void value) {
    // we only make sure that both vectors are union vectors.
    Preconditions.checkArgument(targetVector.getMinorType() == deltaVector.getMinorType(),
        "The vector to append must have the same type as the targetVector being appended");

    if (deltaVector.getValueCount() == 0) {
      return targetVector; // optimization, nothing to append, return
    }

    DenseUnionVector targetDenseUnionVector = (DenseUnionVector) targetVector;
    int newValueCount = targetVector.getValueCount() + deltaVector.getValueCount();

    // make sure there is enough capacity
    while (targetDenseUnionVector.getValueCapacity() < newValueCount) {
      targetDenseUnionVector.reAlloc();
    }

    // append type buffers
    MemoryUtil.UNSAFE.copyMemory(deltaVector.getTypeBuffer().memoryAddress(),
        targetDenseUnionVector.getTypeBuffer() .memoryAddress() + targetVector.getValueCount(),
        deltaVector.getValueCount());

    // append offset buffers
    for (int i = 0; i < deltaVector.getValueCount(); i++) {
      byte typeId = deltaVector.getTypeId(i);
      ValueVector targetChildVector = targetDenseUnionVector.getVectorByType(typeId);
      int offsetBase = targetChildVector == null ? 0 : targetChildVector.getValueCount();
      int deltaOffset = deltaVector.getOffset(i);
      long index = (long) (targetVector.getValueCount() + i) * DenseUnionVector.OFFSET_WIDTH;

      targetVector.getOffsetBuffer().setInt(index, offsetBase + deltaOffset);
    }

    // append child vectors
    for (int i = 0; i <= Byte.MAX_VALUE; i++) {
      ValueVector targetChildVector = targetDenseUnionVector.getVectorByType((byte) i);
      ValueVector deltaChildVector = deltaVector.getVectorByType((byte) i);

      if (targetChildVector == null && deltaChildVector == null) {
        // the type id is not registered in either vector, we are done.
        continue;
      } else if (targetChildVector == null && deltaChildVector != null) {
        // first register a new child in the target vector
        targetDenseUnionVector.registerNewTypeId(deltaChildVector.getField());
        targetChildVector = targetDenseUnionVector.addVector(
            (byte) i, deltaChildVector.getField().createVector(targetDenseUnionVector.getAllocator()));

        // now we have both child vecors not null, we can append them.
        VectorAppender childAppender = new VectorAppender(targetChildVector);
        deltaChildVector.accept(childAppender, null);
      } else if (targetChildVector != null && deltaChildVector == null) {
        // the value only exists in the target vector, so we are done
        continue;
      } else {
        // both child vectors are non-null

        // first check vector types
        TypeEqualsVisitor childTypeVisitor =
            new TypeEqualsVisitor(targetChildVector, /* check name */ false, /* check meta data*/ false);
        if (!childTypeVisitor.equals(deltaChildVector)) {
          throw new IllegalArgumentException("dense union vectors have different child vector types with type id " + i);
        }

        // append child vectors
        VectorAppender childAppender = new VectorAppender(targetChildVector);
        deltaChildVector.accept(childAppender, null);
      }
    }

    targetVector.setValueCount(newValueCount);
    return targetVector;
  }

  @Override
  public ValueVector visit(NullVector deltaVector, Void value) {
    Preconditions.checkArgument(targetVector.getField().getType().equals(deltaVector.getField().getType()),
            "The targetVector to append must have the same type as the targetVector being appended");
    return targetVector;
  }

  @Override
  public ValueVector visit(ExtensionTypeVector<?> deltaVector, Void value) {
    ValueVector targetUnderlying = ((ExtensionTypeVector<?>) targetVector).getUnderlyingVector();
    VectorAppender underlyingAppender = new VectorAppender(targetUnderlying);
    deltaVector.getUnderlyingVector().accept(underlyingAppender, null);
    return targetVector;
  }
}
