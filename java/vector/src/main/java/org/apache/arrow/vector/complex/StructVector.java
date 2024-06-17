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
package org.apache.arrow.vector.complex;

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.NullableStructReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A Struct vector consists of nullability/validity buffer and children vectors that make up the
 * struct's fields. The children vectors are handled by the parent class.
 */
public class StructVector extends NonNullableStructVector
    implements FieldVector, ValueIterableVector<Map<String, ?>> {

  /**
   * Construct a new empty instance which replaces an existing field with the new one in case of
   * name conflict.
   */
  public static StructVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(Struct.INSTANCE);
    return new StructVector(
        name, allocator, fieldType, null, ConflictPolicy.CONFLICT_REPLACE, false);
  }

  /** Construct a new empty instance which preserve fields with identical names. */
  public static StructVector emptyWithDuplicates(String name, BufferAllocator allocator) {
    FieldType fieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    return new StructVector(name, allocator, fieldType, null, ConflictPolicy.CONFLICT_APPEND, true);
  }

  private final NullableStructReaderImpl reader = new NullableStructReaderImpl(this);
  private final NullableStructWriter writer = new NullableStructWriter(this);

  protected ArrowBuf validityBuffer;
  private int validityAllocationSizeInBytes;

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack A schema change callback.
   */
  public StructVector(
      String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, checkNotNull(allocator), fieldType, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes =
        BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack A schema change callback.
   * @param conflictPolicy policy to determine how duplicate names are handled.
   * @param allowConflictPolicyChanges whether duplicate names are allowed at all.
   */
  public StructVector(
      String name,
      BufferAllocator allocator,
      FieldType fieldType,
      CallBack callBack,
      ConflictPolicy conflictPolicy,
      boolean allowConflictPolicyChanges) {
    super(
        name,
        checkNotNull(allocator),
        fieldType,
        callBack,
        conflictPolicy,
        allowConflictPolicyChanges);
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes =
        BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param callBack A schema change callback.
   */
  public StructVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field, checkNotNull(allocator), callBack);
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes =
        BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use to allocating/reallocating buffers.
   * @param callBack A schema change callback.
   * @param conflictPolicy policy to determine how duplicate names are handled.
   * @param allowConflictPolicyChanges whether duplicate names are allowed at all.
   */
  public StructVector(
      Field field,
      BufferAllocator allocator,
      CallBack callBack,
      ConflictPolicy conflictPolicy,
      boolean allowConflictPolicyChanges) {
    super(field, checkNotNull(allocator), callBack, conflictPolicy, allowConflictPolicyChanges);
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes =
        BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException(
          "Illegal buffer count, expected " + 1 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);

    validityBuffer.getReferenceManager().release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    valueCount = fieldNode.getLength();
    validityAllocationSizeInBytes = checkedCastToInt(validityBuffer.capacity());
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);
    setReaderAndWriterIndex();
    result.add(validityBuffer);

    return result;
  }

  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    validityBuffer.writerIndex(BitVectorHelper.getValidityBufferSize(valueCount));
  }

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner
   *     vectors.
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  @Override
  public NullableStructReaderImpl getReader() {
    return reader;
  }

  public NullableStructWriter getWriter() {
    return writer;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new NullableStructTransferPair(
        this,
        new StructVector(
            name,
            allocator,
            field.getFieldType(),
            null,
            getConflictPolicy(),
            allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new NullableStructTransferPair(this, (StructVector) to, false);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new NullableStructTransferPair(
        this,
        new StructVector(
            ref,
            allocator,
            field.getFieldType(),
            null,
            getConflictPolicy(),
            allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new NullableStructTransferPair(
        this,
        new StructVector(
            ref,
            allocator,
            field.getFieldType(),
            callBack,
            getConflictPolicy(),
            allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new NullableStructTransferPair(
        this,
        new StructVector(field, allocator, null, getConflictPolicy(), allowConflictPolicyChanges),
        false);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return new NullableStructTransferPair(
        this,
        new StructVector(
            field, allocator, callBack, getConflictPolicy(), allowConflictPolicyChanges),
        false);
  }

  /** {@link TransferPair} for this (nullable) {@link StructVector}. */
  protected class NullableStructTransferPair extends StructTransferPair {

    private StructVector target;

    protected NullableStructTransferPair(StructVector from, StructVector to, boolean allocate) {
      super(from, to, allocate);
      this.target = to;
    }

    @Override
    public void transfer() {
      target.clear();
      target.validityBuffer = BaseValueVector.transferBuffer(validityBuffer, target.allocator);
      super.transfer();
      clear();
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      while (toIndex >= target.getValidityBufferValueCapacity()) {
        target.reallocValidityBuffer();
      }
      BitVectorHelper.setValidityBit(target.validityBuffer, toIndex, isSet(fromIndex));
      super.copyValueSafe(fromIndex, toIndex);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      Preconditions.checkArgument(
          startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
          "Invalid parameters startIndex: %s, length: %s for valueCount: %s",
          startIndex,
          length,
          valueCount);
      target.clear();
      splitAndTransferValidityBuffer(startIndex, length, target);
      super.splitAndTransfer(startIndex, length);
    }
  }

  /*
   * transfer the validity.
   */
  private void splitAndTransferValidityBuffer(int startIndex, int length, StructVector target) {
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = BitVectorHelper.getValidityBufferSize(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        // slice
        if (target.validityBuffer != null) {
          target.validityBuffer.getReferenceManager().release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.getReferenceManager().retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        target.allocateValidityBuffer(byteSizeTarget);

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(validityBuffer, firstByteSource + i, offset);
          byte b2 =
              BitVectorHelper.getBitsFromNextByte(validityBuffer, firstByteSource + i + 1, offset);

          target.validityBuffer.setByte(i, (b1 + b2));
        }

        /* Copying the last piece is done in the following manner:
         * if the source vector has 1 or more bytes remaining, we copy
         * the last piece as a byte formed by shifting data
         * from the current byte and the next byte.
         *
         * if the source vector has no more bytes remaining
         * (we are at the last byte), we copy the last piece as a byte
         * by shifting data from the current byte.
         */
        if ((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(
                  validityBuffer, firstByteSource + byteSizeTarget - 1, offset);
          byte b2 =
              BitVectorHelper.getBitsFromNextByte(
                  validityBuffer, firstByteSource + byteSizeTarget, offset);

          target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(
                  validityBuffer, firstByteSource + byteSizeTarget - 1, offset);
          target.validityBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
  }

  /**
   * Get the value capacity of the internal validity buffer.
   *
   * @return number of elements that validity buffer can hold
   */
  private int getValidityBufferValueCapacity() {
    return checkedCastToInt(validityBuffer.capacity() * 8);
  }

  /**
   * Get the current value capacity for the vector.
   *
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    return Math.min(getValidityBufferValueCapacity(), super.getValueCapacity());
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the
   * reference counts for this buffer so it only should be used for in-context access. Also note
   * that this buffer changes regularly thus external classes shouldn't hold a reference to it
   * (unless they change it).
   *
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted but
   *     the returned array will be the only reference to them
   * @return The underlying {@link ArrowBuf buffers} that is used by this vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    setReaderAndWriterIndex();
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      List<ArrowBuf> list = new ArrayList<>();
      list.add(validityBuffer);
      list.addAll(Arrays.asList(super.getBuffers(false)));
      buffers = list.toArray(new ArrowBuf[list.size()]);
    }
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.getReferenceManager().retain();
      }
      clear();
    }

    return buffers;
  }

  /** Close the vector and release the associated buffers. */
  @Override
  public void close() {
    clearValidityBuffer();
    super.close();
  }

  /** Same as {@link #close()}. */
  @Override
  public void clear() {
    clearValidityBuffer();
    super.clear();
  }

  /** Reset this vector to empty, does not release buffers. */
  @Override
  public void reset() {
    super.reset();
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  /** Release the validity buffer. */
  private void clearValidityBuffer() {
    validityBuffer.getReferenceManager().release();
    validityBuffer = allocator.getEmpty();
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this vector.
   *
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSize() + BitVectorHelper.getValidityBufferSize(valueCount);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   *
   * @param valueCount desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSizeFor(valueCount) + BitVectorHelper.getValidityBufferSize(valueCount);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(numRecords);
    super.setInitialCapacity(numRecords);
  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {
    validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(numRecords);
    super.setInitialCapacity(numRecords, density);
  }

  @Override
  public boolean allocateNewSafe() {
    /* Boolean to keep track if all the memory allocations were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      clear();
      allocateValidityBuffer(validityAllocationSizeInBytes);
      success = super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    return success;
  }

  private void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    /* reallocate the validity buffer */
    reallocValidityBuffer();
    super.reAlloc();
  }

  private void reallocValidityBuffer() {
    final int currentBufferCapacity = checkedCastToInt(validityBuffer.capacity());
    long newAllocationSize = getNewAllocationSize(currentBufferCapacity);

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    validityBuffer.getReferenceManager().release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  private long getNewAllocationSize(int currentBufferCapacity) {
    long newAllocationSize = currentBufferCapacity * 2L;
    if (newAllocationSize == 0) {
      if (validityAllocationSizeInBytes > 0) {
        newAllocationSize = validityAllocationSizeInBytes;
      } else {
        newAllocationSize =
            BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION) * 2L;
      }
    }
    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }
    return newAllocationSize;
  }

  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, ?> getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return super.getObject(index);
    }
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    if (isSet(index) == 0) {
      return ArrowBufPointer.NULL_HASH_CODE;
    } else {
      return super.hashCode(index, hasher);
    }
  }

  @Override
  public void get(int index, ComplexHolder holder) {
    holder.isSet = isSet(index);
    if (holder.isSet == 0) {
      holder.reader = null;
      return;
    }
    super.get(index, holder);
  }

  /** Return the number of null values in the vector. */
  @Override
  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  /** Returns true if the value at the provided index is null. */
  @Override
  public boolean isNull(int index) {
    return isSet(index) == 0;
  }

  /** Returns true the value at the given index is set (i.e. not null). */
  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Marks the value at index as being set. Reallocates the validity buffer if index is larger than
   * current capacity.
   */
  public void setIndexDefined(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.setBit(validityBuffer, index);
  }

  /** Marks the value at index as null/not set. */
  @Override
  public void setNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.unsetBit(validityBuffer, index);
  }

  @Override
  public void setValueCount(int valueCount) {
    Preconditions.checkArgument(valueCount >= 0);
    while (valueCount > getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    super.setValueCount(valueCount);
    this.valueCount = valueCount;
  }
}
