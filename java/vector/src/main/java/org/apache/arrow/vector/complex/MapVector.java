/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.complex;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.impl.NullableMapReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableMapWriter;
import org.apache.arrow.vector.holders.ComplexHolder;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

public class MapVector extends NonNullableMapVector implements FieldVector {

  public static MapVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(Struct.INSTANCE);
    return new MapVector(name, allocator, fieldType, null);
  }

  private final NullableMapReaderImpl reader = new NullableMapReaderImpl(this);
  private final NullableMapWriter writer = new NullableMapWriter(this);

  protected ArrowBuf validityBuffer;
  private int validityAllocationSizeInBytes;

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public MapVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, FieldType.nullable(ArrowType.Struct.INSTANCE), callBack);
  }

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public MapVector(String name, BufferAllocator allocator, DictionaryEncoding dictionary, CallBack callBack) {
    this(name, allocator, new FieldType(true, ArrowType.Struct.INSTANCE, dictionary, null), callBack);
  }

  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, checkNotNull(allocator), fieldType, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(BaseValueVector.INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public Field getField() {
    Field f = super.getField();
    FieldType type = new FieldType(true, f.getType(), f.getFieldType().getDictionary(), f.getFieldType().getMetadata());
    return new Field(f.getName(), type, f.getChildren());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 1 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);

    validityBuffer.release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    valueCount = fieldNode.getLength();
    validityAllocationSizeInBytes = validityBuffer.capacity();
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

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  @Override
  public NullableMapReaderImpl getReader() {
    return reader;
  }

  public NullableMapWriter getWriter() {
    return writer;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new MapVector(name, allocator, fieldType, null), false);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new NullableMapTransferPair(this, (MapVector) to, true);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new NullableMapTransferPair(this, new MapVector(ref, allocator, fieldType, null), false);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new NullableMapTransferPair(this, new MapVector(ref, allocator, fieldType, callBack), false);
  }

  protected class NullableMapTransferPair extends MapTransferPair {

    private MapVector target;

    protected NullableMapTransferPair(MapVector from, MapVector to, boolean allocate) {
      super(from, to, allocate);
      this.target = to;
    }

    @Override
    public void transfer() {
      target.clear();
      target.validityBuffer = validityBuffer.transferOwnership(target.allocator).buffer;
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
      target.clear();
      splitAndTransferValidityBuffer(startIndex, length, target);
      super.splitAndTransfer(startIndex, length);
    }
  }

  /*
   * transfer the validity.
   */
  private void splitAndTransferValidityBuffer(int startIndex, int length, MapVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = BitVectorHelper.getValidityBufferSize(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        // slice
        if (target.validityBuffer != null) {
          target.validityBuffer.release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        target.allocateValidityBuffer(byteSizeTarget);

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer, firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(validityBuffer, firstByteSource + i + 1, offset);

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
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(validityBuffer,
                  firstByteSource + byteSizeTarget, offset);

          target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          target.validityBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
  }

  /**
   * Get the value capacity of the internal validity buffer.
   * @return number of elements that validity buffer can hold
   */
  private int getValidityBufferValueCapacity() {
    return (int) (validityBuffer.capacity() * 8L);
  }

  /**
   * Get the current value capacity for the vector
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    return Math.min(getValidityBufferValueCapacity(),
            super.getValueCapacity());
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't
   * impact the reference counts for this buffer so it only should be used for in-context
   * access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   *
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted
   *              but the returned array will be the only reference to them
   * @return The underlying {@link io.netty.buffer.ArrowBuf buffers} that is used by this
   *         vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    setReaderAndWriterIndex();
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      buffers = ObjectArrays.concat(new ArrowBuf[]{validityBuffer}, super.getBuffers(false),
              ArrowBuf.class);
    }
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.retain();
      }
      clear();
    }

    return buffers;
  }

  /**
   * Close the vector and release the associated buffers.
   */
  @Override
  public void close() {
    clearValidityBuffer();
    super.close();
  }

  /**
   * Same as {@link #close()}
   */
  @Override
  public void clear() {
    clearValidityBuffer();
    super.clear();
  }

  /**
   * Release the validity buffer
   */
  private void clearValidityBuffer() {
    validityBuffer.release();
    validityBuffer = allocator.getEmpty();
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this
   * vector
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSize() +
            BitVectorHelper.getValidityBufferSize(valueCount);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   * @param valueCount desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds
   *         a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return super.getBufferSizeFor(valueCount)
            + BitVectorHelper.getValidityBufferSize(valueCount);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = BitVectorHelper.getValidityBufferSize(numRecords);
    super.setInitialCapacity(numRecords);
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
        return false;
      }
    }
    return true;
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
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setZero(0, newBuf.capacity());
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    validityBuffer.release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
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
  public Object getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return super.getObject(index);
    }
  }

  @Override
  public void get(int index, ComplexHolder holder) {
    holder.isSet = isSet(index);
    super.get(index, holder);
  }

  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  public boolean isNull(int index) {
    return isSet(index) == 0;
  }

  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return Long.bitCount(b & (1L << bitIndex));
  }

  public void setIndexDefined(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
  }

  public void setNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  @Override
  public void setValueCount(int valueCount) {
    assert valueCount >= 0;
    while (valueCount > getValidityBufferValueCapacity()) {
      /* realloc the inner buffers if needed */
      reallocValidityBuffer();
    }
    super.setValueCount(valueCount);
    this.valueCount = valueCount;
  }

  public void reset() {
    valueCount = 0;
  }

  @Override
  @Deprecated
  public Accessor getAccessor() {
    throw new UnsupportedOperationException("Accessor is not supported for reading from Nullable MAP");
  }

  @Override
  @Deprecated
  public Mutator getMutator() {
    throw new UnsupportedOperationException("Mutator is not supported for writing to Nullable MAP");
  }
}
