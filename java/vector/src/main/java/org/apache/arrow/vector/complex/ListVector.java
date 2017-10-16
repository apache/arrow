/*******************************************************************************

 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.apache.arrow.vector.complex;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

public class ListVector extends BaseRepeatedValueVector implements FieldVector, PromotableVector {

  public static ListVector empty(String name, BufferAllocator allocator) {
    return new ListVector(name, allocator, FieldType.nullable(ArrowType.List.INSTANCE), null);
  }

  private ArrowBuf validityBuffer;
  private UnionListReader reader;
  private CallBack callBack;
  private final FieldType fieldType;
  private int validityAllocationSizeInBytes;
  private int lastSet;

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public ListVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, FieldType.nullable(ArrowType.List.INSTANCE), callBack);
  }

  // deprecated, use FieldType or static constructor instead
  @Deprecated
  public ListVector(String name, BufferAllocator allocator, DictionaryEncoding dictionary, CallBack callBack) {
    this(name, allocator, new FieldType(true, ArrowType.List.INSTANCE, dictionary, null), callBack);
  }

  public ListVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.reader = new UnionListReader(this);
    this.fieldType = checkNotNull(fieldType);
    this.callBack = callBack;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    this.lastSet = 0;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (children.size() != 1) {
      throw new IllegalArgumentException("Lists have only one child. Found: " + children);
    }
    Field field = children.get(0);
    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(field.getFieldType());
    if (!addOrGetVector.isCreated()) {
      throw new IllegalArgumentException("Child vector already existed: " + addOrGetVector.getVector());
    }

    addOrGetVector.getVector().initializeChildrenFromFields(field.getChildren());
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return singletonList(getDataVector());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 2) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 2 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);
    ArrowBuf offBuffer = ownBuffers.get(1);

    validityBuffer.release();
    validityBuffer = bitBuffer.retain(allocator);
    offsetBuffer.release();
    offsetBuffer = offBuffer.retain(allocator);

    validityAllocationSizeInBytes = validityBuffer.capacity();
    offsetAllocationSizeInBytes = offsetBuffer.capacity();

    lastSet = fieldNode.getLength();
    valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(2);
    validityBuffer.readerIndex(0);
    validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
    offsetBuffer.readerIndex(0);
    offsetBuffer.writerIndex((valueCount + 1) * OFFSET_WIDTH);

    result.add(validityBuffer);
    result.add(offsetBuffer);

    return result;
  }

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  public UnionListWriter getWriter() {
    return new UnionListWriter(this);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
   if (!allocateNewSafe()) {
     throw new OutOfMemoryException("Failure while allocating memory");
   }
  }

  public boolean allocateNewSafe() {
    boolean success = false;
    try {
      /* allocate validity buffer */
      allocateValidityBuffer(validityAllocationSizeInBytes);
      /* allocate offset and data buffer */
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
    final int curSize = (int)size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    /* reallocate the validity buffer */
    reallocValidityBuffer();
    /* reallocate the offset and data */
    super.reAlloc();
  }

  private void reallocValidityAndOffsetBuffers() {
    reallocOffsetBuffer();
    reallocValidityBuffer();
  }

  private void reallocValidityBuffer() {
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long)currentBufferCapacity) {
      baseSize = (long)currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int)newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    validityBuffer.release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int)newAllocationSize;
  }

  public void copyFromSafe(int inIndex, int outIndex, ListVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int inIndex, int outIndex, ListVector from) {
    FieldReader in = from.getReader();
    in.setPosition(inIndex);
    FieldWriter out = getWriter();
    out.setPosition(outIndex);
    ComplexCopier.copy(in, out);
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new TransferImpl(ref, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((ListVector) target);
  }

  @Override
  public long getValidityBufferAddress() {
    return (validityBuffer.memoryAddress());
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    return (offsetBuffer.memoryAddress());
  }

  @Override
  public ArrowBuf getValidityBuffer() { return validityBuffer; }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() { return offsetBuffer; }

  private class TransferImpl implements TransferPair {

    ListVector to;
    TransferPair dataTransferPair;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      this(new ListVector(name, allocator, fieldType, callBack));
    }

    public TransferImpl(ListVector to) {
      this.to = to;
      to.addOrGetVector(vector.getField().getFieldType());
      if (to.getDataVector() instanceof ZeroVector) {
        to.addOrGetVector(vector.getField().getFieldType());
      }
      dataTransferPair = getDataVector().makeTransferPair(to.getDataVector());
    }

    @Override
    public void transfer() {
      dataTransferPair.transfer();
      to.validityBuffer = validityBuffer.transferOwnership(to.allocator).buffer;
      to.offsetBuffer = offsetBuffer.transferOwnership(to.allocator).buffer;
      to.lastSet = lastSet;
      to.setValueCount(valueCount);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      final int startPoint = offsetBuffer.getInt(startIndex * OFFSET_WIDTH);
      final int sliceLength = offsetBuffer.getInt((startIndex + length) * OFFSET_WIDTH) - startPoint;
      to.clear();
      to.allocateOffsetBuffer((length + 1) * OFFSET_WIDTH);
      /* splitAndTransfer offset buffer */
      for (int i = 0; i < length + 1; i++) {
        final int relativeOffset = offsetBuffer.getInt((startIndex + i) * OFFSET_WIDTH) - startPoint;
        to.offsetBuffer.setInt(i * OFFSET_WIDTH, relativeOffset);
      }
      /* splitAndTransfer validity buffer */
      splitAndTransferValidityBuffer(startIndex, length, to);
      /* splitAndTransfer data buffer */
      dataTransferPair.splitAndTransfer(startPoint, sliceLength);
      to.lastSet = length;
      to.setValueCount(length);
    }

    /*
     * transfer the validity.
     */
    private void splitAndTransferValidityBuffer(int startIndex, int length, ListVector target) {
      assert startIndex + length <= valueCount;
      int firstByteSource = BitVectorHelper.byteIndex(startIndex);
      int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
      int byteSizeTarget = getValidityBufferSizeFromCount(length);
      int offset = startIndex % 8;

      if (length > 0) {
        if (offset == 0) {
          // slice
          if (target.validityBuffer != null) {
            target.validityBuffer.release();
          }
          target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
          target.validityBuffer.retain(1);
        }
        else {
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
          if((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
            byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                    firstByteSource + byteSizeTarget - 1, offset);
            byte b2 = BitVectorHelper.getBitsFromNextByte(validityBuffer,
                    firstByteSource + byteSizeTarget, offset);

            target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
          }
          else {
            byte b1 = BitVectorHelper.getBitsFromCurrentByte(validityBuffer,
                    firstByteSource + byteSizeTarget - 1, offset);
            target.validityBuffer.setByte(byteSizeTarget - 1, b1);
          }
        }
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, ListVector.this);
    }
  }

  @Override
  public UnionListReader getReader() {
    return reader;
  }

  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    AddOrGetResult<T> result = super.addOrGetVector(fieldType);
    reader = new UnionListReader(this);
    return result;
  }

  @Override
  public int getBufferSize() {
    if (getValueCount() == 0) {
      return 0;
    }
    final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    return offsetBufferSize + validityBufferSize + vector.getBufferSize();
  }

  @Override
  public Field getField() {
    return new Field(name, fieldType, ImmutableList.of(getDataVector().getField()));
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
  }

  @Override
  public void clear() {
    super.clear();
    validityBuffer = releaseBuffer(validityBuffer);
    lastSet = 0;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(new ArrowBuf[]{offsetBuffer}, ObjectArrays.concat(new ArrowBuf[] {validityBuffer},
        vector.getBuffers(false), ArrowBuf.class), ArrowBuf.class);
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.retain();
      }
      clear();
    }
    return buffers;
  }

  @Override
  public UnionVector promoteToUnion() {
    UnionVector vector = new UnionVector("$data$", allocator, callBack);
    replaceDataVector(vector);
    reader = new UnionListReader(this);
    if (callBack != null) {
      callBack.doWork();
    }
    return vector;
  }

  @Override
  public Object getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    }
    final List<Object> vals = new JsonStringArrayList<>();
    final int start = offsetBuffer.getInt(index * OFFSET_WIDTH);
    final int end = offsetBuffer.getInt((index + 1) * OFFSET_WIDTH);
    final ValueVector vv = getDataVector();
    for (int i = start; i < end; i++) {
      vals.add(vv.getObject(i));
    }

    return vals;
  }

  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return Long.bitCount(b & (1L << bitIndex));
  }

  @Override
  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  public void setNotNull(int index) {
    if (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    lastSet = index + 1;
  }

  @Override
  public int startNewValue(int index) {
    if (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    for (int i = lastSet; i <= index; i++) {
      final int currentOffset = offsetBuffer.getInt(i * OFFSET_WIDTH);
      offsetBuffer.setInt((i + 1) * OFFSET_WIDTH, currentOffset);
    }
    setNotNull(index);
    lastSet = index + 1;
    return offsetBuffer.getInt(lastSet * OFFSET_WIDTH);
  }

  /**
   * End the current value
   *
   * @param index index of the value to end
   * @param size  number of elements in the list that was written
   */
  public void endValue(int index, int size) {
    final int currentOffset = offsetBuffer.getInt((index + 1) * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, currentOffset + size);
  }

  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    if (valueCount > 0) {
      while (valueCount > getValueCapacity()) {
        /* realloc the inner buffers if needed */
        reallocValidityAndOffsetBuffers();
      }
      for (int i = lastSet; i < valueCount; i++) {
        /* fill the holes with offsets */
        final int currentOffset = offsetBuffer.getInt(i * OFFSET_WIDTH);
        offsetBuffer.setInt((i + 1) * OFFSET_WIDTH, currentOffset);
      }
    }
    /* valueCount for the data vector is the current end offset */
    final int childValueCount = (valueCount == 0) ? 0 :
            offsetBuffer.getInt(valueCount * OFFSET_WIDTH);
    vector.setValueCount(childValueCount);
  }

  public void setLastSet(int value) {
    lastSet = value;
  }

  public int getLastSet() {
    return lastSet;
  }
}
