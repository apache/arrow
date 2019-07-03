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

import static java.util.Collections.singletonList;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.DATA_VECTOR_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.impl.UnionFixedSizeListReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/** A ListVector where every list value is of the same size. */
public class FixedSizeListVector extends BaseValueVector implements FieldVector, PromotableVector {

  public static FixedSizeListVector empty(String name, int size, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(new ArrowType.FixedSizeList(size));
    return new FixedSizeListVector(name, allocator, fieldType, null);
  }

  private FieldVector vector;
  private ArrowBuf validityBuffer;
  private final int listSize;
  private final FieldType fieldType;
  private final String name;

  private UnionFixedSizeListReader reader;
  private int valueCount;
  private int validityAllocationSizeInBytes;

  /**
   * @deprecated use FieldType or static constructor instead.
   */
  @Deprecated
  public FixedSizeListVector(String name,
                             BufferAllocator allocator,
                             int listSize,
                             DictionaryEncoding dictionary,
                             CallBack schemaChangeCallback) {
    this(name, allocator, new FieldType(true, new ArrowType.FixedSizeList(listSize), dictionary), schemaChangeCallback);
  }

  /**
   * Creates a new instance.
   *
   * @param name The name for the vector.
   * @param allocator The allocator to use for creating/reallocating buffers for the vector.
   * @param fieldType The underlying data type of the vector.
   * @param unusedSchemaChangeCallback Currently unused.
   */
  public FixedSizeListVector(String name,
                             BufferAllocator allocator,
                             FieldType fieldType,
                             CallBack unusedSchemaChangeCallback) {
    super(allocator);

    this.name = name;
    this.validityBuffer = allocator.getEmpty();
    this.vector = ZeroVector.INSTANCE;
    this.fieldType = fieldType;
    this.listSize = ((ArrowType.FixedSizeList) fieldType.getType()).getListSize();
    Preconditions.checkArgument(listSize > 0, "list size must be positive");
    this.reader = new UnionFixedSizeListReader(this);
    this.valueCount = 0;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public Field getField() {
    List<Field> children = Collections.singletonList(getDataVector().getField());
    return new Field(name, fieldType, children);
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.FIXED_SIZE_LIST;
  }

  @Override
  public String getName() {
    return name;
  }

  /** Get the fixed size for each list. */
  public int getListSize() {
    return listSize;
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
    return singletonList(vector);
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected " + 1 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);

    validityBuffer.getReferenceManager().release();
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
    validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
  }

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  @Override
  public UnionFixedSizeListReader getReader() {
    return reader;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating memory");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* boolean to keep track if all the memory allocation were successful
     * Used in the case of composite vectors when we need to allocate multiple
     * buffers for multiple vectors. If one of the allocations failed we need to
     * clear all the memory that we allocated
     */
    boolean success = false;
    try {
      /* we are doing a new allocation -- release the current buffers */
      clear();
      /* allocate validity buffer */
      allocateValidityBuffer(validityAllocationSizeInBytes);
      success = vector.allocateNewSafe();
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
    reallocValidityBuffer();
    vector.reAlloc();
  }

  private void reallocValidityBuffer() {
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    validityBuffer.getReferenceManager().release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(numRecords);
    vector.setInitialCapacity(numRecords * listSize);
  }

  @Override
  public int getValueCapacity() {
    if (vector == ZeroVector.INSTANCE) {
      return 0;
    }
    return Math.min(vector.getValueCapacity() / listSize, getValidityBufferValueCapacity());
  }

  @Override
  public int getBufferSize() {
    if (getValueCount() == 0) {
      return 0;
    }
    return getValidityBufferSizeFromCount(valueCount) + vector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    return getValidityBufferSizeFromCount(valueCount) +
            vector.getBufferSizeFor(valueCount * listSize);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>singleton(vector).iterator();
  }

  @Override
  public void clear() {
    validityBuffer = releaseBuffer(validityBuffer);
    vector.clear();
    valueCount = 0;
    super.clear();
  }

  @Override
  public void reset() {
    validityBuffer.setZero(0, validityBuffer.capacity());
    vector.reset();
    valueCount = 0;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    setReaderAndWriterIndex();
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      List<ArrowBuf> list = new ArrayList<>();
      list.add(validityBuffer);
      list.addAll(Arrays.asList(vector.getBuffers(false)));
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

  /**
   * Get value indicating if inner vector is set.
   * @return 1 if inner vector is explicitly set via #addOrGetVector else 0
   */
  public int size() {
    return vector == ZeroVector.INSTANCE ? 0 : 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType type) {
    boolean created = false;
    if (vector == ZeroVector.INSTANCE) {
      vector = type.createNewSingleVector(DATA_VECTOR_NAME, allocator, null);
      this.reader = new UnionFixedSizeListReader(this);
      created = true;
    }
    // returned vector must have the same field
    if (!Objects.equals(vector.getField().getType(), type.getType())) {
      final String msg = String.format("Inner vector type mismatch. Requested type: [%s], actual type: [%s]",
              type.getType(), vector.getField().getType());
      throw new SchemaChangeRuntimeException(msg);
    }

    return new AddOrGetResult<>((T) vector, created);
  }

  public void copyFromSafe(int inIndex, int outIndex, FixedSizeListVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public void copyFrom(int fromIndex, int thisIndex, FixedSizeListVector from) {
    TransferPair pair = from.makeTransferPair(this);
    pair.copyValueSafe(fromIndex, thisIndex);
  }

  @Override
  public UnionVector promoteToUnion() {
    UnionVector vector = new UnionVector(name, allocator, null);
    this.vector.clear();
    this.vector = vector;
    this.reader = new UnionFixedSizeListReader(this);
    return vector;
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
    }
    final List<Object> vals = new JsonStringArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      vals.add(vector.getObject(index * listSize + i));
    }
    return vals;
  }

  /**
   * Returns whether the value at index null.
   */
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Returns non-zero when the value at index is non-null.
   */
  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  @Override
  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  /**
   * Returns the number of elements the validity buffer can represent with its
   * current capacity.
   */
  private int getValidityBufferValueCapacity() {
    return validityBuffer.capacity() * 8;
  }

  /**
   * Sets the value at index to null.  Reallocates if index is larger than capacity.
   */
  public void setNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /** Sets the value at index to not-null. Reallocates if index is larger than capacity. */
  public void setNotNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      reallocValidityBuffer();
    }
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
  }

  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getValidityBufferValueCapacity()) {
      reallocValidityBuffer();
    }
    vector.setValueCount(valueCount * listSize);
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
    return new TransferImpl((FixedSizeListVector) target);
  }

  private class TransferImpl implements TransferPair {

    FixedSizeListVector to;
    TransferPair dataPair;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      this(new FixedSizeListVector(name, allocator, fieldType, callBack));
    }

    public TransferImpl(FixedSizeListVector to) {
      this.to = to;
      to.addOrGetVector(vector.getField().getFieldType());
      dataPair = vector.makeTransferPair(to.vector);
    }

    @Override
    public void transfer() {
      to.clear();
      dataPair.transfer();
      to.validityBuffer = BaseValueVector.transferBuffer(validityBuffer, to.allocator);
      to.setValueCount(valueCount);
      clear();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      to.clear();
      to.allocateNew();
      for (int i = 0; i < length; i++) {
        copyValueSafe(startIndex + i, i);
      }
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      while (toIndex >= to.getValueCapacity()) {
        to.reAlloc();
      }
      BitVectorHelper.setValidityBit(to.validityBuffer, toIndex, isSet(fromIndex));
      int fromOffset = fromIndex * listSize;
      int toOffset = toIndex * listSize;
      for (int i = 0; i < listSize; i++) {
        dataPair.copyValueSafe(fromOffset + i, toOffset + i);
      }
    }
  }
}
