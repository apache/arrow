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

import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;

import java.util.Iterator;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;

public abstract class BaseRepeatedValueViewVector extends BaseValueVector
    implements RepeatedValueVector, BaseListVector {

  public static final FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public static final String DATA_VECTOR_NAME = "$data$";

  public static final byte OFFSET_WIDTH = 4;
  public static final byte SIZE_WIDTH = 4;
  protected ArrowBuf offsetBuffer;
  protected ArrowBuf sizeBuffer;
  protected FieldVector vector;
  protected final CallBack repeatedCallBack;
  protected int valueCount;
  protected long offsetAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  protected long sizeAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * SIZE_WIDTH;
  private final String name;

  protected String defaultDataVectorName = DATA_VECTOR_NAME;

  protected BaseRepeatedValueViewVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseRepeatedValueViewVector(
      String name, BufferAllocator allocator, FieldVector vector, CallBack callBack) {
    super(allocator);
    this.name = name;
    this.offsetBuffer = allocator.getEmpty();
    this.sizeBuffer = allocator.getEmpty();
    this.vector = Preconditions.checkNotNull(vector, "data vector cannot be null");
    this.repeatedCallBack = callBack;
    this.valueCount = 0;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean allocateNewSafe() {
    boolean dataAlloc = false;
    try {
      allocateBuffers();
      dataAlloc = vector.allocateNewSafe();
    } catch (Exception e) {
      e.printStackTrace();
      clear();
      return false;
    } finally {
      if (!dataAlloc) {
        clear();
      }
    }
    return dataAlloc;
  }

  private void allocateBuffers() {
    offsetBuffer = allocateOffsetBuffer(offsetAllocationSizeInBytes);
    sizeBuffer = allocateSizeBuffer(sizeAllocationSizeInBytes);
  }

  private ArrowBuf allocateOffsetBuffer(final long size) {
    final int curSize = (int) size;
    ArrowBuf offsetBuffer = allocator.buffer(curSize);
    offsetBuffer.readerIndex(0);
    offsetAllocationSizeInBytes = curSize;
    offsetBuffer.setZero(0, offsetBuffer.capacity());
    return offsetBuffer;
  }

  private ArrowBuf allocateSizeBuffer(final long size) {
    final int curSize = (int) size;
    ArrowBuf sizeBuffer = allocator.buffer(curSize);
    sizeBuffer.readerIndex(0);
    sizeAllocationSizeInBytes = curSize;
    sizeBuffer.setZero(0, sizeBuffer.capacity());
    return sizeBuffer;
  }

  @Override
  public void reAlloc() {
    reallocOffsetBuffer();
    reallocSizeBuffer();
    vector.reAlloc();
  }

  protected void reallocOffsetBuffer() {
    final long currentBufferCapacity = offsetBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (offsetAllocationSizeInBytes > 0) {
        newAllocationSize = offsetAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH * 2;
      }
    }

    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    newAllocationSize = Math.min(newAllocationSize, (long) OFFSET_WIDTH * Integer.MAX_VALUE);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE || newAllocationSize <= offsetBuffer.capacity()) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, offsetBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    offsetBuffer.getReferenceManager().release(1);
    offsetBuffer = newBuf;
    offsetAllocationSizeInBytes = newAllocationSize;
  }

  protected void reallocSizeBuffer() {
    final long currentBufferCapacity = sizeBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (sizeAllocationSizeInBytes > 0) {
        newAllocationSize = sizeAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_VALUE_ALLOCATION * SIZE_WIDTH * 2;
      }
    }

    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    newAllocationSize = Math.min(newAllocationSize, (long) SIZE_WIDTH * Integer.MAX_VALUE);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE || newAllocationSize <= sizeBuffer.capacity()) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, sizeBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    sizeBuffer.getReferenceManager().release(1);
    sizeBuffer = newBuf;
    sizeAllocationSizeInBytes = newAllocationSize;
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsetAllocationSizeInBytes = (numRecords) * OFFSET_WIDTH;
    sizeAllocationSizeInBytes = (numRecords) * SIZE_WIDTH;
    if (vector instanceof BaseFixedWidthVector || vector instanceof BaseVariableWidthVector) {
      vector.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    } else {
      vector.setInitialCapacity(numRecords);
    }
  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {

  }

  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {

  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  protected int getOffsetBufferValueCapacity() {
    return capAtMaxInt(offsetBuffer.capacity() / OFFSET_WIDTH);
  }

  protected int getSizeBufferValueCapacity() {
    return capAtMaxInt(sizeBuffer.capacity() / SIZE_WIDTH);
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return 0;
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return null;
  }

  @Override
  public void clear() {
    offsetBuffer = releaseBuffer(offsetBuffer);
    sizeBuffer = releaseBuffer(sizeBuffer);
    vector.clear();
    valueCount = 0;
    super.clear();
  }

  @Override
  public void reset() {

  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  @Override
  public boolean isNull(int index) {
    return false;
  }

  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    while (valueCount > getSizeBufferValueCapacity()) {
      reallocSizeBuffer();
    }
    final int childValueCount = valueCount == 0 ? 0 : getLengthOfChildVector();
    vector.setValueCount(childValueCount);
  }

  private int getLengthOfChildVector() {
    int length = 0;
    for (int i = 0; i <= valueCount; i++) {
      length += sizeBuffer.getInt(i * SIZE_WIDTH);
    }
    return length;
  }

  /**
   * Initialize the data vector (and execute callback) if it hasn't already been done,
   * returns the data vector.
   */
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    boolean created = false;
    if (vector instanceof NullVector) {
      vector = fieldType.createNewSingleVector(defaultDataVectorName, allocator, repeatedCallBack);
      // returned vector must have the same field
      created = true;
      if (repeatedCallBack != null &&
              // not a schema change if changing from ZeroVector to ZeroVector
              (fieldType.getType().getTypeID() != ArrowType.ArrowTypeID.Null)) {
        repeatedCallBack.doWork();
      }
    }

    if (vector.getField().getType().getTypeID() != fieldType.getType().getTypeID()) {
      final String msg = String.format("Inner vector type mismatch. Requested type: [%s], actual type: [%s]",
              fieldType.getType().getTypeID(), vector.getField().getType().getTypeID());
      throw new SchemaChangeRuntimeException(msg);
    }

    return new AddOrGetResult<>((T) vector, created);
  }

  protected void replaceDataVector(FieldVector v) {
    vector.clear();
    vector = v;
  }

  public boolean isEmpty(int index) {
    return false;
  }

  public int startNewValue(int index) {
    return 0;
  }

  @Override
  @Deprecated
  public UInt4Vector getOffsetVector() {
    throw new UnsupportedOperationException("There is no inner offset vector");
  }
}
