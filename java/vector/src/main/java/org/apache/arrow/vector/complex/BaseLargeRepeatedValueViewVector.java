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
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DensityAwareVector;
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

public abstract class BaseLargeRepeatedValueViewVector extends BaseValueVector
    implements RepeatedValueVector, FieldVector {
  public static final FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public static final String DATA_VECTOR_NAME = "$data$";

  public static final byte OFFSET_WIDTH = 8;
  public static final byte SIZE_WIDTH = 8;
  protected ArrowBuf offsetBuffer;
  protected ArrowBuf sizeBuffer;
  protected FieldVector vector;
  protected final CallBack repeatedCallBack;
  protected int valueCount;
  protected long offsetAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  protected long sizeAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * SIZE_WIDTH;
  private final String name;

  protected String defaultDataVectorName = DATA_VECTOR_NAME;

  protected BaseLargeRepeatedValueViewVector(
      String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseLargeRepeatedValueViewVector(
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
    offsetBuffer = allocateBuffers(offsetAllocationSizeInBytes);
    sizeBuffer = allocateBuffers(sizeAllocationSizeInBytes);
  }

  protected ArrowBuf allocateBuffers(final long size) {
    final int curSize = (int) size;
    ArrowBuf buffer = allocator.buffer(curSize);
    buffer.readerIndex(0);
    buffer.setZero(0, buffer.capacity());
    return buffer;
  }

  @Override
  public void reAlloc() {
    reallocateBuffers();
    vector.reAlloc();
  }

  protected void reallocateBuffers() {
    reallocOffsetBuffer();
    reallocSizeBuffer();
  }

  private void reallocOffsetBuffer() {
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

  private void reallocSizeBuffer() {
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
    offsetAllocationSizeInBytes = (long) (numRecords) * OFFSET_WIDTH;
    sizeAllocationSizeInBytes = (long) (numRecords) * SIZE_WIDTH;
    if (vector instanceof BaseFixedWidthVector || vector instanceof BaseVariableWidthVector) {
      vector.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    } else {
      vector.setInitialCapacity(numRecords);
    }
  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {
    if ((numRecords * density) >= Integer.MAX_VALUE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }

    offsetAllocationSizeInBytes = (long) numRecords * OFFSET_WIDTH;
    sizeAllocationSizeInBytes = (long) numRecords * SIZE_WIDTH;

    int innerValueCapacity = Math.max((int) (numRecords * density), 1);

    if (vector instanceof DensityAwareVector) {
      ((DensityAwareVector) vector).setInitialCapacity(innerValueCapacity, density);
    } else {
      vector.setInitialCapacity(innerValueCapacity);
    }
  }

  /**
   * Specialized version of setInitialTotalCapacity() for LargeListViewVector. This is used by some
   * callers when they want to explicitly control and be conservative about memory allocated for
   * inner data vector. This is very useful when we are working with memory constraints for a query
   * and have a fixed amount of memory reserved for the record batch. In such cases, we are likely
   * to face OOM or related problems when we reserve memory for a record batch with value count x
   * and do setInitialCapacity(x) such that each vector allocates only what is necessary and not the
   * default amount, but the multiplier forces the memory requirement to go beyond what was needed.
   *
   * @param numRecords value count
   * @param totalNumberOfElements the total number of elements to allow for in this vector across
   *     all records.
   */
  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {
    offsetAllocationSizeInBytes = (long) numRecords * OFFSET_WIDTH;
    sizeAllocationSizeInBytes = (long) numRecords * SIZE_WIDTH;
    vector.setInitialCapacity(totalNumberOfElements);
  }

  @Override
  public int getValueCapacity() {
    throw new UnsupportedOperationException(
        "Get value capacity is not supported in RepeatedValueVector");
  }

  protected int getOffsetBufferValueCapacity() {
    return checkedCastToInt(offsetBuffer.capacity() / OFFSET_WIDTH);
  }

  protected int getSizeBufferValueCapacity() {
    return capAtMaxInt(sizeBuffer.capacity() / SIZE_WIDTH);
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0) {
      return 0;
    }
    return (valueCount * OFFSET_WIDTH) + (valueCount * SIZE_WIDTH) + vector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    int innerVectorValueCount = 0;

    for (int i = 0; i < valueCount; i++) {
      innerVectorValueCount += sizeBuffer.getInt(i * SIZE_WIDTH);
    }

    return (valueCount * OFFSET_WIDTH)
        + (valueCount * SIZE_WIDTH)
        + vector.getBufferSizeFor(checkedCastToInt(innerVectorValueCount));
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>singleton(getDataVector()).iterator();
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
    offsetBuffer.setZero(0, offsetBuffer.capacity());
    sizeBuffer.setZero(0, sizeBuffer.capacity());
    vector.reset();
    valueCount = 0;
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
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getOffsetBufferValueCapacity()) {
      reallocateBuffers();
    }
    final int childValueCount = valueCount == 0 ? 0 : getLengthOfChildVector();
    vector.setValueCount(childValueCount);
  }

  protected int getLengthOfChildVector() {
    int maxOffsetSizeSum = offsetBuffer.getInt(0) + sizeBuffer.getInt(0);
    int minOffset = offsetBuffer.getInt(0);
    for (int i = 0; i < valueCount; i++) {
      int currentOffset = offsetBuffer.getInt((long) i * OFFSET_WIDTH);
      int currentSize = sizeBuffer.getInt((long) i * SIZE_WIDTH);
      int currentSum = currentOffset + currentSize;

      maxOffsetSizeSum = Math.max(maxOffsetSizeSum, currentSum);
      minOffset = Math.min(minOffset, currentOffset);
    }

    return maxOffsetSizeSum - minOffset;
  }

  protected int getLengthOfChildVectorByIndex(int index) {
    int maxOffsetSizeSum = offsetBuffer.getInt(0) + sizeBuffer.getInt(0);
    int minOffset = offsetBuffer.getInt(0);
    for (int i = 0; i < index; i++) {
      int currentOffset = offsetBuffer.getInt((long) i * OFFSET_WIDTH);
      int currentSize = sizeBuffer.getInt((long) i * SIZE_WIDTH);
      int currentSum = currentOffset + currentSize;

      maxOffsetSizeSum = Math.max(maxOffsetSizeSum, currentSum);
      minOffset = Math.min(minOffset, currentOffset);
    }

    return maxOffsetSizeSum - minOffset;
  }

  /**
   * Initialize the data vector (and execute callback) if it hasn't already been done, returns the
   * data vector.
   */
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    boolean created = false;
    if (vector instanceof NullVector) {
      vector = fieldType.createNewSingleVector(defaultDataVectorName, allocator, repeatedCallBack);
      // returned vector must have the same field
      created = true;
      if (repeatedCallBack != null
          &&
          // not a schema change if changing from ZeroVector to ZeroVector
          (fieldType.getType().getTypeID() != ArrowType.ArrowTypeID.Null)) {
        repeatedCallBack.doWork();
      }
    }

    if (vector.getField().getType().getTypeID() != fieldType.getType().getTypeID()) {
      final String msg =
          String.format(
              "Inner vector type mismatch. Requested type: [%s], actual type: [%s]",
              fieldType.getType().getTypeID(), vector.getField().getType().getTypeID());
      throw new SchemaChangeRuntimeException(msg);
    }

    return new AddOrGetResult<>((T) vector, created);
  }

  protected void replaceDataVector(FieldVector v) {
    vector.clear();
    vector = v;
  }

  public abstract boolean isEmpty(int index);

  /**
   * Start a new value at the given index.
   *
   * @param index the index to start the new value at
   * @return the offset in the data vector where the new value starts
   */
  public int startNewValue(int index) {
    while (index >= getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    while (index >= getSizeBufferValueCapacity()) {
      reallocSizeBuffer();
    }

    if (index > 0) {
      final int prevOffset = getLengthOfChildVectorByIndex(index);
      offsetBuffer.setInt((long) index * OFFSET_WIDTH, prevOffset);
    }

    setValueCount(index + 1);
    return offsetBuffer.getInt((long) index * OFFSET_WIDTH);
  }

  @Override
  @Deprecated
  public UInt4Vector getOffsetVector() {
    throw new UnsupportedOperationException("There is no inner offset vector");
  }
}
