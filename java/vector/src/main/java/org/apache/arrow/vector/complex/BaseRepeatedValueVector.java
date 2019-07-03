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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DensityAwareVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;

import io.netty.buffer.ArrowBuf;

/** Base class for Vectors that contain repeated values. */
public abstract class BaseRepeatedValueVector extends BaseValueVector implements RepeatedValueVector {

  public static final FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public static final String DATA_VECTOR_NAME = "$data$";

  public static final byte OFFSET_WIDTH = 4;
  protected ArrowBuf offsetBuffer;
  protected FieldVector vector;
  protected final CallBack callBack;
  protected int valueCount;
  protected int offsetAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  private final String name;

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, FieldVector vector, CallBack callBack) {
    super(allocator);
    this.name = name;
    this.offsetBuffer = allocator.getEmpty();
    this.vector = Preconditions.checkNotNull(vector, "data vector cannot be null");
    this.callBack = callBack;
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
      allocateOffsetBuffer(offsetAllocationSizeInBytes);
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

  protected void allocateOffsetBuffer(final long size) {
    final int curSize = (int) size;
    offsetBuffer = allocator.buffer(curSize);
    offsetBuffer.readerIndex(0);
    offsetAllocationSizeInBytes = curSize;
    offsetBuffer.setZero(0, offsetBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    reallocOffsetBuffer();
    vector.reAlloc();
  }

  protected void reallocOffsetBuffer() {
    final int currentBufferCapacity = offsetBuffer.capacity();
    long baseSize = offsetAllocationSizeInBytes;

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
    newBuf.setBytes(0, offsetBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    offsetBuffer.getReferenceManager().release(1);
    offsetBuffer = newBuf;
    offsetAllocationSizeInBytes = (int) newAllocationSize;
  }

  @Override
  @Deprecated
  public UInt4Vector getOffsetVector() {
    throw new UnsupportedOperationException("There is no inner offset vector");
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsetAllocationSizeInBytes = (numRecords + 1) * OFFSET_WIDTH;
    if (vector instanceof BaseFixedWidthVector || vector instanceof BaseVariableWidthVector) {
      vector.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
    } else {
      vector.setInitialCapacity(numRecords);
    }
  }

  /**
   * Specialized version of setInitialCapacity() for ListVector. This is
   * used by some callers when they want to explicitly control and be
   * conservative about memory allocated for inner data vector. This is
   * very useful when we are working with memory constraints for a query
   * and have a fixed amount of memory reserved for the record batch. In
   * such cases, we are likely to face OOM or related problems when
   * we reserve memory for a record batch with value count x and
   * do setInitialCapacity(x) such that each vector allocates only
   * what is necessary and not the default amount but the multiplier
   * forces the memory requirement to go beyond what was needed.
   *
   * @param numRecords value count
   * @param density density of ListVector. Density is the average size of
   *                list per position in the List vector. For example, a
   *                density value of 10 implies each position in the list
   *                vector has a list of 10 values.
   *                A density value of 0.1 implies out of 10 positions in
   *                the list vector, 1 position has a list of size 1 and
   *                remaining positions are null (no lists) or empty lists.
   *                This helps in tightly controlling the memory we provision
   *                for inner data vector.
   */
  @Override
  public void setInitialCapacity(int numRecords, double density) {
    if ((numRecords * density) >= Integer.MAX_VALUE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }

    offsetAllocationSizeInBytes = (numRecords + 1) * OFFSET_WIDTH;

    int innerValueCapacity = Math.max((int)(numRecords * density), 1);

    if (vector instanceof DensityAwareVector) {
      ((DensityAwareVector)vector).setInitialCapacity(innerValueCapacity, density);
    } else {
      vector.setInitialCapacity(innerValueCapacity);
    }
  }

  @Override
  public int getValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity() - 1, 0);
    if (vector == DEFAULT_DATA_VECTOR) {
      return offsetValueCapacity;
    }
    return Math.min(vector.getValueCapacity(), offsetValueCapacity);
  }

  protected int getOffsetBufferValueCapacity() {
    return offsetBuffer.capacity() / OFFSET_WIDTH;
  }

  @Override
  public int getBufferSize() {
    if (getValueCount() == 0) {
      return 0;
    }
    return ((valueCount + 1) * OFFSET_WIDTH) + vector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return ((valueCount + 1) * OFFSET_WIDTH) + vector.getBufferSizeFor(valueCount);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>singleton(getDataVector()).iterator();
  }

  @Override
  public void clear() {
    offsetBuffer = releaseBuffer(offsetBuffer);
    vector.clear();
    valueCount = 0;
    super.clear();
  }

  @Override
  public void reset() {
    offsetBuffer.setZero(0, offsetBuffer.capacity());
    vector.reset();
    valueCount = 0;
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      List<ArrowBuf> list = new ArrayList<>();
      list.add(offsetBuffer);
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
    return vector == DEFAULT_DATA_VECTOR ? 0 : 1;
  }

  /**
   * Initialize the data vector (and execute callback) if it hasn't already been done,
   * returns the data vector.
   */
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    boolean created = false;
    if (vector instanceof ZeroVector) {
      vector = fieldType.createNewSingleVector(DATA_VECTOR_NAME, allocator, callBack);
      // returned vector must have the same field
      created = true;
      if (callBack != null &&
              // not a schema change if changing from ZeroVector to ZeroVector
              (fieldType.getType().getTypeID() != ArrowTypeID.Null)) {
        callBack.doWork();
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


  @Override
  public int getValueCount() {
    return valueCount;
  }

  /* returns the value count for inner data vector for this list vector */
  public int getInnerValueCount() {
    return vector.getValueCount();
  }


  /** Returns the value count for inner data vector at a particular index. */
  public int getInnerValueCountAt(int index) {
    return offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) -
            offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  /** Return if value at index is null (this implementation is always false). */
  public boolean isNull(int index) {
    return false;
  }

  /** Return if value at index is empty (this implementation is always false). */
  public boolean isEmpty(int index) {
    return false;
  }

  /** Starts a new repeated value. */
  public int startNewValue(int index) {
    while (index >= getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, offset);
    setValueCount(index + 1);
    return offset;
  }

  /** Preallocates the number of repeated values. */
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    final int childValueCount = valueCount == 0 ? 0 :
            offsetBuffer.getInt(valueCount * OFFSET_WIDTH);
    vector.setValueCount(childValueCount);
  }
}
