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

import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;

public abstract class BaseRepeatedValueVector extends BaseValueVector implements RepeatedValueVector {

  public final static FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public final static String DATA_VECTOR_NAME = "$data$";

  public final static byte OFFSET_WIDTH = 4;
  protected ArrowBuf offsetBuffer;
  protected FieldVector vector;
  protected final CallBack callBack;
  protected int valueCount;
  protected int offsetAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, FieldVector vector, CallBack callBack) {
    super(name, allocator);
    this.offsetBuffer = allocator.getEmpty();
    this.vector = Preconditions.checkNotNull(vector, "data vector cannot be null");
    this.callBack = callBack;
    this.valueCount = 0;
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

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setBytes(0, offsetBuffer, 0, currentBufferCapacity);
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    offsetBuffer.release(1);
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

  @Override
  public int getValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity() - 1, 0);
    if (vector == DEFAULT_DATA_VECTOR) {
      return offsetValueCapacity;
    }
    return Math.min(vector.getValueCapacity(), offsetValueCapacity);
  }

  protected int getOffsetBufferValueCapacity() {
    return (int) ((offsetBuffer.capacity() * 1.0) / OFFSET_WIDTH);
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
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      buffers = ObjectArrays.concat(new ArrowBuf[]{offsetBuffer}, vector.getBuffers(false),
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
   * @return 1 if inner vector is explicitly set via #addOrGetVector else 0
   */
  public int size() {
    return vector == DEFAULT_DATA_VECTOR ? 0 : 1;
  }

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


  /* returns the value count for inner data vector at a particular index */
  public int getInnerValueCountAt(int index) {
    return offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) -
            offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  public boolean isNull(int index) {
    return false;
  }

  public boolean isEmpty(int index) {
    return false;
  }

  public int startNewValue(int index) {
    while (index >= getOffsetBufferValueCapacity()) {
      reallocOffsetBuffer();
    }
    int offset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, offset);
    setValueCount(index + 1);
    return offset;
  }

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
