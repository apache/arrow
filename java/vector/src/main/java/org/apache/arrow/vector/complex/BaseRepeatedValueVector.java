/**
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
 */
package org.apache.arrow.vector.complex;

import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ObjectArrays;

import io.netty.buffer.ArrowBuf;

public abstract class BaseRepeatedValueVector extends BaseValueVector implements RepeatedValueVector {

  public final static FieldVector DEFAULT_DATA_VECTOR = ZeroVector.INSTANCE;
  public final static String OFFSETS_VECTOR_NAME = "$offsets$";
  public final static String DATA_VECTOR_NAME = "$data$";

  protected final UInt4Vector offsets;
  protected FieldVector vector;
  protected final CallBack callBack;

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, CallBack callBack) {
    this(name, allocator, DEFAULT_DATA_VECTOR, callBack);
  }

  protected BaseRepeatedValueVector(String name, BufferAllocator allocator, FieldVector vector, CallBack callBack) {
    super(name, allocator);
    this.offsets = new UInt4Vector(OFFSETS_VECTOR_NAME, allocator);
    this.vector = Preconditions.checkNotNull(vector, "data vector cannot be null");
    this.callBack = callBack;
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
      if (!offsets.allocateNewSafe()) {
        return false;
      }
      success = vector.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    offsets.zeroVector();
    return success;
  }

  @Override
  public void reAlloc() {
    offsets.reAlloc();
    vector.reAlloc();
  }

  @Override
  public UInt4Vector getOffsetVector() {
    return offsets;
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    offsets.setInitialCapacity(numRecords + 1);
    vector.setInitialCapacity(numRecords * RepeatedValueVector.DEFAULT_REPEAT_PER_RECORD);
  }

  @Override
  public int getValueCapacity() {
    final int offsetValueCapacity = Math.max(offsets.getValueCapacity() - 1, 0);
    if (vector == DEFAULT_DATA_VECTOR) {
      return offsetValueCapacity;
    }
    return Math.min(vector.getValueCapacity(), offsetValueCapacity);
  }

  @Override
  public int getBufferSize() {
    if (getAccessor().getValueCount() == 0) {
      return 0;
    }
    return offsets.getBufferSize() + vector.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    return offsets.getBufferSizeFor(valueCount + 1) + vector.getBufferSizeFor(valueCount);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>singleton(getDataVector()).iterator();
  }

  @Override
  public void clear() {
    offsets.clear();
    vector.clear();
    super.clear();
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers = ObjectArrays.concat(offsets.getBuffers(false), vector.getBuffers(false), ArrowBuf.class);
    if (clear) {
      for (ArrowBuf buffer:buffers) {
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
    return vector == DEFAULT_DATA_VECTOR ? 0:1;
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

    return new AddOrGetResult<>((T)vector, created);
  }

  protected void replaceDataVector(FieldVector v) {
    vector.clear();
    vector = v;
  }

  public abstract class BaseRepeatedAccessor extends BaseValueVector.BaseAccessor implements RepeatedAccessor {

    @Override
    public int getValueCount() {
      return Math.max(offsets.getAccessor().getValueCount() - 1, 0);
    }

    @Override
    public int getInnerValueCount() {
      return vector.getAccessor().getValueCount();
    }

    @Override
    public int getInnerValueCountAt(int index) {
      return offsets.getAccessor().get(index+1) - offsets.getAccessor().get(index);
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public boolean isEmpty(int index) {
      return false;
    }
  }

  public abstract class BaseRepeatedMutator extends BaseValueVector.BaseMutator implements RepeatedMutator {

    @Override
    public int startNewValue(int index) {
      while (offsets.getValueCapacity() <= index) {
        offsets.reAlloc();
      }
      int offset = offsets.getAccessor().get(index);
      offsets.getMutator().setSafe(index+1, offset);
      setValueCount(index+1);
      return offset;
    }

    @Override
    public void setValueCount(int valueCount) {
      // TODO: populate offset end points
      offsets.getMutator().setValueCount(valueCount == 0 ? 0 : valueCount+1);
      final int childValueCount = valueCount == 0 ? 0 : offsets.getAccessor().get(valueCount);
      vector.getMutator().setValueCount(childValueCount);
    }
  }

}
