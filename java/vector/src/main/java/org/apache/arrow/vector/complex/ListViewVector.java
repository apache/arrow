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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;

/**
 * A list vector contains lists of a specific type of elements.  Its structure contains 4 elements.
 * <ol>
 * <li>A validity buffer.</li>
 * <li> A child data vector that contains the elements of lists. </li>
 * <li> An offset buffer, stores the starting index of each list. </li>
 * <li> A size buffer, stored explicitly each list lengths. </li>
 * </ol>
 * The latter two are managed by its superclass.
 */
public class ListViewVector extends ListVector {
  protected ArrowBuf sizeBuffer;
  protected long sizeAllocationSizeInBytes = INITIAL_VALUE_ALLOCATION * OFFSET_WIDTH;
  private int valueCountOfDataVector = 0;

  /**
   * Constructs a new instance.
   *
   * @param name      The name of the instance.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack  A schema change callback.
   */
  public ListViewVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, fieldType, callBack);
    this.sizeBuffer = allocator.getEmpty();
  }

  /**
   * Constructs a new instance.
   *
   * @param field     The field materialized by this vector.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param callBack  A schema change callback.
   */
  public ListViewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field, allocator, callBack);
    this.sizeBuffer = allocator.getEmpty();
  }

  public static ListViewVector empty(String name, BufferAllocator allocator) {
    return new ListViewVector(name, allocator, FieldType.nullable(ArrowType.List.INSTANCE), null);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating memory");
    }
  }

  /**
   * Allocate memory for the vector. We internally use a default value count
   * of 4096 to allocate memory for at least these many elements in the
   * vector.
   *
   * @return false if memory allocation fails, true otherwise.
   */
  @Override
  public boolean allocateNewSafe() {
    boolean success = false;
    try {
      clear();
      success = super.allocateNewSafe();
      allocateSizeBuffer(sizeAllocationSizeInBytes);
    } finally {
      if (!success) {
        clear();
      }
    }
    return success;
  }

  @Override
  public void clear() {
    super.clear();
    sizeBuffer = releaseBuffer(sizeBuffer);
  }

  /**
   * Update index values for offset and size buffer.
   *
   * @param index Current index position.
   * @param size The number of items added.
   */
  public void endValue(int index, int size) {
    sizeBuffer.setInt(index * OFFSET_WIDTH, size);
    valueCountOfDataVector = valueCountOfDataVector + size;
  }

  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    if (valueCount > 0) {
      while (valueCount > getValueCapacity()) {
        reallocValidityAndOffsetBuffers();
      }
    }
    vector.setValueCount(valueCountOfDataVector);
  }

  /**
   * Start a new value in the list vector.
   *
   * @param index index of the value to start
   * @param offset encode the start position of each slot in the child array
   */
  public int startNewValue(int index, int offset) {
    while (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    offsetBuffer.setInt(index * OFFSET_WIDTH, offset);
    BitVectorHelper.setBit(validityBuffer, index);
    return offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  /**
   * Get the element in the list vector at a particular index.
   * @param index position of the element
   * @return Object at given position
   */
  @Override
  public List<?> getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    }
    final List<Object> vals = new JsonStringArrayList<>();
    final int end = sizeBuffer.getInt(index * OFFSET_WIDTH);
    final ValueVector vv = getDataVector();
    int delta = 0;
    if (index > 0) {
      for (int i = index; i > 0; i--) {
        delta += sizeBuffer.getInt((i - 1) * OFFSET_WIDTH);
      }
    }
    for (int i = 0; i < end; i++) {
      vals.add(i, vv.getObject(i + delta));
    }

    return vals;
  }

  protected void allocateSizeBuffer(final long size) {
    final int curSize = (int) size;
    sizeBuffer = allocator.buffer(curSize);
    sizeBuffer.readerIndex(0);
    sizeAllocationSizeInBytes = curSize;
    sizeBuffer.setZero(0, sizeBuffer.capacity());
  }
}
