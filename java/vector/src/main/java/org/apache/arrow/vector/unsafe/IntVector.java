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

package org.apache.arrow.vector.unsafe;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.IntReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.IntHolder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Unsafe implementation of org.apache.arrow.vector.IntVector.
 * Compared with org.apache.arrow.vector.IntVector, it avoids checks and directly operates on the off-heap memory,
 * so it provides much better performance.
 */
public class IntVector extends org.apache.arrow.vector.IntVector {

  /**
   * The number of bits to shift to multiply/divide by the type width.
   */
  public static final byte TYPE_LOG2_WIDTH = 2;

  /**
   * Instantiate a IntVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name      name of the vector
   * @param allocator allocator for memory management.
   */
  public IntVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  /**
   * Instantiate a IntVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name      name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public IntVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, fieldType, allocator);
  }

  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Get the element at the given index from the vector.
   *
   * @param index position of element
   * @return element at given index
   */
  public int get(int index) throws IllegalStateException {
    return PlatformDependent.getInt(valueBuffer.memoryAddress() + (index << TYPE_LOG2_WIDTH));
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableIntHolder holder) {
    if (UnsafeBitVectorHelper.isValidityBitSet(validityBuffer, index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = PlatformDependent.getInt(valueBuffer.memoryAddress() + (index << TYPE_LOG2_WIDTH));
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  public Integer getObject(int index) {
    if (UnsafeBitVectorHelper.isValidityBitSet(validityBuffer, index) == 0) {
      return null;
    } else {
      return get(index);
    }
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from      source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, org.apache.arrow.vector.IntVector from) {
    UnsafeBitVectorHelper.setValidityBit(validityBuffer, thisIndex,
            UnsafeBitVectorHelper.isValidityBitSet(from.getValidityBuffer(), fromIndex));
    final int value = PlatformDependent.getInt(from.getDataBufferAddress() + (thisIndex << TYPE_LOG2_WIDTH));
    this.set(thisIndex, value);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  private void setValue(int index, int value) {
    PlatformDependent.putInt(valueBuffer.memoryAddress() + (index << TYPE_LOG2_WIDTH), value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, int value) {
    UnsafeBitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index  position of element
   * @param holder nullable data holder for value of element
   */
  public void set(int index, NullableIntHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      UnsafeBitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
    } else {
      UnsafeBitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index  position of element
   * @param holder data holder for value of element
   */
  public void set(int index, IntHolder holder) {
    UnsafeBitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index position of element
   */
  public void setNull(int index) {
    handleSafe(index);
    // not really needed to set the bit to 0 as long as
    // the buffer always starts from 0.
    UnsafeBitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void set(int index, int isSet, int value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      UnsafeBitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index  position of element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  @Override
  public int isSet(int index) {
    return UnsafeBitVectorHelper.isValidityBitSet(validityBuffer, index);
  }

  /**
   * Given a data buffer, get the value stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return value stored at the index.
   */
  public static int get(final ArrowBuf buffer, final int index) {
    return PlatformDependent.getInt(buffer.memoryAddress() + (index << TYPE_LOG2_WIDTH));
  }
}
