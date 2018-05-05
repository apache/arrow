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

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.VarBinaryReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * VarBinaryVector implements a variable width vector of binary
 * values which could be NULL. A validity buffer (bit vector) is maintained
 * to track which elements in the vector are null.
 */
public class VarBinaryVector extends BaseVariableWidthVector {
  private final FieldReader reader;

  /**
   * Instantiate a VarBinaryVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public VarBinaryVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.VARBINARY.getType()), allocator);
  }

  /**
   * Instantiate a VarBinaryVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public VarBinaryVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator, fieldType);
    reader = new VarBinaryReaderImpl(VarBinaryVector.this);
  }

  /**
   * Get a reader that supports reading values from this vector
   * @return Field Reader for this vector
   */
  @Override
  public FieldReader getReader() {
    return reader;
  }

  /**
   * Get minor type for this vector. The vector holds values belonging
   * to a particular type.
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VARBINARY;
  }


  /******************************************************************
   *                                                                *
   *          vector value getter methods                           *
   *                                                                *
   ******************************************************************/


  /**
   * Get the variable length element at specified index as byte array.
   *
   * @param index   position of element to get
   * @return array of bytes for non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    final int startOffset = getstartOffset(index);
    final int dataLength =
            offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
    final byte[] result = new byte[dataLength];
    valueBuffer.getBytes(startOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of element to get
   * @return byte array for non-null element, null otherwise
   */
  public byte[] getObject(int index) {
    byte[] b;
    try {
      b = get(index);
    } catch (IllegalStateException e) {
      return null;
    }
    return b;
  }

  /**
   * Get the variable length element at specified index and sets the state
   * in provided holder.
   *
   * @param index   position of element to get
   * @param holder  data holder to be populated by this function
   */
  public void get(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.start = getstartOffset(index);
    holder.end = offsetBuffer.getInt((index + 1) * OFFSET_WIDTH);
    holder.buffer = valueBuffer;
  }


  /******************************************************************
   *                                                                *
   *          vector value setter methods                           *
   *                                                                *
   ******************************************************************/


  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, VarBinaryVector from) {
    final int start = from.offsetBuffer.getInt(fromIndex * OFFSET_WIDTH);
    final int end = from.offsetBuffer.getInt((fromIndex + 1) * OFFSET_WIDTH);
    final int length = end - start;
    fillHoles(thisIndex);
    BitVectorHelper.setValidityBit(this.validityBuffer, thisIndex, from.isSet(fromIndex));
    final int copyStart = offsetBuffer.getInt(thisIndex * OFFSET_WIDTH);
    from.valueBuffer.getBytes(start, this.valueBuffer, copyStart, length);
    offsetBuffer.setInt((thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
    lastSet = thisIndex;
  }

  /**
   * Same as {@link #copyFrom(int, int, VarBinaryVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, VarBinaryVector from) {
    final int start = from.offsetBuffer.getInt(fromIndex * OFFSET_WIDTH);
    final int end = from.offsetBuffer.getInt((fromIndex + 1) * OFFSET_WIDTH);
    final int length = end - start;
    handleSafe(thisIndex, length);
    fillHoles(thisIndex);
    BitVectorHelper.setValidityBit(this.validityBuffer, thisIndex, from.isSet(fromIndex));
    final int copyStart = offsetBuffer.getInt(thisIndex * OFFSET_WIDTH);
    from.valueBuffer.getBytes(start, this.valueBuffer, copyStart, length);
    offsetBuffer.setInt((thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
    lastSet = thisIndex;
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, VarBinaryHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int dataLength = holder.end - holder.start;
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, VarBinaryHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, VarBinaryHolder holder) {
    assert index >= 0;
    final int dataLength = holder.end - holder.start;
    fillEmpties(index);
    handleSafe(index, dataLength);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    final int dataLength = holder.end - holder.start;
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, NullableVarBinaryHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    final int dataLength = holder.end - holder.start;
    fillEmpties(index);
    handleSafe(index, dataLength);
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }


  /******************************************************************
   *                                                                *
   *                      vector transfer                           *
   *                                                                *
   ******************************************************************/

  /**
   * Construct a TransferPair comprising of this and and a target vector of
   * the same type.
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((VarBinaryVector) to);
  }

  private class TransferImpl implements TransferPair {
    VarBinaryVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new VarBinaryVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(VarBinaryVector to) {
      this.to = to;
    }

    @Override
    public VarBinaryVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, VarBinaryVector.this);
    }
  }
}
