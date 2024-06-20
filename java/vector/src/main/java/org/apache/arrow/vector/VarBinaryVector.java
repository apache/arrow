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
package org.apache.arrow.vector;

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.vector.complex.impl.VarBinaryReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * VarBinaryVector implements a variable width vector of binary values which could be NULL. A
 * validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public final class VarBinaryVector extends BaseVariableWidthVector
    implements ValueIterableVector<byte[]> {

  /**
   * Instantiate a VarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public VarBinaryVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.VARBINARY.getType()), allocator);
  }

  /**
   * Instantiate a VarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public VarBinaryVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a VarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public VarBinaryVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new VarBinaryReaderImpl(VarBinaryVector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VARBINARY;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Get the variable length element at specified index as byte array.
   *
   * @param index position of element to get
   * @return array of bytes for non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }
    final int startOffset = getStartOffset(index);
    final int dataLength = getEndOffset(index) - startOffset;
    final byte[] result = new byte[dataLength];
    valueBuffer.getBytes(startOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Read the value at the given position to the given output buffer. The caller is responsible for
   * checking for nullity first.
   *
   * @param index position of element.
   * @param buffer the buffer to write into.
   */
  @Override
  public void read(int index, ReusableBuffer<?> buffer) {
    final int startOffset = getStartOffset(index);
    final int dataLength = getEndOffset(index) - startOffset;
    buffer.set(valueBuffer, startOffset, dataLength);
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index position of element to get
   * @return byte array for non-null element, null otherwise
   */
  @Override
  public byte[] getObject(int index) {
    return get(index);
  }

  /**
   * Get the variable length element at specified index and sets the state in provided holder.
   *
   * @param index position of element to get
   * @param holder data holder to be populated by this function
   */
  public void get(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.start = getStartOffset(index);
    holder.end = getEndOffset(index);
    holder.buffer = valueBuffer;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, VarBinaryHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final int dataLength = holder.end - holder.start;
    final int startOffset = getStartOffset(index);
    offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, VarBinaryHolder)} except that it handles the case where index and
   * length of new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, VarBinaryHolder holder) {
    assert index >= 0;
    final int dataLength = holder.end - holder.start;
    handleSafe(index, dataLength);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final int startOffset = getStartOffset(index);
    offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    final int startOffset = getStartOffset(index);
    if (holder.isSet != 0) {
      final int dataLength = holder.end - holder.start;
      offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    } else {
      offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset);
    }
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, NullableVarBinaryHolder)} except that it handles the case where index
   * and length of new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, NullableVarBinaryHolder holder) {
    assert index >= 0;
    if (holder.isSet != 0) {
      final int dataLength = holder.end - holder.start;
      handleSafe(index, dataLength);
      fillHoles(index);
      final int startOffset = getStartOffset(index);
      offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    } else {
      fillEmpties(index + 1);
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    lastSet = index;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |                      vector transfer                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising of this and a target vector of the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
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

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new VarBinaryVector(field, allocator);
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
