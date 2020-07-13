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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.LargeVarCharReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.LargeVarCharHolder;
import org.apache.arrow.vector.holders.NullableLargeVarCharHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;

/**
 * LargeVarCharVector implements a variable width vector of VARCHAR
 * values which could be NULL. A validity buffer (bit vector) is maintained
 * to track which elements in the vector are null.
 * <p>
 *   The offset width of this vector is 8, so the underlying buffer can be larger than 2GB.
 * </p>
 */
public final class LargeVarCharVector extends BaseLargeVariableWidthVector {
  private final FieldReader reader;

  /**
   * Instantiate a LargeVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public LargeVarCharVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(Types.MinorType.LARGEVARCHAR.getType()), allocator);
  }

  /**
   * Instantiate a LargeVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public LargeVarCharVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a LargeVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public LargeVarCharVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
    reader = new LargeVarCharReaderImpl(LargeVarCharVector.this);
  }

  /**
   * Get a reader that supports reading values from this vector.
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
  public Types.MinorType getMinorType() {
    return Types.MinorType.LARGEVARCHAR;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Get the variable length element at specified index as byte array.
   *
   * @param index   position of element to get
   * @return array of bytes for non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      return null;
    }
    final long startOffset = getStartOffset(index);
    final int dataLength =
        (int) (offsetBuffer.getLong((long) (index + 1) * OFFSET_WIDTH) - startOffset);
    final byte[] result = new byte[dataLength];
    valueBuffer.getBytes(startOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of element to get
   * @return Text object for non-null element, null otherwise
   */
  public Text getObject(int index) {
    byte[] b = get(index);
    if (b == null) {
      return null;
    } else {
      return new Text(b);
    }
  }

  /**
   * Get the variable length element at specified index and sets the state
   * in provided holder.
   *
   * @param index   position of element to get
   * @param holder  data holder to be populated by this function
   */
  public void get(int index, NullableLargeVarCharHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.start = getStartOffset(index);
    holder.end = offsetBuffer.getLong((long) (index + 1) * OFFSET_WIDTH);
    holder.buffer = valueBuffer;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, LargeVarCharHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final int dataLength = (int) (holder.end - holder.start);
    final long startOffset = getStartOffset(index);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, LargeVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, LargeVarCharHolder holder) {
    assert index >= 0;
    final int dataLength = (int) (holder.end - holder.start);
    handleSafe(index, dataLength);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final long startOffset = getStartOffset(index);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, NullableLargeVarCharHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    final long startOffset = getStartOffset(index);
    if (holder.isSet != 0) {
      final int dataLength = (int) (holder.end - holder.start);
      offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    } else {
      offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset);
    }
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, NullableLargeVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, NullableLargeVarCharHolder holder) {
    assert index >= 0;
    if (holder.isSet != 0) {
      final int dataLength = (int) (holder.end - holder.start);
      handleSafe(index, dataLength);
      fillHoles(index);
      final long startOffset = getStartOffset(index);
      offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + dataLength);
      valueBuffer.setBytes(startOffset, holder.buffer, holder.start, dataLength);
    } else {
      fillHoles(index + 1);
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied Text.
   *
   * @param index   position of the element to set
   * @param text    Text object with data
   */
  public void set(int index, Text text) {
    set(index, text.getBytes(), 0, text.getLength());
  }

  /**
   * Same as {@link #set(int, NullableLargeVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set.
   * @param text    Text object with data
   */
  public void setSafe(int index, Text text) {
    setSafe(index, text.getBytes(), 0, text.getLength());
  }

  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising of this and a target vector of
   * the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new LargeVarCharVector.TransferImpl(ref, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new LargeVarCharVector.TransferImpl((LargeVarCharVector) to);
  }

  private class TransferImpl implements TransferPair {
    LargeVarCharVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new LargeVarCharVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(LargeVarCharVector to) {
      this.to = to;
    }

    @Override
    public LargeVarCharVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, LargeVarCharVector.this);
    }
  }
}
