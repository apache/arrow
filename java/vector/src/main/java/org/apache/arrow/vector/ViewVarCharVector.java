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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.vector.complex.impl.ViewVarCharReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableViewVarCharHolder;
import org.apache.arrow.vector.holders.ViewVarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.validate.ValidateUtil;

/**
 * VarCharVector implements a variable width vector of VARCHAR
 * values which could be NULL. A validity buffer (bit vector) is maintained
 * to track which elements in the vector are null.
 */
public final class ViewVarCharVector extends BaseVariableWidthViewVector {

  /**
   * Instantiate a VarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator);
  }

  /**
   * Instantiate a VarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a VarCharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new ViewVarCharReaderImpl(ViewVarCharVector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging
   * to a particular type.
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VIEWVARCHAR;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  private byte[] getData(int index, int dataLength) {
    byte[] result = new byte[dataLength];
    if (dataLength > INLINE_SIZE) {
      // data is in the inline buffer
      // get buffer index
      final int bufferIndex =
          valueBuffer.getInt(((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      // get data offset
      final int dataOffset =
          valueBuffer.getInt(
              ((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      dataBuffers.get(bufferIndex).getBytes(dataOffset, result, 0, dataLength);
    } else {
      // data is in the value buffer
      valueBuffer.getBytes(
          (long) index * VIEW_BUFFER_SIZE + BUF_INDEX_WIDTH, result, 0, dataLength);
    }
    return result;
  }

  /**
   * Get the variable length element at specified index as byte array.
   *
   * @param index   position of element to get
   * @return array of bytes for non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }
    final int startOffset = getStartOffset(index);
    final int dataLength = getEndOffset(index) - startOffset;
    return getData(index, dataLength);
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of element to get
   * @return Text object for non-null element, null otherwise
   */
  @Override
  public Text getObject(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }

    final Text result = new Text();
    read(index, result);
    return result;
  }

  /**
   * Read the value at the given position to the given output buffer.
   * The caller is responsible for checking for nullity first.
   *
   * @param index position of element.
   * @param buffer the buffer to write into.
   */
  @Override
  public void read(int index, ReusableBuffer<?> buffer) {
    final int startOffset = getStartOffset(index);
    final int dataLength = getEndOffset(index) - startOffset;
    byte[] data = getData(index, dataLength);
    buffer.set(data, 0, data.length);
  }

  public static class HolderCallback {
    /**
     * Get the variable length element at specified index and sets the state.
     * @param index position of element.
     * @param dataLength length of the buffer.
     * @param input input buffer.
     * @param dataBufs list of data buffers.
     * @param output output buffer.
    */
    public void getData(int index, int dataLength, ArrowBuf input, List<ArrowBuf> dataBufs, ArrowBuf output) {
      if (dataLength > INLINE_SIZE) {
        // data is in the inline buffer
        // get buffer index
        final int bufferIndex =
            input.getInt(((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
        // get data offset
        final int dataOffset =
            input.getInt(
                ((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
        dataBufs.get(bufferIndex).getBytes(dataOffset, output, 0, dataLength);
      } else {
        // data is in the value buffer
        input.getBytes(
            (long) index * VIEW_BUFFER_SIZE + BUF_INDEX_WIDTH, output, 0, dataLength);
      }
    }
  }

  /**
   * Get the variable length element at specified index and sets the state
   * in provided holder.
   *
   * @param index   position of element to get
   * @param holder  data holder to be populated by this function
   */
  public void get(int index, NullableViewVarCharHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.start = getStartOffset(index);
    holder.end = getEndOffset(index);
    holder.buffer = valueBuffer;
    holder.dataBuffers = dataBuffers;
    holder.callBack = new HolderCallback();
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
  public void set(int index, ViewVarCharHolder holder) {
    // TODO: fix this
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
   * Same as {@link #set(int, ViewVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, ViewVarCharHolder holder) {
    // TODO: fix this
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
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, NullableViewVarCharHolder holder) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, holder.isSet);
    final int startOffset = getStartOffset(index);
    if (holder.isSet != 0) {
      final int dataLength = holder.end - holder.start;
      byte[] data = new byte[dataLength];
      holder.buffer.getBytes(holder.start, data, 0, dataLength);
      setBytes(index, data, holder.start, dataLength);
    } else {
      byte[] data = new byte[0];
      setBytes(index, data, 0, 0);
      offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset);
    }
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, NullableViewVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, NullableViewVarCharHolder holder) {
    assert index >= 0;
    if (holder.isSet != 0) {
      final int dataLength = holder.end - holder.start;
      handleSafe(index, dataLength);
      fillHoles(index);
      byte[] data = new byte[dataLength];
      holder.buffer.getBytes(holder.start, data, 0, dataLength);
      setBytes(index, data, holder.start, dataLength);
    } else {
      fillEmpties(index + 1);
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
    set(index, text.getBytes(), 0, (int) text.getLength());
  }

  /**
   * Same as {@link #set(int, NullableViewVarCharHolder)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set.
   * @param text    Text object with data
   */
  public void setSafe(int index, Text text) {
    setSafe(index, text.getBytes(), 0, (int) text.getLength());
  }

  @Override
  public void validateScalars() {
    for (int i = 0; i < getValueCount(); ++i) {
      byte[] value = get(i);
      if (value != null) {
        ValidateUtil.validateOrThrow(Text.validateUTF8NoThrow(value),
            "Non-UTF-8 data in VarCharVector at position " + i + ".");
      }
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |                      vector transfer                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising this and a target vector of the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support getTransferPair(String, BufferAllocator)");
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param field The field materialized by this vector.
   * @param allocator allocator for the target vector
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support getTransferPair(Field, BufferAllocator)");
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param target the target for the transfer
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support makeTransferPair(ValueVector)");
  }
}
