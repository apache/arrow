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

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.DecimalReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.DecimalHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * DecimalVector implements a fixed width vector (16 bytes) of
 * decimal values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class DecimalVector extends BaseFixedWidthVector {
  public static final byte TYPE_WIDTH = 16;
  private final FieldReader reader;

  private final int precision;
  private final int scale;

  /**
   * Instantiate a DecimalVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public DecimalVector(String name, BufferAllocator allocator,
                               int precision, int scale) {
    this(name, FieldType.nullable(
      new ArrowType.Decimal(precision, scale)), allocator);
  }

  /**
   * Instantiate a DecimalVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public DecimalVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator, fieldType, TYPE_WIDTH);
    ArrowType.Decimal arrowType = (ArrowType.Decimal) fieldType.getType();
    reader = new DecimalReaderImpl(DecimalVector.this);
    this.precision = arrowType.getPrecision();
    this.scale = arrowType.getScale();
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
    return MinorType.DECIMAL;
  }


  /******************************************************************
   *                                                                *
   *          vector value retrieval methods                        *
   *                                                                *
   ******************************************************************/


  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public ArrowBuf get(int index) throws IllegalStateException {
    if (isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.slice(index * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(int index, NullableDecimalHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.buffer = valueBuffer;
    holder.precision = precision;
    holder.scale = scale;
    holder.start = index * TYPE_WIDTH;
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public BigDecimal getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return DecimalUtility.getBigDecimalFromArrowBuf(valueBuffer, index, scale);
    }
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, DecimalVector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    from.valueBuffer.getBytes(fromIndex * TYPE_WIDTH, valueBuffer,
            thisIndex * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Same as {@link #copyFrom(int, int, DecimalVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, DecimalVector from) {
    handleSafe(thisIndex);
    copyFrom(fromIndex, thisIndex, from);
  }

  /**
   * Return scale for the decimal value
   */
  public int getScale() {
    return scale;
  }


  /******************************************************************
   *                                                                *
   *          vector value setter methods                           *
   *                                                                *
   ******************************************************************/


  /**
   * Set the element at the given index to the given value.
   *
   * @param index    position of element
   * @param buffer   ArrowBuf containing decimal value.
   */
  public void set(int index, ArrowBuf buffer) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setBytes(index * TYPE_WIDTH, buffer, 0, TYPE_WIDTH);
  }

  /**
   * Set the decimal element at given index to the provided array of bytes.
   * Decimal is now implemented as Little Endian. This API allows the user
   * to pass a decimal value in the form of byte array in BE byte order.
   *
   * Consumers of Arrow code can use this API instead of first swapping
   * the source bytes (doing a write and read) and then finally writing to
   * ArrowBuf of decimal vector.
   *
   * This method takes care of adding the necessary padding if the length
   * of byte array is less then 16 (length of decimal type).
   *
   * @param index position of element
   * @param value array of bytes containing decimal in big endian byte order.
   */
  public void setBigEndian(int index, byte[] value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int length = value.length;
    int startIndex = index * TYPE_WIDTH;
    if (length == TYPE_WIDTH) {
      for (int i = TYPE_WIDTH - 1; i >= 3; i -= 4) {
        valueBuffer.setByte(startIndex, value[i]);
        valueBuffer.setByte(startIndex + 1, value[i - 1]);
        valueBuffer.setByte(startIndex + 2, value[i - 2]);
        valueBuffer.setByte(startIndex + 3, value[i - 3]);
        startIndex += 4;
      }

      return;
    }

    if (length == 0) {
      valueBuffer.setZero(startIndex, TYPE_WIDTH);
      return;
    }

    if (length < 16) {
      for (int i = length - 1; i >= 0; i--) {
        valueBuffer.setByte(startIndex, value[i]);
        startIndex++;
      }

      final byte pad = (byte) (value[0] < 0 ? 0xFF : 0x00);
      final int maxStartIndex = (index + 1) * TYPE_WIDTH;
      while (startIndex < maxStartIndex) {
        valueBuffer.setByte(startIndex, pad);
        startIndex++;
      }

      return;
    }

    throw new IllegalArgumentException("Invalid decimal value length. Valid length in [1 - 16], got " + length);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index    position of element
   * @param start    start index of data in the buffer
   * @param buffer   ArrowBuf containing decimal value.
   */
  public void set(int index, int start, ArrowBuf buffer) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setBytes(index * TYPE_WIDTH, buffer, start, TYPE_WIDTH);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   BigDecimal containing decimal value.
   */
  public void set(int index, BigDecimal value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    DecimalUtility.checkPrecisionAndScale(value, precision, scale);
    DecimalUtility.writeBigDecimalToArrowBuf(value, valueBuffer, index);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableDecimalHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      valueBuffer.setBytes(index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void set(int index, DecimalHolder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setBytes(index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
  }

  /**
   * Same as {@link #set(int, ArrowBuf)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param buffer  ArrowBuf containing decimal value.
   */
  public void setSafe(int index, ArrowBuf buffer) {
    handleSafe(index);
    set(index, buffer);
  }

  /**
   * Same as {@link #setBigEndian(int, byte[])} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   */
  public void setBigEndianSafe(int index, byte[] value) {
    handleSafe(index);
    setBigEndian(index, value);
  }

  /**
   * Same as {@link #set(int, int, ArrowBuf)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index    position of element
   * @param start    start index of data in the buffer
   * @param buffer   ArrowBuf containing decimal value.
   */
  public void setSafe(int index, int start, ArrowBuf buffer) {
    handleSafe(index);
    set(index, start, buffer);
  }

  /**
   * Same as {@link #set(int, BigDecimal)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   BigDecimal containing decimal value.
   */
  public void setSafe(int index, BigDecimal value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableDecimalHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableDecimalHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, DecimalHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, DecimalHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index   position of element
   */
  public void setNull(int index) {
    handleSafe(index);
    // not really needed to set the bit to 0 as long as
    // the buffer always starts from 0.
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of the value in the buffer
   * @param buffer buffer containing the value to be stored in the vector
   */
  public void set(int index, int isSet, int start, ArrowBuf buffer) {
    if (isSet > 0) {
      set(index, start, buffer);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #setSafe(int, int, int, ArrowBuf)} except that it handles
   * the case when the position of new value is beyond the current value
   * capacity of the vector.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of the value in the buffer
   * @param buffer buffer containing the value to be stored in the vector
   */
  public void setSafe(int index, int isSet, int start, ArrowBuf buffer) {
    handleSafe(index);
    set(index, isSet, start, buffer);
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
    return new TransferImpl((DecimalVector) to);
  }

  private class TransferImpl implements TransferPair {
    DecimalVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new DecimalVector(ref, allocator, DecimalVector.this.precision,
              DecimalVector.this.scale);
    }

    public TransferImpl(DecimalVector to) {
      this.to = to;
    }

    @Override
    public DecimalVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, DecimalVector.this);
    }
  }
}
