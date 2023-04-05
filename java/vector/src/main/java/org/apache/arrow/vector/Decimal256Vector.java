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

import java.math.BigDecimal;
import java.nio.ByteOrder;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.complex.impl.Decimal256ReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.Decimal256Holder;
import org.apache.arrow.vector.holders.NullableDecimal256Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.arrow.vector.util.TransferPair;


/**
 * Decimal256Vector implements a fixed width vector (32 bytes) of
 * decimal values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public final class Decimal256Vector extends BaseFixedWidthVector {
  public static final int MAX_PRECISION = 76;
  public static final byte TYPE_WIDTH = 32;
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  private final FieldReader reader;

  private final int precision;
  private final int scale;

  /**
   * Instantiate a Decimal256Vector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public Decimal256Vector(String name, BufferAllocator allocator,
                               int precision, int scale) {
    this(name, FieldType.nullable(new ArrowType.Decimal(precision, scale, /*bitWidth=*/TYPE_WIDTH * 8)), allocator);
  }

  /**
   * Instantiate a Decimal256Vector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public Decimal256Vector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a Decimal256Vector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public Decimal256Vector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
    ArrowType.Decimal arrowType = (ArrowType.Decimal) field.getFieldType().getType();
    reader = new Decimal256ReaderImpl(Decimal256Vector.this);
    this.precision = arrowType.getPrecision();
    this.scale = arrowType.getScale();
  }

  /**
   * Get a reader that supports reading values from this vector.
   *
   * @return Field Reader for this vector
   */
  @Override
  public FieldReader getReader() {
    return reader;
  }

  /**
   * Get minor type for this vector. The vector holds values belonging
   * to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.DECIMAL256;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public ArrowBuf get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.slice((long) index * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(int index, NullableDecimal256Holder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.buffer = valueBuffer;
    holder.precision = precision;
    holder.scale = scale;
    holder.start = ((long) index) * TYPE_WIDTH;
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
      return DecimalUtility.getBigDecimalFromArrowBuf(valueBuffer, index, scale, TYPE_WIDTH);
    }
  }

  /**
   * Return precision for the decimal value.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * Return scale for the decimal value.
   */
  public int getScale() {
    return scale;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Set the element at the given index to the given value.
   *
   * @param index    position of element
   * @param buffer   ArrowBuf containing decimal value.
   */
  public void set(int index, ArrowBuf buffer) {
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * TYPE_WIDTH, buffer, 0, TYPE_WIDTH);
  }

  /**
   * Set the decimal element at given index to the provided array of bytes.
   * Decimal256 is now implemented as Native Endian. This API allows the user
   * to pass a decimal value in the form of byte array in BE byte order.
   *
   * <p>Consumers of Arrow code can use this API instead of first swapping
   * the source bytes (doing a write and read) and then finally writing to
   * ArrowBuf of decimal vector.
   *
   * <p>This method takes care of adding the necessary padding if the length
   * of byte array is less then 32 (length of decimal type).
   *
   * @param index position of element
   * @param value array of bytes containing decimal in big endian byte order.
   */
  public void setBigEndian(int index, byte[] value) {
    BitVectorHelper.setBit(validityBuffer, index);
    final int length = value.length;

    // do the bound check.
    valueBuffer.checkBytes((long) index * TYPE_WIDTH, (long) (index + 1) * TYPE_WIDTH);

    long outAddress = valueBuffer.memoryAddress() + (long) index * TYPE_WIDTH;
    if (length == 0) {
      MemoryUtil.UNSAFE.setMemory(outAddress, Decimal256Vector.TYPE_WIDTH, (byte) 0);
      return;
    }
    if (LITTLE_ENDIAN) {
      // swap bytes to convert BE to LE
      for (int byteIdx = 0; byteIdx < length; ++byteIdx) {
        MemoryUtil.UNSAFE.putByte(outAddress + byteIdx, value[length - 1 - byteIdx]);
      }

      if (length == TYPE_WIDTH) {
        return;
      }

      if (length < TYPE_WIDTH) {
        // sign extend
        final byte pad = (byte) (value[0] < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress + length, Decimal256Vector.TYPE_WIDTH - length, pad);
        return;
      }
    } else {
      if (length <= TYPE_WIDTH) {
        // copy data from value to outAddress
        MemoryUtil.UNSAFE.copyMemory(
                value,
                MemoryUtil.BYTE_ARRAY_BASE_OFFSET,
                null,
                outAddress + Decimal256Vector.TYPE_WIDTH - length,
                length);
        // sign extend
        final byte pad = (byte) (value[0] < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress, Decimal256Vector.TYPE_WIDTH - length, pad);
        return;
      }
    }
    throw new IllegalArgumentException(
        "Invalid decimal value length. Valid length in [1 - 32], got " + length);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index    position of element
   * @param start    start index of data in the buffer
   * @param buffer   ArrowBuf containing decimal value.
   */
  public void set(int index, long start, ArrowBuf buffer) {
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * TYPE_WIDTH, buffer, start, TYPE_WIDTH);
  }

  /**
   * Sets the element at given index using the buffer whose size maybe <= 32 bytes.
   * @param index index to write the decimal to
   * @param start start of value in the buffer
   * @param buffer contains the decimal in native endian bytes
   * @param length length of the value in the buffer
   */
  public void setSafe(int index, long start, ArrowBuf buffer, int length) {
    handleSafe(index);
    BitVectorHelper.setBit(validityBuffer, index);

    // do the bound checks.
    buffer.checkBytes(start, start + length);
    valueBuffer.checkBytes((long) index * TYPE_WIDTH, (long) (index + 1) * TYPE_WIDTH);

    long inAddress = buffer.memoryAddress() + start;
    long outAddress = valueBuffer.memoryAddress() + (long) index * TYPE_WIDTH;
    if (LITTLE_ENDIAN) {
      MemoryUtil.UNSAFE.copyMemory(inAddress, outAddress, length);
      // sign extend
      if (length < TYPE_WIDTH) {
        byte msb = MemoryUtil.UNSAFE.getByte(inAddress + length - 1);
        final byte pad = (byte) (msb < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress + length, Decimal256Vector.TYPE_WIDTH - length, pad);
      }
    } else {
      MemoryUtil.UNSAFE.copyMemory(inAddress, outAddress + Decimal256Vector.TYPE_WIDTH - length, length);
      // sign extend
      if (length < TYPE_WIDTH) {
        byte msb = MemoryUtil.UNSAFE.getByte(inAddress);
        final byte pad = (byte) (msb < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress, Decimal256Vector.TYPE_WIDTH - length, pad);
      }
    }
  }


  /**
   * Sets the element at given index using the buffer whose size maybe <= 32 bytes.
   * @param index index to write the decimal to
   * @param start start of value in the buffer
   * @param buffer contains the decimal in big endian bytes
   * @param length length of the value in the buffer
   */
  public void setBigEndianSafe(int index, long start, ArrowBuf buffer, int length) {
    handleSafe(index);
    BitVectorHelper.setBit(validityBuffer, index);

    // do the bound checks.
    buffer.checkBytes(start, start + length);
    valueBuffer.checkBytes((long) index * TYPE_WIDTH, (long) (index + 1) * TYPE_WIDTH);

    // not using buffer.getByte() to avoid boundary checks for every byte.
    long inAddress = buffer.memoryAddress() + start;
    long outAddress = valueBuffer.memoryAddress() + (long) index * TYPE_WIDTH;
    if (LITTLE_ENDIAN) {
      // swap bytes to convert BE to LE
      for (int byteIdx = 0; byteIdx < length; ++byteIdx) {
        byte val = MemoryUtil.UNSAFE.getByte((inAddress + length - 1) - byteIdx);
        MemoryUtil.UNSAFE.putByte(outAddress + byteIdx, val);
      }
      // sign extend
      if (length < 32) {
        byte msb = MemoryUtil.UNSAFE.getByte(inAddress);
        final byte pad = (byte) (msb < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress + length, Decimal256Vector.TYPE_WIDTH - length, pad);
      }
    } else {
      MemoryUtil.UNSAFE.copyMemory(inAddress, outAddress + Decimal256Vector.TYPE_WIDTH - length, length);
      // sign extend
      if (length < TYPE_WIDTH) {
        byte msb = MemoryUtil.UNSAFE.getByte(inAddress);
        final byte pad = (byte) (msb < 0 ? 0xFF : 0x00);
        MemoryUtil.UNSAFE.setMemory(outAddress, Decimal256Vector.TYPE_WIDTH - length, pad);
      }
    }
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   BigDecimal containing decimal value.
   */
  public void set(int index, BigDecimal value) {
    BitVectorHelper.setBit(validityBuffer, index);
    DecimalUtility.checkPrecisionAndScale(value, precision, scale);
    DecimalUtility.writeBigDecimalToArrowBuf(value, valueBuffer, index, TYPE_WIDTH);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   long value.
   */
  public void set(int index, long value) {
    BitVectorHelper.setBit(validityBuffer, index);
    DecimalUtility.writeLongToArrowBuf(value, valueBuffer, index, TYPE_WIDTH);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableDecimal256Holder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setBit(validityBuffer, index);
      valueBuffer.setBytes((long) index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void set(int index, Decimal256Holder holder) {
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * TYPE_WIDTH, holder.buffer, holder.start, TYPE_WIDTH);
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
  public void setSafe(int index, long start, ArrowBuf buffer) {
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
   * Same as {@link #set(int, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   long value.
   */
  public void setSafe(int index, long value) {
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
  public void setSafe(int index, NullableDecimal256Holder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, Decimal256Holder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, Decimal256Holder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of the value in the buffer
   * @param buffer buffer containing the value to be stored in the vector
   */
  public void set(int index, int isSet, long start, ArrowBuf buffer) {
    if (isSet > 0) {
      set(index, start, buffer);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #setSafe(int, int, int, ArrowBuf)} except that it handles
   * the case when the position of new value is beyond the current value
   * capacity of the vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of the value in the buffer
   * @param buffer buffer containing the value to be stored in the vector
   */
  public void setSafe(int index, int isSet, long start, ArrowBuf buffer) {
    handleSafe(index);
    set(index, isSet, start, buffer);
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
    return new TransferImpl(ref, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Decimal256Vector) to);
  }

  private class TransferImpl implements TransferPair {
    Decimal256Vector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new Decimal256Vector(ref, allocator, Decimal256Vector.this.precision,
              Decimal256Vector.this.scale);
    }

    public TransferImpl(Decimal256Vector to) {
      this.to = to;
    }

    @Override
    public Decimal256Vector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, Decimal256Vector.this);
    }
  }
}
