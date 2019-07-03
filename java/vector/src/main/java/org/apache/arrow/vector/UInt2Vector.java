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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.impl.UInt2ReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.UInt2Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * UInt2Vector implements a fixed width (2 bytes) vector of
 * integer values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class UInt2Vector extends BaseFixedWidthVector implements BaseIntVector {
  private static final byte TYPE_WIDTH = 2;
  private final FieldReader reader;

  public UInt2Vector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.UINT2.getType()), allocator);
  }

  public UInt2Vector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  public UInt2Vector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
    reader = new UInt2ReaderImpl(UInt2Vector.this);
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.UINT2;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/
  /**
   * Given a data buffer, get the value stored at a particular position
   * in the vector.
   *
   * <p>This method is mainly meant for integration tests.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  public static char get(final ArrowBuf buffer, final int index) {
    return buffer.getChar(index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public char get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getChar(index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(int index, NullableUInt2Holder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getChar(index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public Character getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getChar(index * TYPE_WIDTH);
    }
  }

  /** Copies a value and validity bit from the given vector to this one. */
  public void copyFrom(int fromIndex, int thisIndex, UInt2Vector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    final char value = from.valueBuffer.getChar(fromIndex * TYPE_WIDTH);
    valueBuffer.setChar(thisIndex * TYPE_WIDTH, value);
  }

  /**
   * Same as {@link #copyFrom(int, int, UInt2Vector)} but reallocate buffer if
   * index is larger than capacity.
   */
  public void copyFromSafe(int fromIndex, int thisIndex, UInt2Vector from) {
    handleSafe(thisIndex);
    copyFrom(fromIndex, thisIndex, from);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  private void setValue(int index, int value) {
    valueBuffer.setChar(index * TYPE_WIDTH, value);
  }

  private void setValue(int index, char value) {
    valueBuffer.setChar(index * TYPE_WIDTH, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void set(int index, int value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void set(int index, char value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableUInt2Holder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
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
  public void set(int index, UInt2Holder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, int)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void setSafe(int index, int value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, char)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void setSafe(int index, char value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableUInt2Holder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableUInt2Holder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, UInt2Holder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, UInt2Holder holder) {
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
   * Sets the given index to value is isSet is positive, otherwise sets
   * the position as invalid/null.
   */
  public void set(int index, int isSet, char value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int, char)} but will reallocate the buffer if index
   * is larger than current capacity.
   */
  public void setSafe(int index, int isSet, char value) {
    handleSafe(index);
    set(index, isSet, value);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/


  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((UInt2Vector) to);
  }

  @Override
  public void setEncodedValue(int index, int value) {
    Preconditions.checkArgument(value <= Character.MAX_VALUE, "value is overflow: %s", value);
    this.setSafe(index, value);
  }

  private class TransferImpl implements TransferPair {
    UInt2Vector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new UInt2Vector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(UInt2Vector to) {
      this.to = to;
    }

    @Override
    public UInt2Vector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, UInt2Vector.this);
    }
  }
}
