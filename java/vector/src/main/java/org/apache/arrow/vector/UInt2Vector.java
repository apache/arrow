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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.UInt2ReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.UInt2Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * UInt2Vector implements a fixed width (2 bytes) vector of integer values which could be null. A
 * validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public final class UInt2Vector extends BaseFixedWidthVector
    implements BaseIntVector, ValueIterableVector<Character> {

  /** The maximum 16-bit unsigned integer. */
  public static final char MAX_UINT2 = (char) 0XFFFF;

  public static final byte TYPE_WIDTH = 2;

  public UInt2Vector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.UINT2.getType()), allocator);
  }

  public UInt2Vector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Constructor for UInt2Vector type.
   *
   * @param field Field type
   * @param allocator Allocator type
   */
  public UInt2Vector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new UInt2ReaderImpl(UInt2Vector.this);
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
   * Given a data buffer, get the value stored at a particular position in the vector.
   *
   * <p>This method is mainly meant for integration tests.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  public static char get(final ArrowBuf buffer, final int index) {
    return buffer.getChar((long) index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index position of element
   * @return element at given index
   */
  public char get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getChar((long) index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableUInt2Holder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getChar((long) index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public Character getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getChar((long) index * TYPE_WIDTH);
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  private void setValue(int index, int value) {
    valueBuffer.setChar((long) index * TYPE_WIDTH, value);
  }

  private void setValue(int index, char value) {
    valueBuffer.setChar((long) index * TYPE_WIDTH, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, int value) {
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, char value) {
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the value set in data holder. If the value in holder is
   * not indicated as set, element in the at the given index will be null.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void set(int index, NullableUInt2Holder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setBit(validityBuffer, index);
      setValue(index, holder.value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void set(int index, UInt2Holder holder) {
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, int)} except that it handles the case when index is greater than or
   * equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafe(int index, int value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, char)} except that it handles the case when index is greater than or
   * equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafe(int index, char value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableUInt2Holder)} except that it handles the case when index is
   * greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableUInt2Holder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, UInt2Holder)} except that it handles the case when index is greater
   * than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, UInt2Holder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Sets the given index to value is isSet is positive, otherwise sets the position as
   * invalid/null.
   */
  public void set(int index, int isSet, char value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, int, char)} but will reallocate the buffer if index is larger than
   * current capacity.
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

  /**
   * Construct a TransferPair comprising this and a target vector of the same type.
   *
   * @param field Field object used by the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((UInt2Vector) to);
  }

  @Override
  public void setWithPossibleTruncate(int index, long value) {
    this.setSafe(index, (int) value);
  }

  @Override
  public void setUnsafeWithPossibleTruncate(int index, long value) {
    this.set(index, (int) value);
  }

  @Override
  public long getValueAsLong(int index) {
    return this.get(index);
  }

  @Override
  public String toString() {
    return ValueVectorUtility.getToString(
        this,
        0,
        getValueCount(),
        (v, i) -> v.isNull(i) ? "null" : Integer.toString(v.get(i) & 0x0000ffff));
  }

  private class TransferImpl implements TransferPair {
    UInt2Vector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new UInt2Vector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new UInt2Vector(field, allocator);
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
