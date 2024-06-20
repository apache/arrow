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
import org.apache.arrow.vector.complex.impl.UInt1ReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.UInt1Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * UInt1Vector implements a fixed width (1 bytes) vector of integer values which could be null. A
 * validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public final class UInt1Vector extends BaseFixedWidthVector
    implements BaseIntVector, ValueIterableVector<Byte> {
  /** The mask to use when promoting the unsigned byte value to an integer. */
  public static final int PROMOTION_MASK = 0xFF;

  /** The maximum 8-bit unsigned integer. */
  public static final byte MAX_UINT1 = (byte) 0XFF;

  public static final byte TYPE_WIDTH = 1;

  public UInt1Vector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.UINT1.getType()), allocator);
  }

  public UInt1Vector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Constructor for UInt1Vector.
   *
   * @param field Field type
   * @param allocator Allocator type
   */
  public UInt1Vector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new UInt1ReaderImpl(UInt1Vector.this);
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.UINT1;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/
  /**
   * Given a data buffer, get the value stored at a particular position in the vector.
   *
   * <p>To avoid overflow, the returned type is one step up from the signed type.
   *
   * <p>This method is mainly meant for integration tests.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  public static short getNoOverflow(final ArrowBuf buffer, final int index) {
    byte b = buffer.getByte(index * TYPE_WIDTH);
    return (short) (PROMOTION_MASK & b);
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index position of element
   * @return element at given index
   */
  public byte get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getByte(index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableUInt1Holder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getByte(index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public Byte getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getByte(index * TYPE_WIDTH);
    }
  }

  /**
   * Returns the value stored at index without the potential for overflow.
   *
   * @param index position of element
   * @return element at given index
   */
  public Short getObjectNoOverflow(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return getNoOverflow(valueBuffer, index);
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  private void setValue(int index, int value) {
    valueBuffer.setByte(index * TYPE_WIDTH, value);
  }

  private void setValue(int index, byte value) {
    valueBuffer.setByte(index * TYPE_WIDTH, value);
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
  public void set(int index, byte value) {
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
  public void set(int index, NullableUInt1Holder holder) throws IllegalArgumentException {
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
  public void set(int index, UInt1Holder holder) {
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
   * Same as {@link #set(int, byte)} except that it handles the case when index is greater than or
   * equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafe(int index, byte value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableUInt1Holder)} except that it handles the case when index is
   * greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableUInt1Holder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, UInt1Holder)} except that it handles the case when index is greater
   * than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, UInt1Holder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Sets the value at index to value isSet > 0, otherwise sets the index position to invalid/null.
   */
  public void set(int index, int isSet, byte value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, int, byte)} but will reallocate the buffer if index is larger than
   * current capacity.
   */
  public void setSafe(int index, int isSet, byte value) {
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
    return new TransferImpl((UInt1Vector) to);
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
    return this.get(index) & PROMOTION_MASK;
  }

  @Override
  public String toString() {
    return ValueVectorUtility.getToString(
        this, 0, getValueCount(), (v, i) -> v.getObjectNoOverflow(i));
  }

  private class TransferImpl implements TransferPair {
    UInt1Vector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new UInt1Vector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new UInt1Vector(field, allocator);
    }

    public TransferImpl(UInt1Vector to) {
      this.to = to;
    }

    @Override
    public UInt1Vector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, UInt1Vector.this);
    }
  }
}
