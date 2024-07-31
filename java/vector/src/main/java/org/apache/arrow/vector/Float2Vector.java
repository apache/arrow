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
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.complex.impl.Float2ReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.Float2Holder;
import org.apache.arrow.vector.holders.NullableFloat2Holder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Float2Vector implements a fixed width (2 bytes) vector of short values which could be null. A
 * validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public final class Float2Vector extends BaseFixedWidthVector
    implements FloatingPointVector, ValueIterableVector<Short> {
  public static final byte TYPE_WIDTH = 2;

  /**
   * Instantiate a Float2Vector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public Float2Vector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.FLOAT2.getType()), allocator);
  }

  /**
   * Instantiate a Float2Vector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public Float2Vector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a Float2Vector. This doesn't allocate any memory for the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public Float2Vector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new Float2ReaderImpl(Float2Vector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.FLOAT2;
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
  public short get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getShort((long) index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableFloat2Holder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getShort((long) index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public Short getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getShort((long) index * TYPE_WIDTH);
    }
  }

  /**
   * Given a data buffer, get the value stored at a particular position in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  static short get(final ArrowBuf buffer, final int index) {
    return buffer.getShort((long) index * TYPE_WIDTH);
  }

  @Override
  public double getValueAsDouble(int index) {
    return getValueAsFloat(index);
  }

  public float getValueAsFloat(int index) {
    return Float16.toFloat(this.get(index));
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  private void setValue(int index, short value) {
    valueBuffer.setShort((long) index * TYPE_WIDTH, value);
  }

  private void setValue(int index, float value) {
    valueBuffer.setShort((long) index * TYPE_WIDTH, Float16.toFloat16(value));
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, short value) {
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setWithPossibleTruncate(int index, float value) {
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
  public void set(int index, NullableFloat2Holder holder) throws IllegalArgumentException {
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
  public void set(int index, Float2Holder holder) {
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, short)} except that it handles the case when index is greater than or
   * equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafe(int index, short value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #setWithPossibleTruncate(int, float)} except that it handles the case when index
   * is greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafeWithPossibleTruncate(int index, float value) {
    handleSafe(index);
    setWithPossibleTruncate(index, value);
  }

  /**
   * Same as {@link #set(int, NullableFloat2Holder)} except that it handles the case when index is
   * greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableFloat2Holder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, Float2Holder)} except that it handles the case when index is greater
   * than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, Float2Holder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates whether the value
   * is NULL or not.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void set(int index, int isSet, short value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates whether the value
   * is NULL or not.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setWithPossibleTruncate(int index, int isSet, float value) {
    if (isSet > 0) {
      setWithPossibleTruncate(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, int, short)} except that it handles the case when index is greater
   * than or equal to current value capacity of the vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setSafe(int index, int isSet, short value) {
    handleSafe(index);
    set(index, isSet, value);
  }

  /**
   * Same as {@link #set(int, int, short)} except that it handles the case when index is greater
   * than or equal to current value capacity of the vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setSafeWithPossibleTruncate(int index, int isSet, float value) {
    handleSafe(index);
    setWithPossibleTruncate(index, isSet, value);
  }

  @Override
  public void setWithPossibleTruncate(int index, double value) {
    throw new UnsupportedOperationException(
        "The operation for double data types is not supported.");
  }

  @Override
  public void setSafeWithPossibleTruncate(int index, double value) {
    throw new UnsupportedOperationException(
        "The operation for double data types is not supported.");
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
   * @return {@link TransferPair}
   */
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

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((Float2Vector) to);
  }

  private class TransferImpl implements TransferPair {
    Float2Vector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new Float2Vector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new Float2Vector(field, allocator);
    }

    public TransferImpl(Float2Vector to) {
      this.to = to;
    }

    @Override
    public Float2Vector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, Float2Vector.this);
    }
  }
}
