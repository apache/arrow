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
import org.apache.arrow.vector.complex.impl.TimeStampMilliTZReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.TimeStampMilliTZHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * TimeStampMilliTZVector implements a fixed width vector (8 bytes) of timestamp (millisecond
 * resolution) values which could be null. A validity buffer (bit vector) is maintained to track
 * which elements in the vector are null.
 */
public final class TimeStampMilliTZVector extends TimeStampVector
    implements ValueIterableVector<Long> {
  private final String timeZone;

  /**
   * Instantiate a TimeStampMilliTZVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public TimeStampMilliTZVector(String name, BufferAllocator allocator, String timeZone) {
    this(
        name,
        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZone)),
        allocator);
  }

  /**
   * Instantiate a TimeStampMilliTZVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public TimeStampMilliTZVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, fieldType, allocator);
    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) fieldType.getType();
    timeZone = arrowType.getTimezone();
  }

  /**
   * Instantiate a TimeStampMilliTZVector. This doesn't allocate any memory for the data in vector.
   *
   * @param field Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public TimeStampMilliTZVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) field.getFieldType().getType();
    timeZone = arrowType.getTimezone();
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new TimeStampMilliTZReaderImpl(TimeStampMilliTZVector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.TIMESTAMPMILLITZ;
  }

  /**
   * Get the time zone of the timestamps stored in this vector.
   *
   * @return the time zone of the timestamps stored in this vector.
   */
  public String getTimeZone() {
    return this.timeZone;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableTimeStampMilliTZHolder holder) {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getLong((long) index * TYPE_WIDTH);
    holder.timezone = timeZone;
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public Long getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return valueBuffer.getLong((long) index * TYPE_WIDTH);
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Set the element at the given index to the value set in data holder. If the value in holder is
   * not indicated as set, element in the at the given index will be null.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void set(int index, NullableTimeStampMilliTZHolder holder)
      throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (!this.timeZone.equals(holder.timezone)) {
      throw new IllegalArgumentException(
          String.format(
              "holder.timezone: %s not equal to vector timezone: %s",
              holder.timezone, this.timeZone));
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
  public void set(int index, TimeStampMilliTZHolder holder) {
    if (!this.timeZone.equals(holder.timezone)) {
      throw new IllegalArgumentException(
          String.format(
              "holder.timezone: %s not equal to vector timezone: %s",
              holder.timezone, this.timeZone));
    }
    BitVectorHelper.setBit(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, NullableTimeStampMilliTZHolder)} except that it handles the case when
   * index is greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableTimeStampMilliTZHolder holder)
      throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, TimeStampMilliTZHolder)} except that it handles the case when index is
   * greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, TimeStampMilliTZHolder holder) {
    handleSafe(index);
    set(index, holder);
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
    TimeStampMilliTZVector to = new TimeStampMilliTZVector(ref, field.getFieldType(), allocator);
    return new TransferImpl(to);
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
    TimeStampMilliTZVector to = new TimeStampMilliTZVector(field, allocator);
    return new TransferImpl(to);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((TimeStampMilliTZVector) to);
  }
}
