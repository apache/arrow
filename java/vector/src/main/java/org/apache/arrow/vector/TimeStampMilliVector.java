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
import org.apache.arrow.vector.complex.impl.TimeStampMilliReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.joda.time.LocalDateTime;

/**
 * TimeStampMilliVector implements a fixed width vector (8 bytes) of
 * timestamp (millisecond resolution) values which could be null. A validity buffer
 * (bit vector) is maintained to track which elements in the vector are null.
 */
public class TimeStampMilliVector extends TimeStampVector {
  private final FieldReader reader;

  /**
   * Instantiate a TimeStampMilliVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public TimeStampMilliVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.TIMESTAMPMILLI.getType()), allocator);
  }

  /**
   * Instantiate a TimeStampMilliVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public TimeStampMilliVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, fieldType, allocator);
    reader = new TimeStampMilliReaderImpl(TimeStampMilliVector.this);
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
    return MinorType.TIMESTAMPMILLI;
  }


  /******************************************************************
   *                                                                *
   *          vector value retrieval methods                        *
   *                                                                *
   ******************************************************************/


  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(int index, NullableTimeStampMilliHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public LocalDateTime getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      final long millis = valueBuffer.getLong(index * TYPE_WIDTH);
      final org.joda.time.LocalDateTime localDateTime = new org.joda.time.LocalDateTime(millis,
              org.joda.time.DateTimeZone.UTC);
      return localDateTime;
    }
  }


  /******************************************************************
   *                                                                *
   *          vector value setter methods                           *
   *                                                                *
   ******************************************************************/


  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableTimeStampMilliHolder holder) throws IllegalArgumentException {
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
  public void set(int index, TimeStampMilliHolder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, NullableTimeStampMilliHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableTimeStampMilliHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, TimeStampMilliHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, TimeStampMilliHolder holder) {
    handleSafe(index);
    set(index, holder);
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
    TimeStampMilliVector to = new TimeStampMilliVector(ref,
            field.getFieldType(), allocator);
    return new TransferImpl(to);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((TimeStampMilliVector) to);
  }
}
