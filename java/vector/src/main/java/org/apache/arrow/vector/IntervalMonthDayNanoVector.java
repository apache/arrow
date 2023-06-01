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

import java.time.Duration;
import java.time.Period;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.IntervalMonthDayNanoReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.IntervalMonthDayNanoHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * IntervalMonthDayNanoVector implements a fixed width vector (16 bytes) of
 * interval (month, days and nanoseconds) values which could be null.
 * A validity buffer (bit vector) is maintained to track which elements in the
 * vector are null.
 *
 * Month, day and nanoseconds are indepndent from one another and there
 * is no specific limits imposed on their values.
 */
public final class IntervalMonthDayNanoVector extends BaseFixedWidthVector {
  public static final byte TYPE_WIDTH = 16;
  private static final byte DAY_OFFSET = 4;
  private static final byte NANOSECOND_OFFSET = 8;
  private final FieldReader reader;


  /**
   * Instantiate a IntervalMonthDayNanoVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public IntervalMonthDayNanoVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.INTERVALMONTHDAYNANO.getType()), allocator);
  }

  /**
   * Instantiate a IntervalMonthDayNanoVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public IntervalMonthDayNanoVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a IntervalMonthDayNanoVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public IntervalMonthDayNanoVector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
    reader = new IntervalMonthDayNanoReaderImpl(IntervalMonthDayNanoVector.this);
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
    return MinorType.INTERVALMONTHDAYNANO;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/

  /**
   * Given a data buffer, get the number of months stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return day value stored at the index.
   */
  public static int getMonths(final ArrowBuf buffer, final int index) {
    return buffer.getInt((long) index * TYPE_WIDTH);
  }


  /**
   * Given a data buffer, get the number of days stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return day value stored at the index.
   */
  public static int getDays(final ArrowBuf buffer, final int index) {
    return buffer.getInt((long) index * TYPE_WIDTH + DAY_OFFSET);
  }

  /**
   * Given a data buffer, get the get the number of nanoseconds stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return nanoseconds value stored at the index.
   */
  public static long getNanoseconds(final ArrowBuf buffer, final int index) {
    return buffer.getLong((long) index * TYPE_WIDTH + NANOSECOND_OFFSET);
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public ArrowBuf get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
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
  public void get(int index, NullableIntervalMonthDayNanoHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    final long startIndex = (long) index * TYPE_WIDTH;
    holder.isSet = 1;
    holder.months = valueBuffer.getInt(startIndex);
    holder.days = valueBuffer.getInt(startIndex + DAY_OFFSET);
    holder.nanoseconds = valueBuffer.getLong(startIndex + NANOSECOND_OFFSET);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public PeriodDuration getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      final long startIndex = (long) index * TYPE_WIDTH;
      final int months = valueBuffer.getInt(startIndex);
      final int days = valueBuffer.getInt(startIndex + DAY_OFFSET);
      final long nanoseconds = valueBuffer.getLong(startIndex + NANOSECOND_OFFSET);

      return new PeriodDuration(Period.ofMonths(months).plusDays(days),
            Duration.ofNanos(nanoseconds));
    }
  }

  /**
   * Get the Interval value at a given index as a {@link StringBuilder} object.
   *
   * @param index position of the element
   * @return String Builder object with Interval value as
   */
  public StringBuilder getAsStringBuilder(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return getAsStringBuilderHelper(index);
    }
  }

  private StringBuilder getAsStringBuilderHelper(int index) {
    return new StringBuilder().append(getObject(index).toString()).append(" ");
  }

  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void set(int index, ArrowBuf value) {
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * TYPE_WIDTH, value, 0, TYPE_WIDTH);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index          position of element
   * @param months months component of interval
   * @param days days component of interval
   * @param nanoseconds nanosecond component of interval
   */
  public void set(int index, int months, int days, long nanoseconds) {
    final long offsetIndex = (long) index * TYPE_WIDTH;
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setInt(offsetIndex, months);
    valueBuffer.setInt(offsetIndex + DAY_OFFSET, days);
    valueBuffer.setLong((offsetIndex + NANOSECOND_OFFSET), nanoseconds);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableIntervalMonthDayNanoHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      set(index, holder.months, holder.days, holder.nanoseconds);
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
  public void set(int index, IntervalMonthDayNanoHolder holder) {
    set(index, holder.months, holder.days, holder.nanoseconds);
  }

  /**
   * Same as {@link #set(int, ArrowBuf)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void setSafe(int index, ArrowBuf value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, int, int, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index          position of element
   * @param months         months for the interval
   * @param days           days for the interval
   * @param nanoseconds   nanoseconds for the interval
   */
  public void setSafe(int index, int months, int days, long nanoseconds) {
    handleSafe(index);
    set(index, months, days, nanoseconds);
  }

  /**
   * Same as {@link #set(int, NullableIntervalMonthDayNanoHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableIntervalMonthDayNanoHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, IntervalMonthDayNanoHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, IntervalMonthDayNanoHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param months months component of interval
   * @param days days component of interval
   * @param nanoseconds nanosecond component of interval
   */
  public void set(int index, int isSet, int months, int days, long nanoseconds) {
    if (isSet > 0) {
      set(index, months, days, nanoseconds);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, int, int, int, long)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param months months component of interval
   * @param days days component of interval
   * @param nanoseconds nanosecond component of interval
   */
  public void setSafe(int index, int isSet, int months, int days,
       long nanoseconds) {
    handleSafe(index);
    set(index, isSet, months, days, nanoseconds);
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
    return new TransferImpl((IntervalMonthDayNanoVector) to);
  }

  private class TransferImpl implements TransferPair {
    IntervalMonthDayNanoVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new IntervalMonthDayNanoVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(IntervalMonthDayNanoVector to) {
      this.to = to;
    }

    @Override
    public IntervalMonthDayNanoVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, IntervalMonthDayNanoVector.this);
    }
  }
}
