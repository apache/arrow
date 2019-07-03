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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.IntervalDayReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.IntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * IntervalDayVector implements a fixed width vector (8 bytes) of
 * interval (days and milliseconds) values which could be null.
 * A validity buffer (bit vector) is maintained to track which elements in the
 * vector are null.
 */
public class IntervalDayVector extends BaseFixedWidthVector {
  public static final byte TYPE_WIDTH = 8;
  private static final byte MILLISECOND_OFFSET = 4;
  private final FieldReader reader;

  /**
   * Instantiate a IntervalDayVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public IntervalDayVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.INTERVALDAY.getType()), allocator);
  }

  /**
   * Instantiate a IntervalDayVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public IntervalDayVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a IntervalDayVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public IntervalDayVector(Field field, BufferAllocator allocator) {
    super(field, allocator, TYPE_WIDTH);
    reader = new IntervalDayReaderImpl(IntervalDayVector.this);
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
    return MinorType.INTERVALDAY;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value retrieval methods                        |
   |                                                                |
   *----------------------------------------------------------------*/

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
    return buffer.getInt(index * TYPE_WIDTH);
  }

  /**
   * Given a data buffer, get the get the number of milliseconds stored at a particular position
   * in the vector.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return milliseconds value stored at the index.
   */
  public static int getMilliseconds(final ArrowBuf buffer, final int index) {
    return buffer.getInt((index * TYPE_WIDTH) + MILLISECOND_OFFSET);
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
    return valueBuffer.slice(index * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
   *
   * @param index   position of element
   */
  public void get(int index, NullableIntervalDayHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    final int startIndex = index * TYPE_WIDTH;
    holder.isSet = 1;
    holder.days = valueBuffer.getInt(startIndex);
    holder.milliseconds = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index   position of element
   * @return element at given index
   */
  public Duration getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      final int startIndex = index * TYPE_WIDTH;
      final int days = valueBuffer.getInt(startIndex);
      final int milliseconds = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);
      return Duration.ofDays(days).plusMillis(milliseconds);
    }
  }

  /**
   * Get the Interval value at a given index as a {@link StringBuilder} object.
   *
   * @param index position of the element
   * @return String Builder object with Interval value as
   *         [days, hours, minutes, seconds, millis]
   */
  public StringBuilder getAsStringBuilder(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return getAsStringBuilderHelper(index);
    }
  }

  private StringBuilder getAsStringBuilderHelper(int index) {
    final int startIndex = index * TYPE_WIDTH;

    final int days = valueBuffer.getInt(startIndex);
    int millis = valueBuffer.getInt(startIndex + MILLISECOND_OFFSET);

    final int hours = millis / (org.apache.arrow.vector.util.DateUtility.hoursToMillis);
    millis = millis % (org.apache.arrow.vector.util.DateUtility.hoursToMillis);

    final int minutes = millis / (org.apache.arrow.vector.util.DateUtility.minutesToMillis);
    millis = millis % (org.apache.arrow.vector.util.DateUtility.minutesToMillis);

    final int seconds = millis / (org.apache.arrow.vector.util.DateUtility.secondsToMillis);
    millis = millis % (org.apache.arrow.vector.util.DateUtility.secondsToMillis);

    final String dayString = (Math.abs(days) == 1) ? " day " : " days ";

    return (new StringBuilder()
            .append(days).append(dayString)
            .append(hours).append(":")
            .append(minutes).append(":")
            .append(seconds).append(".")
            .append(millis));
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, IntervalDayVector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    from.valueBuffer.getBytes(fromIndex * TYPE_WIDTH, this.valueBuffer,
              thisIndex * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Same as {@link #copyFrom(int, int, IntervalDayVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, IntervalDayVector from) {
    handleSafe(thisIndex);
    copyFrom(fromIndex, thisIndex, from);
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
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setBytes(index * TYPE_WIDTH, value, 0, TYPE_WIDTH);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index          position of element
   * @param days           days for the interval
   * @param milliseconds   milliseconds for the interval
   */
  public void set(int index, int days, int milliseconds) {
    final int offsetIndex = index * TYPE_WIDTH;
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setInt(offsetIndex, days);
    valueBuffer.setInt((offsetIndex + MILLISECOND_OFFSET), milliseconds);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableIntervalDayHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      set(index, holder.days, holder.milliseconds);
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
  public void set(int index, IntervalDayHolder holder) {
    set(index, holder.days, holder.milliseconds);
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
   * Same as {@link #set(int, int, int)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index          position of element
   * @param days           days for the interval
   * @param milliseconds   milliseconds for the interval
   */
  public void setSafe(int index, int days, int milliseconds) {
    handleSafe(index);
    set(index, days, milliseconds);
  }

  /**
   * Same as {@link #set(int, NullableIntervalDayHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableIntervalDayHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, IntervalDayHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, IntervalDayHolder holder) {
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
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param days days component of interval
   * @param milliseconds millisecond component of interval
   */
  public void set(int index, int isSet, int days, int milliseconds) {
    if (isSet > 0) {
      set(index, days, milliseconds);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int, int, int)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param days days component of interval
   * @param milliseconds millisecond component of interval
   */
  public void setSafe(int index, int isSet, int days, int milliseconds) {
    handleSafe(index);
    set(index, isSet, days, milliseconds);
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |                      vector transfer                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Construct a TransferPair comprising of this and and a target vector of
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
    return new TransferImpl((IntervalDayVector) to);
  }

  private class TransferImpl implements TransferPair {
    IntervalDayVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new IntervalDayVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(IntervalDayVector to) {
      this.to = to;
    }

    @Override
    public IntervalDayVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, IntervalDayVector.this);
    }
  }
}
