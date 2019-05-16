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

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.time.Duration;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.DurationReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.DurationHolder;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * DurationVector implements a fixed width vector (8 bytes) of
 * a configurable TimeUnit granularity duration values which could be null.
 * A validity buffer (bit vector) is maintained to track which elements in the
 * vector are null.
 */
public class DurationVector extends BaseFixedWidthVector {
  private static final byte TYPE_WIDTH = 8;
  private final FieldReader reader;
  private final TimeUnit unit;

  /**
   * Instantiate a DurationVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public DurationVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator, fieldType, TYPE_WIDTH);
    reader = new DurationReaderImpl(DurationVector.this);
    this.unit =  ((ArrowType.Duration)fieldType.getType()).getUnit();

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
   * @return {@link MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.DURATION;
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
   * <p>This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index  position of the element.
   * @return value stored at the index.
   */
  public static long get(final ArrowBuf buffer, final int index) {
    return buffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public ArrowBuf get(int index) throws IllegalStateException {
    if (isSet(index) == 0) {
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
  public void get(int index, NullableDurationHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = get(valueBuffer, index);
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
      final long value =  get(valueBuffer, index);
      return toDuration(value, unit);
    }
  }

  /**
   * Converts the given value and unit to the appropriate {@link Duration}.
   */
  public static Duration toDuration(long value, TimeUnit unit) {
    switch (unit) {
      case SECOND:
        return Duration.ofSeconds(value);
      case MILLISECOND:
        return Duration.ofMillis(value);
      case NANOSECOND:
        return Duration.ofNanos(value);
      case MICROSECOND:
        return Duration.ofNanos(MICROSECONDS.toNanos(value));
      default:
        throw new IllegalArgumentException("Unknown timeunit: " + unit);
    }
  }

  /**
   * Get the Interval value at a given index as a {@link StringBuilder} object.
   *
   * @param index position of the element
   * @return String Builder object with Interval in java.time.Duration format.
   */
  public StringBuilder getAsStringBuilder(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return getAsStringBuilderHelper(index);
    }
  }

  private StringBuilder getAsStringBuilderHelper(int index) {
    return new StringBuilder(getObject(index).toString());
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, DurationVector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    from.valueBuffer.getBytes(fromIndex * TYPE_WIDTH, this.valueBuffer,
              thisIndex * TYPE_WIDTH, TYPE_WIDTH);
  }

  /**
   * Same as {@link #copyFrom(int, int, DurationVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, DurationVector from) {
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
   * @param value   The duration value (in the timeunit associated with this vector)
   */
  public void set(int index, long value) {
    final int offsetIndex = index * TYPE_WIDTH;
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    valueBuffer.setLong(offsetIndex, value);
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void set(int index, NullableDurationHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      set(index, holder.value);
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
  public void set(int index, DurationHolder holder) {
    set(index, holder.value);
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
   * Same as {@link #set(int, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index          position of element
   * @param value   duration in the time unit this vector was constructed with
   */
  public void setSafe(int index, long value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableDurationHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  nullable data holder for value of element
   */
  public void setSafe(int index, NullableDurationHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, DurationHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param holder  data holder for value of element
   */
  public void setSafe(int index, DurationHolder holder) {
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
   * @param value The duration value (in the TimeUnit associated with this vector).
   */
  public void set(int index, int isSet, long value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int, long)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value The duration value (in the timeunit associated with this vector)
   */
  public void setSafe(int index, int isSet, long value) {
    handleSafe(index);
    set(index, isSet, value);
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
    return new TransferImpl((DurationVector) to);
  }

  private class TransferImpl implements TransferPair {
    DurationVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new DurationVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(DurationVector to) {
      this.to = to;
    }

    @Override
    public DurationVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, DurationVector.this);
    }
  }
}
