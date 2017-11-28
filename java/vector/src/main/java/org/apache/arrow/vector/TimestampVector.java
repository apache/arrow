/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import com.google.common.base.Preconditions;
import org.apache.arrow.vector.types.TimeUnit;
import org.joda.time.DateTimeZone;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.impl.TimestampReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableTimestampHolder;
import org.apache.arrow.vector.holders.TimestampHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.joda.time.LocalDateTime;


/**
 * TimestampVector is an abstract interface for fixed width vector (8 bytes)
 * of timestamp values which could be null. A validity buffer (bit vector) is
 * maintained to track which elements in the vector are null.
 */
public class TimestampVector extends BaseFixedWidthVector {
  protected static final byte TYPE_WIDTH = 8;

  private final FieldReader reader;
  private final TimeUnit unit;
  private final DateTimeZone timezone;

  /**
   * Instantiate a TimestampVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   * @param unit time unit
   * @param timezone time zone
   */
  public TimestampVector(String name, BufferAllocator allocator, TimeUnit unit, String timezone) {
    this(name, FieldType.nullable(new ArrowType.Timestamp(unit, timezone)), allocator);
  }
  /**
   * Instantiate a TimestampVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public TimestampVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator, fieldType, TYPE_WIDTH);

    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) fieldType.getType();

    this.reader = new TimestampReaderImpl(this);
    this.unit = arrowType.getUnit();
    // TODO: Decide if this is right - This matches the current behavior
    this.timezone = arrowType.getTimezone() == null ?
            DateTimeZone.UTC : DateTimeZone.forID(arrowType.getTimezone());
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
  public Types.MinorType getMinorType() {
    return Types.MinorType.TIMESTAMP;
  }

  /******************************************************************
   *                                                                *
   *          vector value retrieval methods                        *
   *                                                                *
   ******************************************************************/

  /**
   * Get the element at the given index from the vector.
   *
   * @param index   position of element
   * @return element at given index
   */
  public long get(int index) throws IllegalStateException {
    if (isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return valueBuffer.getLong(index * TYPE_WIDTH);
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, TimestampVector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    final long value = from.valueBuffer.getLong(fromIndex * TYPE_WIDTH);
    valueBuffer.setLong(thisIndex * TYPE_WIDTH, value);
  }

  /**
   * Same as {@link #copyFromSafe(int, int, TimestampVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, TimestampVector from) {
    handleSafe(thisIndex);
    copyFrom(fromIndex, thisIndex, from);
  }


  /******************************************************************
   *                                                                *
   *          vector value setter methods                           *
   *                                                                *
   ******************************************************************/


  protected void setValue(int index, long value) {
    valueBuffer.setLong(index * TYPE_WIDTH, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void set(int index, long value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, value);
  }

  public void set(int index, NullableTimestampHolder holder) {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      setValue(index, holder.value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  public void set(int index, TimestampHolder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setValue(index, holder.value);
  }

  /**
   * Same as {@link #set(int, long)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index   position of element
   * @param value   value of element
   */
  public void setSafe(int index, long value) {
    handleSafe(index);
    set(index, value);
  }

  public void setSafe(int index, NullableTimestampHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  public void setSafe(int index, TimestampHolder holder) {
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
      /* not really needed to set the bit to 0 as long as
       * the buffer always starts from 0.
       */
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
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
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setSafe(int index, int isSet, long value) {
    handleSafe(index);
    set(index, isSet, value);
  }

  /**
   * Given a data buffer, get the value stored at a particular position
   * in the vector.
   *
   * This method should not be used externally.
   *
   * @param buffer data buffer
   * @param index position of the element.
   * @return value stored at the index.
   */
  public static long get(final ArrowBuf buffer, final int index) {
    return buffer.getLong(index * TYPE_WIDTH);
  }

  public void get(int index, NullableTimestampHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = valueBuffer.getLong(index * TYPE_WIDTH);
  }

  @Override
  public LocalDateTime getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      long millis = unit.toMillis(get(index));
      LocalDateTime date = new LocalDateTime(millis, timezone);
      return date;
    }
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    TimestampVector to = new TimestampVector(ref, field.getFieldType(), allocator);
    return new TransferImpl(to);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((TimestampVector) to);
  }


  /******************************************************************
   *                                                                *
   *                      vector transfer                           *
   *                                                                *
   ******************************************************************/


  public class TransferImpl implements TransferPair {
    TimestampVector to;

    public TransferImpl(TimestampVector to) {
      Preconditions.checkArgument(unit == to.unit);
      this.to = to;
    }

    @Override
    public TimestampVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, TimestampVector.this);
    }
  }
}