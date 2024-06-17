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
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.impl.FixedSizeBinaryReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.FixedSizeBinaryHolder;
import org.apache.arrow.vector.holders.NullableFixedSizeBinaryHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.validate.ValidateUtil;

/**
 * FixedSizeBinaryVector implements a fixed width vector of binary values which could be null. A
 * validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public class FixedSizeBinaryVector extends BaseFixedWidthVector
    implements ValueIterableVector<byte[]> {
  private final int byteWidth;

  /**
   * Instantiate a FixedSizeBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   * @param byteWidth byte width of the binary values
   */
  public FixedSizeBinaryVector(String name, BufferAllocator allocator, int byteWidth) {
    this(name, FieldType.nullable(new FixedSizeBinary(byteWidth)), allocator);
  }

  /**
   * Instantiate a FixedSizeBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public FixedSizeBinaryVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a FixedSizeBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public FixedSizeBinaryVector(Field field, BufferAllocator allocator) {
    super(field, allocator, ((FixedSizeBinary) field.getFieldType().getType()).getByteWidth());
    byteWidth = ((FixedSizeBinary) field.getFieldType().getType()).getByteWidth();
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new FixedSizeBinaryReaderImpl(FixedSizeBinaryVector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.FIXEDSIZEBINARY;
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
  public byte[] get(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }
    final byte[] dst = new byte[byteWidth];
    valueBuffer.getBytes((long) index * byteWidth, dst, 0, byteWidth);
    return dst;
  }

  /**
   * Read the value at the given position to the given output buffer. The caller is responsible for
   * checking for nullity first.
   *
   * @param index position of element.
   * @param buffer the buffer to write into.
   */
  public void read(int index, ReusableBuffer<?> buffer) {
    final int startOffset = index * byteWidth;
    buffer.set(valueBuffer, startOffset, byteWidth);
  }

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   * @param holder nullable holder to carry the buffer
   */
  public void get(int index, NullableFixedSizeBinaryHolder holder) {
    assert index >= 0;
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.buffer = valueBuffer.slice((long) index * byteWidth, byteWidth);
    holder.byteWidth = byteWidth;
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public byte[] getObject(int index) {
    return get(index);
  }

  public int getByteWidth() {
    return byteWidth;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /** Sets the value at index to the provided one. */
  public void set(int index, byte[] value) {
    assert index >= 0;
    Preconditions.checkNotNull(value, "expecting a valid byte array");
    assert byteWidth <= value.length;
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * byteWidth, value, 0, byteWidth);
  }

  /**
   * Same as {@link #set(int, byte[])} but reallocates if <code>index</code> is larger than
   * capacity.
   */
  public void setSafe(int index, byte[] value) {
    handleSafe(index);
    set(index, value);
  }

  /** Sets the value if isSet is positive, otherwise sets the index to null/invalid. */
  public void set(int index, int isSet, byte[] value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  public void setSafe(int index, int isSet, byte[] value) {
    handleSafe(index);
    set(index, isSet, value);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param buffer ArrowBuf containing binary value.
   */
  public void set(int index, ArrowBuf buffer) {
    assert index >= 0;
    assert byteWidth <= buffer.capacity();
    BitVectorHelper.setBit(validityBuffer, index);
    valueBuffer.setBytes((long) index * byteWidth, buffer, 0, byteWidth);
  }

  /**
   * Same as {@link #set(int, ArrowBuf)} except that it handles the case when index is greater than
   * or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param buffer ArrowBuf containing binary value.
   */
  public void setSafe(int index, ArrowBuf buffer) {
    handleSafe(index);
    set(index, buffer);
  }

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param buffer ArrowBuf containing binary value.
   */
  public void set(int index, int isSet, ArrowBuf buffer) {
    if (isSet > 0) {
      set(index, buffer);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, ArrowBuf)} except that it handles the case when index is greater than
   * or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param buffer ArrowBuf containing binary value.
   */
  public void setSafe(int index, int isSet, ArrowBuf buffer) {
    handleSafe(index);
    set(index, isSet, buffer);
  }

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, FixedSizeBinaryHolder holder) {
    if (this.byteWidth != holder.byteWidth) {
      throw new IllegalArgumentException(
          String.format(
              "holder.byteWidth: %d not equal to vector byteWidth: %d",
              holder.byteWidth, this.byteWidth));
    }
    set(index, holder.buffer);
  }

  /**
   * Same as {@link #set(int, FixedSizeBinaryHolder)} except that it handles the case where index
   * and length of new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, FixedSizeBinaryHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, NullableFixedSizeBinaryHolder holder) {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException("holder has a negative isSet value");
    } else if (this.byteWidth != holder.byteWidth) {
      throw new IllegalArgumentException(
          String.format(
              "holder.byteWidth: %d not equal to vector byteWidth: %d",
              holder.byteWidth, this.byteWidth));
    } else if (holder.isSet > 0) {
      set(index, holder.buffer);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, NullableFixedSizeBinaryHolder)} except that it handles the case where
   * index and length of new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, NullableFixedSizeBinaryHolder holder) {
    handleSafe(index);
    set(index, holder);
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
  public static byte[] get(final ArrowBuf buffer, final int index, final int byteWidth) {
    final byte[] dst = new byte[byteWidth];
    buffer.getBytes((long) index * byteWidth, dst, 0, byteWidth);
    return dst;
  }

  @Override
  public void validateScalars() {
    for (int i = 0; i < getValueCount(); ++i) {
      byte[] value = get(i);
      if (value != null) {
        ValidateUtil.validateOrThrow(
            value.length == byteWidth,
            "Invalid value for FixedSizeBinaryVector at position "
                + i
                + ". The length was "
                + value.length
                + " but the length of each element should be "
                + byteWidth
                + ".");
      }
    }
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
    return new TransferImpl((FixedSizeBinaryVector) to);
  }

  private class TransferImpl implements TransferPair {
    FixedSizeBinaryVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new FixedSizeBinaryVector(ref, allocator, FixedSizeBinaryVector.this.byteWidth);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new FixedSizeBinaryVector(field, allocator);
    }

    public TransferImpl(FixedSizeBinaryVector to) {
      this.to = to;
    }

    @Override
    public FixedSizeBinaryVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, FixedSizeBinaryVector.this);
    }
  }
}
