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

import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;
import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.impl.BitReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

/**
 * BitVector implements a fixed width (1 bit) vector of boolean values which could be null. Each
 * value in the vector corresponds to a single bit in the underlying data stream backing the vector.
 */
public final class BitVector extends BaseFixedWidthVector implements ValueIterableVector<Boolean> {

  private static final int HASH_CODE_FOR_ZERO = 17;

  private static final int HASH_CODE_FOR_ONE = 19;

  /**
   * Instantiate a BitVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public BitVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.BIT.getType()), allocator);
  }

  /**
   * Instantiate a BitVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public BitVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a BitVector. This doesn't allocate any memory for the data in vector.
   *
   * @param field the Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public BitVector(Field field, BufferAllocator allocator) {
    super(field, allocator, 0);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new BitReaderImpl(BitVector.this);
  }

  /**
   * Get minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.BIT;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't allocate any memory for
   * the vector.
   *
   * @param valueCount desired number of elements in the vector
   */
  @Override
  public void setInitialCapacity(int valueCount) {
    final int size = getValidityBufferSizeFromCount(valueCount);
    if (size * 2L > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }
    lastValueCapacity = valueCount;
  }

  @Override
  protected int getValueBufferValueCapacity() {
    return capAtMaxInt(valueBuffer.capacity() * 8);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   *
   * @param count desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int count) {
    if (count == 0) {
      return 0;
    }
    return 2 * getValidityBufferSizeFromCount(count);
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this vector.
   *
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    return getBufferSizeFor(valueCount);
  }

  /**
   * Slice this vector at desired index and length and transfer the corresponding data to the target
   * vector.
   *
   * @param startIndex start position of the split in source vector.
   * @param length length of the split.
   * @param target destination vector
   */
  @Override
  public void splitAndTransferTo(int startIndex, int length, BaseFixedWidthVector target) {
    Preconditions.checkArgument(
        startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
        "Invalid parameters startIndex: %s, length: %s for valueCount: %s",
        startIndex,
        length,
        valueCount);
    compareTypes(target, "splitAndTransferTo");
    target.clear();
    target.validityBuffer =
        splitAndTransferBuffer(startIndex, length, validityBuffer, target.validityBuffer);
    target.valueBuffer =
        splitAndTransferBuffer(startIndex, length, valueBuffer, target.valueBuffer);
    target.refreshValueCapacity();

    target.setValueCount(length);
  }

  private ArrowBuf splitAndTransferBuffer(
      int startIndex, int length, ArrowBuf sourceBuffer, ArrowBuf destBuffer) {
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = getValidityBufferSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        /* slice */
        if (destBuffer != null) {
          destBuffer.getReferenceManager().release();
        }
        destBuffer = sourceBuffer.slice(firstByteSource, byteSizeTarget);
        destBuffer.getReferenceManager().retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        destBuffer = allocator.buffer(byteSizeTarget);
        destBuffer.readerIndex(0);
        destBuffer.setZero(0, destBuffer.capacity());

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(sourceBuffer, firstByteSource + i, offset);
          byte b2 =
              BitVectorHelper.getBitsFromNextByte(sourceBuffer, firstByteSource + i + 1, offset);

          destBuffer.setByte(i, (b1 + b2));
        }

        /* Copying the last piece is done in the following manner:
         * if the source vector has 1 or more bytes remaining, we copy
         * the last piece as a byte formed by shifting data
         * from the current byte and the next byte.
         *
         * if the source vector has no more bytes remaining
         * (we are at the last byte), we copy the last piece as a byte
         * by shifting data from the current byte.
         */
        if ((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(
                  sourceBuffer, firstByteSource + byteSizeTarget - 1, offset);
          byte b2 =
              BitVectorHelper.getBitsFromNextByte(
                  sourceBuffer, firstByteSource + byteSizeTarget, offset);

          destBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 =
              BitVectorHelper.getBitsFromCurrentByte(
                  sourceBuffer, firstByteSource + byteSizeTarget - 1, offset);
          destBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }

    return destBuffer;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  private int getBit(int index) {
    final int byteIndex = index >> 3;
    final byte b = valueBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Get the element at the given index from the vector.
   *
   * @param index position of element
   * @return element at given index
   */
  public int get(int index) throws IllegalStateException {
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    return getBit(index);
  }

  /**
   * Get the element at the given index from the vector and sets the state in holder. If element at
   * given index is null, holder.isSet will be zero.
   *
   * @param index position of element
   */
  public void get(int index, NullableBitHolder holder) {
    if (isSet(index) == 0) {
      holder.isSet = 0;
      return;
    }
    holder.isSet = 1;
    holder.value = getBit(index);
  }

  /**
   * Same as {@link #get(int)}.
   *
   * @param index position of element
   * @return element at given index
   */
  @Override
  public Boolean getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return getBit(index) != 0;
    }
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular position in this
   * vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    boolean fromIsSet = BitVectorHelper.get(from.getValidityBuffer(), fromIndex) != 0;
    if (fromIsSet) {
      BitVectorHelper.setBit(validityBuffer, thisIndex);
      BitVectorHelper.setValidityBit(valueBuffer, thisIndex, ((BitVector) from).getBit(fromIndex));
    } else {
      BitVectorHelper.unsetBit(validityBuffer, thisIndex);
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Set the element at the given index to the given value.
   *
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, int value) {
    BitVectorHelper.setBit(validityBuffer, index);
    if (value != 0) {
      BitVectorHelper.setBit(valueBuffer, index);
    } else {
      BitVectorHelper.unsetBit(valueBuffer, index);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder. If the value in holder is
   * not indicated as set, element in the at the given index will be null.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void set(int index, NullableBitHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setBit(validityBuffer, index);
      if (holder.value != 0) {
        BitVectorHelper.setBit(valueBuffer, index);
      } else {
        BitVectorHelper.unsetBit(valueBuffer, index);
      }
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
  public void set(int index, BitHolder holder) {
    BitVectorHelper.setBit(validityBuffer, index);
    if (holder.value != 0) {
      BitVectorHelper.setBit(valueBuffer, index);
    } else {
      BitVectorHelper.unsetBit(valueBuffer, index);
    }
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
   * Same as {@link #set(int, NullableBitHolder)} except that it handles the case when index is
   * greater than or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableBitHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, BitHolder)} except that it handles the case when index is greater than
   * or equal to existing value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, BitHolder holder) {
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
  public void set(int index, int isSet, int value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.unsetBit(validityBuffer, index);
    }
  }

  /**
   * Same as {@link #set(int, int, int)} except that it handles the case when index is greater than
   * or equal to current value capacity of the vector.
   *
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param value element value
   */
  public void setSafe(int index, int isSet, int value) {
    handleSafe(index);
    set(index, isSet, value);
  }

  /**
   * Set the element at the given index to one.
   *
   * @param index position of element
   */
  public void setToOne(int index) {
    BitVectorHelper.setBit(validityBuffer, index);
    BitVectorHelper.setBit(valueBuffer, index);
  }

  /**
   * Same as {@link #setToOne(int)} except that it handles the case when index is greater than or
   * equal to current value capacity of the vector.
   *
   * @param index position of the element
   */
  public void setSafeToOne(int index) {
    handleSafe(index);
    setToOne(index);
  }

  @Override
  public ArrowBufPointer getDataPointer(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBufPointer getDataPointer(int index, ArrowBufPointer reuse) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode(int index) {
    if (isNull(index)) {
      return ArrowBufPointer.NULL_HASH_CODE;
    } else {
      if (get(index) == 0) {
        return HASH_CODE_FOR_ZERO;
      } else {
        return HASH_CODE_FOR_ONE;
      }
    }
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return hashCode(index);
  }

  /**
   * Set count bits to 1 in data starting at firstBitIndex.
   *
   * @param firstBitIndex the index of the first bit to set
   * @param count the number of bits to set
   */
  public void setRangeToOne(int firstBitIndex, int count) {
    int startByteIndex = BitVectorHelper.byteIndex(firstBitIndex);
    final int lastBitIndex = firstBitIndex + count;
    final int endByteIndex = BitVectorHelper.byteIndex(lastBitIndex);
    final int startByteBitIndex = BitVectorHelper.bitIndex(firstBitIndex);
    final int endBytebitIndex = BitVectorHelper.bitIndex(lastBitIndex);
    if (count < 8 && startByteIndex == endByteIndex) {
      // handles the case where we don't have a first and a last byte
      byte bitMask = 0;
      for (int i = startByteBitIndex; i < endBytebitIndex; ++i) {
        bitMask |= (byte) (1L << i);
      }
      BitVectorHelper.setBitMaskedByte(validityBuffer, startByteIndex, bitMask);
      BitVectorHelper.setBitMaskedByte(valueBuffer, startByteIndex, bitMask);
    } else {
      // fill in first byte (if it's not full)
      if (startByteBitIndex != 0) {
        final byte bitMask = (byte) (0xFFL << startByteBitIndex);
        BitVectorHelper.setBitMaskedByte(validityBuffer, startByteIndex, bitMask);
        BitVectorHelper.setBitMaskedByte(valueBuffer, startByteIndex, bitMask);
        ++startByteIndex;
      }

      // fill in one full byte at a time
      validityBuffer.setOne(startByteIndex, endByteIndex - startByteIndex);
      valueBuffer.setOne(startByteIndex, endByteIndex - startByteIndex);

      // fill in the last byte (if it's not full)
      if (endBytebitIndex != 0) {
        final int byteIndex = BitVectorHelper.byteIndex(lastBitIndex - endBytebitIndex);
        final byte bitMask = (byte) (0xFFL >>> ((8 - endBytebitIndex) & 7));
        BitVectorHelper.setBitMaskedByte(validityBuffer, byteIndex, bitMask);
        BitVectorHelper.setBitMaskedByte(valueBuffer, byteIndex, bitMask);
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
    return new TransferImpl((BitVector) to);
  }

  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new BitVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new BitVector(field, allocator);
    }

    public TransferImpl(BitVector to) {
      this.to = to;
    }

    @Override
    public BitVector getTo() {
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
      to.copyFromSafe(fromIndex, toIndex, BitVector.this);
    }
  }
}
