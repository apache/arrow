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
import org.apache.arrow.vector.complex.impl.BitReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * BitVector implements a fixed width (1 bit) vector of
 * boolean values which could be null. Each value in the vector corresponds
 * to a single bit in the underlying data stream backing the vector.
 */
public class BitVector extends BaseFixedWidthVector {
  private final FieldReader reader;

  /**
   * Instantiate a BitVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name      name of the vector
   * @param allocator allocator for memory management.
   */
  public BitVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.BIT.getType()), allocator);
  }

  /**
   * Instantiate a BitVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name      name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public BitVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, allocator, fieldType, 0);
    reader = new BitReaderImpl(BitVector.this);
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
    return MinorType.BIT;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   *
   * @param valueCount desired number of elements in the vector
   */
  @Override
  public void setInitialCapacity(int valueCount) {
    final int size = getValidityBufferSizeFromCount(valueCount);
    if (size * 2 > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }
    lastValueCapacity = valueCount;
  }

  /**
   * Get the current value capacity for the vector
   *
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    return (int) (validityBuffer.capacity() * 8L);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   *
   * @param count desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds
   *         a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int count) {
    if (count == 0) {
      return 0;
    }
    return 2 * getValidityBufferSizeFromCount(count);
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this
   * vector
   *
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    return getBufferSizeFor(valueCount);
  }

  /**
   * Slice this vector at desired index and length and transfer the
   * corresponding data to the target vector.
   *
   * @param startIndex start position of the split in source vector.
   * @param length     length of the split.
   * @param target     destination vector
   */
  public void splitAndTransferTo(int startIndex, int length, BaseFixedWidthVector target) {
    compareTypes(target, "splitAndTransferTo");
    target.clear();
    target.validityBuffer = splitAndTransferBuffer(startIndex, length, target,
            validityBuffer, target.validityBuffer);
    target.valueBuffer = splitAndTransferBuffer(startIndex, length, target,
            valueBuffer, target.valueBuffer);

    target.setValueCount(length);
  }

  private ArrowBuf splitAndTransferBuffer(
      int startIndex,
      int length,
      BaseFixedWidthVector target,
      ArrowBuf sourceBuffer,
      ArrowBuf destBuffer) {
    assert startIndex + length <= valueCount;
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
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer, firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(sourceBuffer, firstByteSource + i + 1, offset);

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
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(sourceBuffer,
                  firstByteSource + byteSizeTarget, offset);

          destBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(sourceBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
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
   * Get the element at the given index from the vector and
   * sets the state in holder. If element at given index
   * is null, holder.isSet will be zero.
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
  public Boolean getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    } else {
      return new Boolean(getBit(index) != 0);
    }
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from      source vector
   */
  public void copyFrom(int fromIndex, int thisIndex, BitVector from) {
    BitVectorHelper.setValidityBit(validityBuffer, thisIndex, from.isSet(fromIndex));
    BitVectorHelper.setValidityBit(valueBuffer, thisIndex, from.getBit(fromIndex));
  }

  /**
   * Same as {@link #copyFrom(int, int, BitVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from      source vector
   */
  public void copyFromSafe(int fromIndex, int thisIndex, BitVector from) {
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
   * @param index position of element
   * @param value value of element
   */
  public void set(int index, int value) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    if (value != 0) {
      BitVectorHelper.setValidityBitToOne(valueBuffer, index);
    } else {
      BitVectorHelper.setValidityBit(valueBuffer, index, 0);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   * If the value in holder is not indicated as set, element in the
   * at the given index will be null.
   *
   * @param index  position of element
   * @param holder nullable data holder for value of element
   */
  public void set(int index, NullableBitHolder holder) throws IllegalArgumentException {
    if (holder.isSet < 0) {
      throw new IllegalArgumentException();
    } else if (holder.isSet > 0) {
      BitVectorHelper.setValidityBitToOne(validityBuffer, index);
      if (holder.value != 0) {
        BitVectorHelper.setValidityBitToOne(valueBuffer, index);
      } else {
        BitVectorHelper.setValidityBit(valueBuffer, index, 0);
      }
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Set the element at the given index to the value set in data holder.
   *
   * @param index  position of element
   * @param holder data holder for value of element
   */
  public void set(int index, BitHolder holder) {
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    if (holder.value != 0) {
      BitVectorHelper.setValidityBitToOne(valueBuffer, index);
    } else {
      BitVectorHelper.setValidityBit(valueBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index position of element
   * @param value value of element
   */
  public void setSafe(int index, int value) {
    handleSafe(index);
    set(index, value);
  }

  /**
   * Same as {@link #set(int, NullableBitHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index  position of element
   * @param holder nullable data holder for value of element
   */
  public void setSafe(int index, NullableBitHolder holder) throws IllegalArgumentException {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Same as {@link #set(int, BitHolder)} except that it handles the
   * case when index is greater than or equal to existing
   * value capacity {@link #getValueCapacity()}.
   *
   * @param index  position of element
   * @param holder data holder for value of element
   */
  public void setSafe(int index, BitHolder holder) {
    handleSafe(index);
    set(index, holder);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index position of element
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
   * @param value element value
   */
  public void set(int index, int isSet, int value) {
    if (isSet > 0) {
      set(index, value);
    } else {
      BitVectorHelper.setValidityBit(validityBuffer, index, 0);
    }
  }

  /**
   * Same as {@link #set(int, int, int)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
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
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    BitVectorHelper.setValidityBitToOne(valueBuffer, index);
  }

  /**
   * Same as {@link #setToOne(int)} except that it handles the case when
   * index is greater than or equal to current value capacity of the vector.
   *
   * @param index position of the element
   */
  public void setSafeToOne(int index) {
    handleSafe(index);
    setToOne(index);
  }

  /**
   * Set count bits to 1 in data starting at firstBitIndex.
   *
   * @param firstBitIndex the index of the first bit to set
   * @param count         the number of bits to set
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
      for (int i = startByteIndex; i < endByteIndex; i++) {
        validityBuffer.setByte(i, 0xFF);
        valueBuffer.setByte(i, 0xFF);
      }

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
   * Construct a TransferPair comprising of this and and a target vector of
   * the same type.
   *
   * @param ref       name of the target vector
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
    return new TransferImpl((BitVector) to);
  }

  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new BitVector(ref, field.getFieldType(), allocator);
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
