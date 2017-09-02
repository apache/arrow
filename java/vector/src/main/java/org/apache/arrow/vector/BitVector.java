/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.BitHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * Bit implements a vector of bit-width values. Elements in the vector are accessed by position from the logical start
 * of the vector. The width of each element is 1 bit. The equivalent Java primitive is an int containing the value '0'
 * or '1'.
 */
public final class BitVector extends BaseDataValueVector implements FixedWidthVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BitVector.class);

  private final Accessor accessor = new Accessor();
  private final Mutator mutator = new Mutator();

  int valueCount;
  private int allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
  private int allocationMonitor = 0;

  public BitVector(String name, BufferAllocator allocator) {
    super(name, allocator);
  }

  @Override
  public void load(ArrowFieldNode fieldNode, ArrowBuf data) {
    // When the vector is all nulls or all defined, the content of the buffer can be omitted
    if (data.readableBytes() == 0 && fieldNode.getLength() != 0) {
      int count = fieldNode.getLength();
      allocateNew(count);
      int n = getSizeFromCount(count);
      if (fieldNode.getNullCount() == 0) {
        // all defined
        // create an all 1s buffer
        // set full bytes
        int fullBytesCount = count / 8;
        for (int i = 0; i < fullBytesCount; ++i) {
          this.data.setByte(i, 0xFF);
        }
        int remainder = count % 8;
        // set remaining bits
        if (remainder > 0) {
          byte bitMask = (byte) (0xFFL >>> ((8 - remainder) & 7));
          this.data.setByte(fullBytesCount, bitMask);
        }
      } else if (fieldNode.getNullCount() == fieldNode.getLength()) {
        // all null
        // create an all 0s buffer
        zeroVector();
      } else {
        throw new IllegalArgumentException("The buffer can be empty only if there's no data or it's all null or all defined");
      }
      this.data.writerIndex(n);
    } else {
      super.load(fieldNode, data);
    }
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public Field getField() {
    throw new UnsupportedOperationException("internal vector");
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.BIT;
  }

  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException("internal vector");
  }

  @Override
  public int getBufferSize() {
    return getSizeFromCount(valueCount);
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    return getSizeFromCount(valueCount);
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    /* this operation is not supported for non-nullable vectors */
    throw new  UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    /* we are not throwing away getBuffer() of BaseDataValueVector so use it wherever applicable */
    return getBuffer();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    /* this operation is not supported for fixed-width vectors */
    throw new UnsupportedOperationException();
  }

  int getSizeFromCount(int valueCount) {
    return (int) Math.ceil(valueCount / 8.0);
  }

  @Override
  public int getValueCapacity() {
    return (int) Math.min((long) Integer.MAX_VALUE, data.capacity() * 8L);
  }

  private int getByteIndex(int index) {
    return (int) Math.floor(index / 8.0);
  }

  @Override
  public void setInitialCapacity(final int valueCount) {
    allocationSizeInBytes = getSizeFromCount(valueCount);
  }

  @Override
  public void allocateNew() {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException();
    }
  }

  @Override
  public boolean allocateNewSafe() {
    long curAllocationSize = allocationSizeInBytes;
    if (allocationMonitor > 10) {
      curAllocationSize = Math.max(8, allocationSizeInBytes / 2);
      allocationMonitor = 0;
    } else if (allocationMonitor < -2) {
      curAllocationSize = allocationSizeInBytes * 2L;
      allocationMonitor = 0;
    }

    try {
      allocateBytes(curAllocationSize);
    } catch (OutOfMemoryException ex) {
      return false;
    }
    return true;
  }

  @Override
  public void reset() {
    valueCount = 0;
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION;
    allocationMonitor = 0;
    zeroVector();
    super.reset();
  }

  /**
   * Allocate a new memory space for this vector. Must be called prior to using the ValueVector.
   *
   * @param valueCount The number of values which can be contained within this vector.
   */
  @Override
  public void allocateNew(int valueCount) {
    final int size = getSizeFromCount(valueCount);
    allocateBytes(size);
  }

  private void allocateBytes(final long size) {
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int) size;
    clear();
    data = allocator.buffer(curSize);
    zeroVector();
    allocationSizeInBytes = curSize;
  }

  /**
   * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's buffer with new buffer, and release old one
   */
  public void reAlloc() {
    final long newAllocationSize = allocationSizeInBytes * 2L;
    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed allocation size");
    }

    final int curSize = (int) newAllocationSize;
    final ArrowBuf newBuf = allocator.buffer(curSize);
    newBuf.setZero(0, newBuf.capacity());
    newBuf.setBytes(0, data, 0, data.capacity());
    data.release();
    data = newBuf;
    allocationSizeInBytes = curSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void zeroVector() {
    data.setZero(0, data.capacity());
  }

  public void copyFrom(int inIndex, int outIndex, BitVector from) {
    this.mutator.set(outIndex, from.accessor.get(inIndex));
  }

  public void copyFromSafe(int inIndex, int outIndex, BitVector from) {
    if (outIndex >= this.getValueCapacity()) {
      reAlloc();
    }
    copyFrom(inIndex, outIndex, from);
  }

  @Override
  public Mutator getMutator() {
    return new Mutator();
  }

  @Override
  public Accessor getAccessor() {
    return new Accessor();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return new TransferImpl(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((BitVector) to);
  }


  public void transferTo(BitVector target) {
    target.clear();
    target.data = data.transferOwnership(target.allocator).buffer;
    target.valueCount = valueCount;
    clear();
  }

  public void splitAndTransferTo(int startIndex, int length, BitVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = getByteIndex(startIndex);
    int lastByteSource = getByteIndex(valueCount - 1);
    int byteSizeTarget = getSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        target.clear();
        // slice
        if (target.data != null) {
          target.data.release();
        }
        target.data = data.slice(firstByteSource, byteSizeTarget);
        target.data.retain(1);
      }
      else {
        // Copy data
        // When the first bit starts from the middle of a byte (offset != 0), copy data from src BitVector.
        // Each byte in the target is composed by a part in i-th byte, another part in (i+1)-th byte.

        target.clear();
        target.allocateNew(byteSizeTarget * 8);

        // TODO maybe do this one word at a time, rather than byte?

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 = getBitsFromCurrentByte(this.data, firstByteSource + i, offset);
          byte b2 = getBitsFromNextByte(this.data, firstByteSource + i + 1, offset);

          target.data.setByte(i, (b1 + b2));
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
        if((firstByteSource + byteSizeTarget - 1) < lastByteSource) {
          byte b1 = getBitsFromCurrentByte(this.data, firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = getBitsFromNextByte(this.data, firstByteSource + byteSizeTarget, offset);

          target.data.setByte(byteSizeTarget - 1, b1 + b2);
        }
        else {
          byte b1 = getBitsFromCurrentByte(this.data, firstByteSource + byteSizeTarget - 1, offset);

          target.data.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
    target.getMutator().setValueCount(length);
  }

  private static byte getBitsFromCurrentByte(ArrowBuf data, int index, int offset) {
    return (byte)((data.getByte(index) & 0xFF) >>> offset);
  }

  private static byte getBitsFromNextByte(ArrowBuf data, int index, int offset) {
    return (byte)((data.getByte(index) << (8 - offset)));
  }

  private class TransferImpl implements TransferPair {
    BitVector to;

    public TransferImpl(String name, BufferAllocator allocator) {
      this.to = new BitVector(name, allocator);
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

  private void decrementAllocationMonitor() {
    if (allocationMonitor > 0) {
      allocationMonitor = 0;
    }
    --allocationMonitor;
  }

  private void incrementAllocationMonitor() {
    ++allocationMonitor;
  }

  public class Accessor extends BaseAccessor {

    /**
     * Get the byte holding the desired bit, then mask all other bits. Iff the result is 0, the bit was not set.
     *
     * @param index position of the bit in the vector
     * @return 1 if set, otherwise 0
     */
    public final int get(int index) {
      int byteIndex = index >> 3;
      byte b = data.getByte(byteIndex);
      int bitIndex = index & 7;
      return Long.bitCount(b & (1L << bitIndex));
    }

    @Override
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public final Boolean getObject(int index) {
      return new Boolean(get(index) != 0);
    }

    @Override
    public final int getValueCount() {
      return valueCount;
    }

    public final void get(int index, BitHolder holder) {
      holder.value = get(index);
    }

    public final void get(int index, NullableBitHolder holder) {
      holder.isSet = 1;
      holder.value = get(index);
    }

    /**
     * Get the number nulls, this correspond to the number of bits set to 0 in the vector
     *
     * @return the number of bits set to 0
     */
    @Override
    public final int getNullCount() {
      int count = 0;
      int sizeInBytes = getSizeFromCount(valueCount);

      for (int i = 0; i < sizeInBytes; ++i) {
        byte byteValue = data.getByte(i);
        // Java uses two's complement binary representation, hence 11111111_b which is -1 when converted to Int
        // will have 32bits set to 1. Masking the MSB and then adding it back solves the issue.
        count += Integer.bitCount(byteValue & 0x7F) - (byteValue >> 7);
      }
      int nullCount = (sizeInBytes * 8) - count;
      // if the valueCount is not a multiple of 8, the bits on the right were counted as null bits
      int remainder = valueCount % 8;
      nullCount -= remainder == 0 ? 0 : 8 - remainder;
      return nullCount;
    }
  }

  /**
   * MutableBit implements a vector of bit-width values. Elements in the vector are accessed by position from the
   * logical start of the vector. Values should be pushed onto the vector sequentially, but may be randomly accessed.
   *
   * NB: this class is automatically generated from ValueVectorTypes.tdd using FreeMarker.
   */
  public class Mutator extends BaseMutator {

    private Mutator() {
    }

    /**
     * Set the bit at the given index to the specified value.
     *
     * @param index position of the bit to set
     * @param value value to set (either 1 or 0)
     */
    public final void set(int index, int value) {
      int byteIndex = byteIndex(index);
      int bitIndex = bitIndex(index);
      byte currentByte = data.getByte(byteIndex);
      byte bitMask = (byte) (1L << bitIndex);
      if (value != 0) {
        currentByte |= bitMask;
      } else {
        currentByte -= (bitMask & currentByte);
      }
      data.setByte(byteIndex, currentByte);
    }

    /**
     * Set the bit at the given index to 1.
     *
     * @param index position of the bit to set
     */
    public final void setToOne(int index) {
      int byteIndex = byteIndex(index);
      int bitIndex = bitIndex(index);
      byte currentByte = data.getByte(byteIndex);
      byte bitMask = (byte) (1L << bitIndex);
      currentByte |= bitMask;
      data.setByte(byteIndex, currentByte);
    }

    /**
     * set count bits to 1 in data starting at firstBitIndex
     *
     * @param firstBitIndex the index of the first bit to set
     * @param count         the number of bits to set
     */
    public void setRangeToOne(int firstBitIndex, int count) {
      int starByteIndex = byteIndex(firstBitIndex);
      final int lastBitIndex = firstBitIndex + count;
      final int endByteIndex = byteIndex(lastBitIndex);
      final int startByteBitIndex = bitIndex(firstBitIndex);
      final int endBytebitIndex = bitIndex(lastBitIndex);
      if (count < 8 && starByteIndex == endByteIndex) {
        // handles the case where we don't have a first and a last byte
        byte bitMask = 0;
        for (int i = startByteBitIndex; i < endBytebitIndex; ++i) {
          bitMask |= (byte) (1L << i);
        }
        byte currentByte = data.getByte(starByteIndex);
        currentByte |= bitMask;
        data.setByte(starByteIndex, currentByte);
      } else {
        // fill in first byte (if it's not full)
        if (startByteBitIndex != 0) {
          byte currentByte = data.getByte(starByteIndex);
          final byte bitMask = (byte) (0xFFL << startByteBitIndex);
          currentByte |= bitMask;
          data.setByte(starByteIndex, currentByte);
          ++starByteIndex;
        }

        // fill in one full byte at a time
        for (int i = starByteIndex; i < endByteIndex; i++) {
          data.setByte(i, 0xFF);
        }

        // fill in the last byte (if it's not full)
        if (endBytebitIndex != 0) {
          final int byteIndex = byteIndex(lastBitIndex - endBytebitIndex);
          byte currentByte = data.getByte(byteIndex);
          final byte bitMask = (byte) (0xFFL >>> ((8 - endBytebitIndex) & 7));
          currentByte |= bitMask;
          data.setByte(byteIndex, currentByte);
        }

      }
    }

    /**
     * @param absoluteBitIndex the index of the bit in the buffer
     * @return the index of the byte containing that bit
     */
    private int byteIndex(int absoluteBitIndex) {
      return absoluteBitIndex >> 3;
    }

    /**
     * @param absoluteBitIndex the index of the bit in the buffer
     * @return the index of the bit inside the byte
     */
    private int bitIndex(int absoluteBitIndex) {
      return absoluteBitIndex & 7;
    }

    public final void set(int index, BitHolder holder) {
      set(index, holder.value);
    }

    final void set(int index, NullableBitHolder holder) {
      set(index, holder.value);
    }

    public void setSafe(int index, int value) {
      while (index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, value);
    }

    public void setSafeToOne(int index) {
      while (index >= getValueCapacity()) {
        reAlloc();
      }
      setToOne(index);
    }

    public void setSafe(int index, BitHolder holder) {
      while (index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder.value);
    }

    public void setSafe(int index, NullableBitHolder holder) {
      while (index >= getValueCapacity()) {
        reAlloc();
      }
      set(index, holder.value);
    }

    @Override
    public final void setValueCount(int valueCount) {
      int currentValueCapacity = getValueCapacity();
      BitVector.this.valueCount = valueCount;
      int idx = getSizeFromCount(valueCount);
      while (valueCount > getValueCapacity()) {
        reAlloc();
      }
      if (valueCount > 0 && currentValueCapacity > valueCount * 2) {
        incrementAllocationMonitor();
      } else if (allocationMonitor > 0) {
        allocationMonitor = 0;
      }
      VectorTrimmer.trim(data, idx);
    }

    @Override
    public final void generateTestData(int values) {
      boolean even = true;
      for (int i = 0; i < values; i++, even = !even) {
        if (even) {
          set(i, 1);
        }
      }
      setValueCount(values);
    }

    public void generateTestDataAlt(int size) {
      setValueCount(size);
      boolean even = true;
      final int valueCount = getAccessor().getValueCount();
      for (int i = 0; i < valueCount; i++, even = !even) {
        if (even) {
          set(i, (byte) 1);
        } else {
          set(i, (byte) 0);
        }
      }
    }
  }

  @Override
  public void clear() {
    this.valueCount = 0;
    super.clear();
  }
}
