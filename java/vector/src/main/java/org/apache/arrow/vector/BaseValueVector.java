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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

/**
 * Base class for other Arrow Vector Types.  Provides basic functionality around
 * memory management.
 */
public abstract class BaseValueVector implements ValueVector {
  private static final Logger logger = LoggerFactory.getLogger(BaseValueVector.class);

  public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
  public static final int MAX_ALLOCATION_SIZE = Integer.getInteger(MAX_ALLOCATION_SIZE_PROPERTY, Integer.MAX_VALUE);
  /*
   * For all fixed width vectors, the value and validity buffers are sliced from a single buffer.
   * Similarly, for variable width vectors, the offsets and validity buffers are sliced from a
   * single buffer. To ensure the single buffer is power-of-2 size, the initial value allocation
   * should be less than power-of-2. For IntVectors, this comes to 3970*4 (15880) for the data
   * buffer and 504 bytes for the validity buffer, totalling to 16384 (2^16).
   */
  public static final int INITIAL_VALUE_ALLOCATION = 3970;

  protected final BufferAllocator allocator;
  protected ArrowBuf validityBuffer;
  protected int validityAllocationSizeInBytes;
  protected int nullCount;

  /**
   * Constructs a new instance.
   *
   * @param allocator allocator for memory management.
   */
  protected BaseValueVector(BufferAllocator allocator) {
    this.validityBuffer = allocator.getEmpty();
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    this.nullCount = -1;
  }

  @Override
  public abstract String getName();

  /**
   * Representation of vector suitable for debugging.
   */
  @Override
  public String toString() {
    return ValueVectorUtility.getToString(this, 0, getValueCount());
  }

  @Override
  public void clear() {
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getName(), allocator);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.emptyIterator();
  }

  /**
   * Checks to ensure that every buffer <code>vv</code> uses
   * has a positive reference count, throws if this precondition
   * isn't met.  Returns true otherwise.
   */
  public static boolean checkBufRefs(final ValueVector vv) {
    for (final ArrowBuf buffer : vv.getBuffers(false)) {
      if (buffer.refCnt() <= 0) {
        throw new IllegalStateException("zero refcount");
      }
    }

    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  void compareTypes(BaseValueVector target, String caller) {
    if (this.getMinorType() != target.getMinorType()) {
      throw new UnsupportedOperationException(caller + " should have vectors of exact same type");
    }
  }


  /**
   * Load the validityBuffer this vector with provided source buffer.
   * The caller manages the source buffers and populates them before invoking
   * this method.
   *
   * @param fieldNode  the fieldNode indicating the value count
   */
  public void loadValidityBuffer(final ArrowFieldNode fieldNode, ArrowBuf bitBuffer) {
    validityBuffer.getReferenceManager().release();
    validityBuffer = bitBuffer.getReferenceManager().retain(bitBuffer, allocator);
    this.nullCount = fieldNode.getNullCount();
  }

  public void add(List<ArrowBuf> list) {
    list.add(validityBuffer);
  }

  public void setValidityBufferReaderIndex(int index) {
    validityBuffer.readerIndex(index);
  }

  public void setValidityBufferWriterIndex(int index) {
    validityBuffer.writerIndex(index);
  }

  public void markValidityBitToOne(int index) {
    BitVectorHelper.setBit(validityBuffer, index);
  }

  public void markValidityBitToZero(int index) {
    BitVectorHelper.unsetBit(validityBuffer, index);
  }

  public void setValidityBit(int index, int value) {
    BitVectorHelper.setValidityBit(validityBuffer, index, value);
  }

  void setBitMaskedByte(int byteIndex, byte bitMask) {
    BitVectorHelper.setBitMaskedByte(validityBuffer, byteIndex, bitMask);
  }

  void setValidityBufferByte(int byteIndex, int bitMask) {
    validityBuffer.setByte(byteIndex, bitMask);
  }

  public void setValidityAllocationSizeInBytes(int size) {
    this.validityAllocationSizeInBytes = size;
  }


  /**
   * During splitAndTransfer, if we splitting from a random position within a byte,
   * we can't just slice the source buffer so we have to explicitly allocate the
   * validityBuffer of the target vector. This is unlike the databuffer which we can
   * always slice for the target vector.
   */
  public void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  /**
   * Get the memory address of buffer that manages the validity
   * (NULL or NON-NULL nature) of elements in the vector.
   * @return starting address of the buffer
   */
  public long getValidityBufferAddress() {
    return (validityBuffer.memoryAddress());
  }

  /**
   * Get buffer that manages the validity (NULL or NON-NULL nature) of
   * elements in the vector. Consider it as a buffer for internal bit vector
   * data structure.
   * @return buffer
   */
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  public void setValidityBuffer(ArrowBuf validityBuffer) {
    this.validityBuffer.getReferenceManager().release();
    this.validityBuffer = validityBuffer;
  }

  public int getValidityBufferCapacity() {
    return this.validityBuffer.capacity();
  }

  public void transferValidityBuffer(BaseValueVector target) {
    target.validityBuffer = transferBuffer(validityBuffer, target.allocator);
  }

  /* zero out the validity buffer */
  public void initValidityBuffer() {
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  public void clearValidityBuffer() {
    this.validityBuffer = releaseBuffer(validityBuffer);
  }

  /**
   * Validity buffer has multiple cases of split and transfer depending on
   * the starting position of the source index.
   */
  public void splitAndTransferValidityBuffer(int startIndex, int length, int valueCount,
                                                   BaseValueVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = getValidityBufferSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        /* slice */
        if (target.validityBuffer != null) {
          target.validityBuffer.release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.retain(1);
      } else {
        /* Copy data
         * When the first bit starts from the middle of a byte (offset != 0),
         * copy data from src BitVector.
         * Each byte in the target is composed by a part in i-th byte,
         * another part in (i+1)-th byte.
         */
        target.allocateValidityBuffer(byteSizeTarget);

        for (int i = 0; i < byteSizeTarget - 1; i++) {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer,
                  firstByteSource + i + 1, offset);

          target.validityBuffer.setByte(i, (b1 + b2));
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
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget, offset);

          target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
        } else {
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer,
                  firstByteSource + byteSizeTarget - 1, offset);
          target.validityBuffer.setByte(byteSizeTarget - 1, b1);
        }
      }
    }
  }

  /**
   * Check if element at given index is null.
   *
   * @param index  position of element
   * @return true if element at given index is null, false otherwise
   */
  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index  position of element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  public int isSet(int index) {
    if (nullCount == 0 && index < getValueCount()) {
      return 1;
    }
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  public void releaseValidityBuffer() {
    validityBuffer = releaseBuffer(validityBuffer);
  }

  public int getValidityBufferValueCapacity() {
    return validityBuffer.capacity() * 8;
  }

  public void setReaderIndex(int index) {
    validityBuffer.readerIndex(index);
  }

  public void setWriterIndex(int index) {
    validityBuffer.writerIndex(index);
  }

  public int  getValidityAllocationSizeInBytes() {
    return validityAllocationSizeInBytes;
  }

  public void setNullCount() {
    this.setNullCount(getNullCount());
  }

  public void setNullCount(int nullCount) {
    this.nullCount = nullCount;
  }

  @Override
  public int getNullCount() {
    if (nullCount == -1) {
      return BitVectorHelper.getNullCount(validityBuffer, getValueCount());
    }
    return this.nullCount;
  }


  /* reallocate the validity buffer */
  protected void reallocValidityBuffer() {
    final int currentBufferCapacity = validityBuffer.capacity();
    long baseSize = validityAllocationSizeInBytes;

    if (baseSize < (long) currentBufferCapacity) {
      baseSize = (long) currentBufferCapacity;
    }

    long newAllocationSize = baseSize * 2L;
    newAllocationSize = BaseAllocator.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer((int) newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    validityBuffer.getReferenceManager().release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  protected void reallocValidityBuffer(ArrowBuf newValidityBuffer) {
    newValidityBuffer.setBytes(0, validityBuffer, 0, validityBuffer.capacity());
    newValidityBuffer.setZero(validityBuffer.capacity(), newValidityBuffer.capacity() - validityBuffer.capacity());
    validityBuffer.getReferenceManager().release();
    validityBuffer = newValidityBuffer;
  }

  protected ArrowBuf releaseBuffer(ArrowBuf buffer) {
    buffer.getReferenceManager().release();
    buffer = allocator.getEmpty();
    return buffer;
  }



  /* number of bytes for the validity buffer for the given valueCount */
  protected static int getValidityBufferSizeFromCount(final int valueCount) {
    return DataSizeRoundingUtil.divideBy8Ceil(valueCount);
  }

  /* round up bytes for the validity buffer for the given valueCount */
  private static long roundUp8ForValidityBuffer(int valueCount) {
    return ((valueCount + 63) >> 6) << 3;
  }

  long computeCombinedBufferSize(int valueCount, int typeWidth) {
    Preconditions.checkArgument(valueCount >= 0, "valueCount must be >= 0");
    Preconditions.checkArgument(typeWidth >= 0, "typeWidth must be >= 0");

    // compute size of validity buffer.
    long bufferSize = roundUp8ForValidityBuffer(valueCount);

    // add the size of the value buffer.
    if (typeWidth == 0) {
      // for boolean type, value-buffer and validity-buffer are of same size.
      bufferSize *= 2;
    } else {
      bufferSize += DataSizeRoundingUtil.roundUpTo8Multiple(valueCount * typeWidth);
    }
    return BaseAllocator.nextPowerOfTwo(bufferSize);
  }

  /**
   * Container for primitive vectors (1 for the validity bit-mask and one to hold the values).
   */
  class DataAndValidityBuffers {
    private ArrowBuf dataBuf;
    private ArrowBuf validityBuf;

    DataAndValidityBuffers(ArrowBuf dataBuf, ArrowBuf validityBuf) {
      this.dataBuf = dataBuf;
      this.validityBuf = validityBuf;
    }

    ArrowBuf getDataBuf() {
      return dataBuf;
    }

    ArrowBuf getValidityBuf() {
      return validityBuf;
    }
  }

  DataAndValidityBuffers allocFixedDataAndValidityBufs(int valueCount, int typeWidth) {
    long bufferSize = computeCombinedBufferSize(valueCount, typeWidth);
    assert bufferSize <= MAX_ALLOCATION_SIZE;

    int validityBufferSize;
    int dataBufferSize;
    if (typeWidth == 0) {
      validityBufferSize = dataBufferSize = (int) (bufferSize / 2);
    } else {
      // Due to roundup to power-of-2 allocation, the bufferSize could be greater than the
      // requested size. Utilize the allocated buffer fully.;
      int actualCount = (int) ((bufferSize * 8.0) / (8 * typeWidth + 1));
      do {
        validityBufferSize = (int) roundUp8ForValidityBuffer(actualCount);
        dataBufferSize = DataSizeRoundingUtil.roundUpTo8Multiple(actualCount * typeWidth);
        if (validityBufferSize + dataBufferSize <= bufferSize) {
          break;
        }
        --actualCount;
      } while (true);
    }


    /* allocate combined buffer */
    ArrowBuf combinedBuffer = allocator.buffer((int) bufferSize);

    /* slice into requested lengths */
    ArrowBuf dataBuf = null;
    ArrowBuf validityBuf = null;
    int bufferOffset = 0;
    for (int numBuffers = 0; numBuffers < 2; ++numBuffers) {
      int len = (numBuffers == 0 ? dataBufferSize : validityBufferSize);
      ArrowBuf buf = combinedBuffer.slice(bufferOffset, len);
      buf.getReferenceManager().retain();
      buf.readerIndex(0);
      buf.writerIndex(0);

      bufferOffset += len;
      if (numBuffers == 0) {
        dataBuf = buf;
      } else {
        validityBuf = buf;
      }
    }
    combinedBuffer.getReferenceManager().release();
    return new DataAndValidityBuffers(dataBuf, validityBuf);
  }

  public static ArrowBuf transferBuffer(final ArrowBuf srcBuffer, final BufferAllocator targetAllocator) {
    final ReferenceManager referenceManager = srcBuffer.getReferenceManager();
    return referenceManager.transferOwnership(srcBuffer, targetAllocator).getTransferredBuffer();
  }

  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException();
  }
}

