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


import io.netty.buffer.ArrowBuf;

import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BaseVariableWidthVector extends BaseValueVector
        implements VariableWidthVector, FieldVector, VectorDefinitionSetter {
  private static final int DEFAULT_RECORD_BYTE_COUNT = 8;
  private static final int INITIAL_BYTE_COUNT = INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT;

  private int valueAllocationSizeInBytes;
  private int validityAllocationSizeInBytes;
  private int offsetAllocationSizeInBytes;

  /* protected members */
  public static final int OFFSET_WIDTH = 4; /* 4 byte unsigned int to track offsets */
  protected static final byte[] emptyByteArray = new byte[]{};
  protected ArrowBuf validityBuffer;
  protected ArrowBuf valueBuffer;
  protected ArrowBuf offsetBuffer;
  protected int valueCount;
  protected int lastSet;
  protected final Field field;
  private boolean cleared;

  public BaseVariableWidthVector(final String name, final BufferAllocator allocator,
                                         FieldType fieldType) {
    super(name, allocator);
    valueAllocationSizeInBytes = INITIAL_BYTE_COUNT;
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    offsetAllocationSizeInBytes = (INITIAL_VALUE_ALLOCATION) * OFFSET_WIDTH;
    field = new Field(name, fieldType, null);
    valueCount = 0;
    lastSet = -1;
    offsetBuffer = allocator.getEmpty();
    validityBuffer = allocator.getEmpty();
    valueBuffer = allocator.getEmpty();
    cleared = false;
  }

  /* TODO:
   * see if getNullCount() can be made faster -- O(1)
   */

  /* TODO:
   * Once the entire hierarchy has been refactored, move common functions
   * like getNullCount(), splitAndTransferValidityBuffer to top level
   * base class BaseValueVector.
   *
   * Along with this, some class members (validityBuffer) can also be
   * abstracted out to top level base class.
   *
   * Right now BaseValueVector is the top level base class for other
   * vector types in ValueVector hierarchy (non-nullable) and those
   * vectors have not yet been refactored/removed so moving things to
   * the top class as of now is not a good idea.
   */

  /**
   * Get buffer that manages the validity (NULL or NON-NULL nature) of
   * elements in the vector. Consider it as a buffer for internal bit vector
   * data structure.
   * @return buffer
   */
  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  /**
   * Get the buffer that stores the data for elements in the vector.
   * @return buffer
   */
  @Override
  public ArrowBuf getDataBuffer() {
    return valueBuffer;
  }

  /**
   * buffer that stores the offsets for elements
   * in the vector. This operation is not supported for fixed-width vectors.
   * @return buffer
   */
  @Override
  public ArrowBuf getOffsetBuffer() {
    return offsetBuffer;
  }

  /**
   * Get the memory address of buffer that stores the offsets for elements
   * in the vector.
   * @return starting address of the buffer
   */
  @Override
  public long getOffsetBufferAddress() {
    return offsetBuffer.memoryAddress();
  }

  /**
   * Get the memory address of buffer that manages the validity
   * (NULL or NON-NULL nature) of elements in the vector.
   * @return starting address of the buffer
   */
  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  /**
   * Get the memory address of buffer that stores the data for elements
   * in the vector.
   * @return starting address of the buffer
   */
  @Override
  public long getDataBufferAddress() {
    return valueBuffer.memoryAddress();
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   * @param valueCount desired number of elements in the vector
   */
  @Override
  public void setInitialCapacity(int valueCount) {
    final long size = (long) valueCount * DEFAULT_RECORD_BYTE_COUNT;
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }
    valueAllocationSizeInBytes = (int) size;
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(valueCount);
    /* to track the end offset of last data element in vector, we need
     * an additional slot in offset buffer.
     */
    offsetAllocationSizeInBytes = (valueCount + 1) * OFFSET_WIDTH;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   * @param valueCount desired number of elements in the vector
   * @param density average number of bytes per variable width element
   */
  @Override
  public void setInitialCapacity(int valueCount, double density) {
    long size = Math.max((long)(valueCount * density), 1L);

    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory is more than max allowed");
    }

    valueAllocationSizeInBytes = (int) size;
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(valueCount);
    /* to track the end offset of last data element in vector, we need
     * an additional slot in offset buffer.
     */
    offsetAllocationSizeInBytes = (valueCount + 1) * OFFSET_WIDTH;
  }

  /**
   * Get the density of this ListVector
   * @return density
   */
  public double getDensity() {
    if (valueCount == 0) {
      return 0.0D;
    }
    final int startOffset = offsetBuffer.getInt(0);
    final int endOffset = offsetBuffer.getInt(valueCount * OFFSET_WIDTH);
    final double totalListSize = endOffset - startOffset;
    return totalListSize/valueCount;
  }

  /**
   * Get the current value capacity for the vector
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity() - 1, 0);
    return Math.min(offsetValueCapacity, getValidityBufferValueCapacity());
  }

  private int getValidityBufferValueCapacity() {
    return (int) (validityBuffer.capacity() * 8L);
  }

  private int getOffsetBufferValueCapacity() {
    return (int) ((offsetBuffer.capacity() * 1.0) / OFFSET_WIDTH);
  }

  /**
   * zero out the vector and the data in associated buffers.
   */
  public void zeroVector() {
    initValidityBuffer();
    initOffsetBuffer();
    valueBuffer.setZero(0, valueBuffer.capacity());
  }

  /* zero out the validity buffer */
  private void initValidityBuffer() {
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  /* zero out the offset buffer */
  private void initOffsetBuffer() {
    offsetBuffer.setZero(0, offsetBuffer.capacity());
  }

  /**
   * Reset the vector to initial state. Same as {@link #zeroVector()}.
   * Note that this method doesn't release any memory.
   */
  public void reset() {
    zeroVector();
    lastSet = -1;
    valueCount = 0;
  }

  /**
   * Close the vector and release the associated buffers.
   */
  @Override
  public void close() {
    clear();
  }

  /**
   * Same as {@link #close()}
   */
  @Override
  public void clear() {
    validityBuffer = releaseBuffer(validityBuffer);
    valueBuffer = releaseBuffer(valueBuffer);
    offsetBuffer = releaseBuffer(offsetBuffer);
    cleared = true;
    lastSet = -1;
    valueCount = 0;
  }

  @Override
  @Deprecated
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  /**
   * Initialize the children in schema for this Field. This operation is a
   * NO-OP for scalar types since they don't have any children.
   * @param children the schema
   * @throws IllegalArgumentException if children is a non-empty list for scalar types.
   */
  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (!children.isEmpty()) {
      throw new IllegalArgumentException("primitive type vector can not have children");
    }
  }

  /**
   * Get the inner child vectors.
   * @return list of child vectors for complex types, empty list for scalar vector
   * types
   */
  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Collections.emptyList();
  }


  /**
   * Load the buffers of this vector with provided source buffers.
   * The caller manages the source buffers and populates them before invoking
   * this method.
   * @param fieldNode  the fieldNode indicating the value count
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    ArrowBuf bitBuffer = ownBuffers.get(0);
    ArrowBuf offBuffer = ownBuffers.get(1);
    ArrowBuf dataBuffer = ownBuffers.get(2);

    validityBuffer.release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    offsetBuffer.release();
    offsetBuffer = offBuffer.retain(allocator);
    valueBuffer.release();
    valueBuffer = dataBuffer.retain(allocator);

    lastSet = fieldNode.getLength() - 1;
    valueCount = fieldNode.getLength();
  }

  /**
   * Get the buffers belonging to this vector
   * @return the inner buffers.
   */
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(3);
    setReaderAndWriterIndex();
    result.add(validityBuffer);
    result.add(offsetBuffer);
    result.add(valueBuffer);

    return result;
  }

  /**
   * Set the reader and writer indexes for the inner buffers.
   */
  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    offsetBuffer.readerIndex(0);
    valueBuffer.readerIndex(0);
    if (valueCount == 0) {
      validityBuffer.writerIndex(0);
      offsetBuffer.writerIndex(0);
      valueBuffer.writerIndex(0);
    } else {
      final int lastDataOffset = getstartOffset(valueCount);
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      offsetBuffer.writerIndex((valueCount + 1) * OFFSET_WIDTH);
      valueBuffer.writerIndex(lastDataOffset);
    }
  }

  /**
   * Same as {@link #allocateNewSafe()}.
   */
  @Override
  public void allocateNew() {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating memory.");
    }
  }

  /**
   * Allocate memory for the vector. We internally use a default value count
   * of 4096 to allocate memory for at least these many elements in the
   * vector. See {@link #allocateNew(int, int)} for allocating memory for specific
   * number of elements in the vector.
   *
   * @return false if memory allocation fails, true otherwise.
   */
  @Override
  public boolean allocateNewSafe() {
    long curAllocationSizeValue = valueAllocationSizeInBytes;
    long curAllocationSizeValidity = validityAllocationSizeInBytes;
    long curAllocationSizeOffset = offsetAllocationSizeInBytes;

    if (curAllocationSizeValue > MAX_ALLOCATION_SIZE ||
            curAllocationSizeOffset > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory exceeds limit");
    }

    /* we are doing a new allocation -- release the current buffers */
    clear();

    try {
      allocateBytes(curAllocationSizeValue, curAllocationSizeValidity, curAllocationSizeOffset);
    } catch (Exception e) {
      e.printStackTrace();
      clear();
      return false;
    }

    return true;
  }

  /**
   * Allocate memory for the vector to support storing at least the provided number of
   * elements in the vector. This method must be called prior to using the ValueVector.
   *
   * @param totalBytes desired total memory capacity
   * @param valueCount the desired number of elements in the vector
   * @throws org.apache.arrow.memory.OutOfMemoryException
   */
  @Override
  public void allocateNew(int totalBytes, int valueCount) {
    assert totalBytes >= 0;
    final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);

    if (totalBytes > MAX_ALLOCATION_SIZE ||
            offsetBufferSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Requested amount of memory exceeds limit");
    }

    /* we are doing a new allocation -- release the current buffers */
    clear();

    try {
      allocateBytes(totalBytes, validityBufferSize, offsetBufferSize);
    } catch (Exception e) {
      e.printStackTrace();
      clear();
    }
  }

  /* allocate the inner buffers */
  private void allocateBytes(final long valueBufferSize, final long validityBufferSize,
                             final long offsetBufferSize) {
    /* allocate data buffer */
    int curSize = (int) valueBufferSize;
    valueBuffer = allocator.buffer(curSize);
    valueBuffer.readerIndex(0);
    valueAllocationSizeInBytes = curSize;
    allocateValidityBuffer(validityBufferSize);
    allocateOffsetBuffer(offsetBufferSize);
  }

  /* allocate offset buffer */
  private void allocateOffsetBuffer(final long size) {
    final int curSize = (int) size;
    offsetBuffer = allocator.buffer(curSize);
    offsetBuffer.readerIndex(0);
    offsetAllocationSizeInBytes = curSize;
    initOffsetBuffer();
  }

  /* allocate validity buffer */
  private void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    initValidityBuffer();
  }

  /**
   * Resize the vector to increase the capacity. The internal behavior is to
   * double the current value capacity.
   */
  public void reAlloc() {
    reallocDataBuffer();
    reallocValidityAndOffsetBuffers();
  }

  /**
   * Reallocate the data buffer. Data Buffer stores the actual data for
   * VARCHAR or VARBINARY elements in the vector. The behavior is to double
   * the size of buffer.
   * @throws OversizedAllocationException if the desired new size is more than
   *                                      max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocDataBuffer() {
    long baseSize = valueAllocationSizeInBytes;
    final int currentBufferCapacity = valueBuffer.capacity();

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
    newBuf.setBytes(0, valueBuffer, 0, currentBufferCapacity);
    valueBuffer.release();
    valueBuffer = newBuf;
    valueAllocationSizeInBytes = (int) newAllocationSize;
  }

  /**
   * Reallocate the validity and offset buffers for this vector. Validity
   * buffer is used to track the NULL or NON-NULL nature of elements in
   * the vector and offset buffer is used to store the lengths of variable
   * width elements in the vector.
   *
   * Note that data buffer for variable length vectors moves independent
   * of the companion validity and offset buffers. This is in
   * contrast to what we have for fixed width vectors.
   *
   * So even though we may have setup an initial capacity of 1024
   * elements in the vector, it is quite possible
   * that we need to reAlloc() the data buffer when we are setting
   * the 5th element in the vector simply because previous
   * variable length elements have exhausted the buffer capacity.
   * However, we really don't need to reAlloc() validity and
   * offset buffers until we try to set the 1025th element
   * This is why we do a separate check for safe methods to
   * determine which buffer needs reallocation.
   * @throws OversizedAllocationException if the desired new size is more than
   *                                      max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocValidityAndOffsetBuffers() {
    offsetBuffer = reallocBufferHelper(offsetBuffer, true);
    validityBuffer = reallocBufferHelper(validityBuffer, false);
  }

  /* helper method to realloc a particular buffer. returns the allocated buffer */
  private ArrowBuf reallocBufferHelper(ArrowBuf buffer, final boolean offsetBuffer) {
    final int currentBufferCapacity = buffer.capacity();
    long baseSize = (offsetBuffer ? offsetAllocationSizeInBytes
            : validityAllocationSizeInBytes);

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
    newBuf.setBytes(0, buffer, 0, currentBufferCapacity);
    final int halfNewCapacity = newBuf.capacity() / 2;
    newBuf.setZero(halfNewCapacity, halfNewCapacity);
    buffer.release(1);
    buffer = newBuf;
    if (offsetBuffer) {
      offsetAllocationSizeInBytes = (int) newAllocationSize;
    } else {
      validityAllocationSizeInBytes = (int) newAllocationSize;
    }

    return buffer;
  }

  /**
   * Get the size (number of bytes) of underlying data buffer.
   * @return
   */
  @Override
  public int getByteCapacity() {
    return valueBuffer.capacity();
  }

  @Override
  public int getCurrentSizeInBytes() {
      /* TODO */
    return 0;
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this
   * vector
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    return getBufferSizeFor(this.valueCount);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   * @param valueCount desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds
   *         a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
      /* get the end offset for this valueCount */
    final int dataBufferSize = offsetBuffer.getInt(valueCount * OFFSET_WIDTH);
    return validityBufferSize + offsetBufferSize + dataBufferSize;
  }

  /**
   * Get information about how this field is materialized.
   * @return the field corresponding to this vector
   */
  @Override
  public Field getField() {
    return field;
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't
   * impact the reference counts for this buffer so it only should be used for in-context
   * access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   *
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted
   *              but the returned array will be the only reference to them
   * @return The underlying {@link io.netty.buffer.ArrowBuf buffers} that is used by this
   *         vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers;
    setReaderAndWriterIndex();
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      buffers = new ArrowBuf[3];
      buffers[0] = validityBuffer;
      buffers[1] = offsetBuffer;
      buffers[2] = valueBuffer;
    }
    if (clear) {
      for (final ArrowBuf buffer : buffers) {
        buffer.retain(1);
      }
      clear();
    }
    return buffers;
  }

  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @param callBack
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(ref, allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

  /**
   * Transfer this vector'data to another vector. The memory associated
   * with this vector is transferred to the allocator of target vector
   * for accounting and management purposes.
   * @param target destination vector for transfer
   */
  public void transferTo(BaseVariableWidthVector target) {
    compareTypes(target, "transferTo");
    target.clear();
    target.validityBuffer = validityBuffer.transferOwnership(target.allocator).buffer;
    target.valueBuffer = valueBuffer.transferOwnership(target.allocator).buffer;
    target.offsetBuffer = offsetBuffer.transferOwnership(target.allocator).buffer;
    target.setLastSet(this.lastSet);
    if (this.valueCount > 0) {
      target.setValueCount(this.valueCount);
    }
    clear();
  }

  /**
   * Slice this vector at desired index and length and transfer the
   * corresponding data to the target vector.
   * @param startIndex start position of the split in source vector.
   * @param length length of the split.
   * @param target destination vector
   */
  public void splitAndTransferTo(int startIndex, int length,
                                 BaseVariableWidthVector target) {
    compareTypes(target, "splitAndTransferTo");
    target.clear();
    splitAndTransferValidityBuffer(startIndex, length, target);
    splitAndTransferOffsetBuffer(startIndex, length, target);
    target.setLastSet(length - 1);
    if (this.valueCount > 0) {
      target.setValueCount(this.valueCount);
    }
  }

  /*
   * Transfer the offsets along with data. Unlike the data buffer, we cannot simply
   * slice the offset buffer for split and transfer. The reason is that offsets
   * in the target vector have to be adjusted and made relative to the staring
   * offset in source vector from the start index of split. This is why, we
   * need to explicitly allocate the offset buffer and set the adjusted offsets
   * in the target vector.
   */
  private void splitAndTransferOffsetBuffer(int startIndex, int length, BaseVariableWidthVector target) {
    final int start = offsetBuffer.getInt(startIndex * OFFSET_WIDTH);
    final int end = offsetBuffer.getInt((startIndex + length) * OFFSET_WIDTH);
    final int dataLength = end - start;
    target.allocateOffsetBuffer((length + 1) * OFFSET_WIDTH);
    for (int i = 0; i < length + 1; i++) {
      final int relativeSourceOffset = offsetBuffer.getInt((startIndex + i) * OFFSET_WIDTH) - start;
      target.offsetBuffer.setInt(i * OFFSET_WIDTH, relativeSourceOffset);
    }
    target.valueBuffer = valueBuffer.slice(start, dataLength).transferOwnership(target.allocator).buffer;
  }

  /*
   * Transfer the validity.
   */
  private void splitAndTransferValidityBuffer(int startIndex, int length,
                                              BaseVariableWidthVector target) {
    assert startIndex + length <= valueCount;
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = getValidityBufferSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        // slice
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
          byte b1 = BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer, firstByteSource + i, offset);
          byte b2 = BitVectorHelper.getBitsFromNextByte(this.validityBuffer, firstByteSource + i + 1, offset);

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


  /******************************************************************
   *                                                                *
   *                common getters and setters                      *
   *                                                                *
   ******************************************************************/


  /**
   * Get the number of elements that are null in the vector
   *
   * @return the number of null elements.
   */
  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  /**
   * Check if the given index is within the current value capacity
   * of the vector
   *
   * @param index  position to check
   * @return true if index is within the current value capacity
   */
  public boolean isSafe(int index) {
    return index < getValueCapacity();
  }

  /**
   * Check if element at given index is null.
   *
   * @param index  position of element
   * @return true if element at given index is null
   */
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
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return Long.bitCount(b & (1L << bitIndex));
  }

  /**
   * Get the value count of vector. This will always be zero unless
   * setValueCount(int) has been called prior to calling this.
   *
   * @return valueCount for the vector
   */
  public int getValueCount() {
    return valueCount;
  }

  /**
   * Sets the value count for the vector
   *
   * @param valueCount   value count
   */
  public void setValueCount(int valueCount) {
    assert valueCount >= 0;
    this.valueCount = valueCount;
    while (valueCount > getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    fillHoles(valueCount);
    lastSet = valueCount - 1;
    setReaderAndWriterIndex();
  }

  /**
   * Create holes in the vector upto the given index (exclusive).
   * Holes will be created from the current last set position in
   * the vector.
   *
   * @param index target index
   */
  public void fillEmpties(int index) {
    handleSafe(index, emptyByteArray.length);
    fillHoles(index);
    lastSet = index - 1;
  }

  /**
   * Set the index of last non-null element in the vector.
   * It is important to call this method with appropriate value
   * before calling {@link #setValueCount(int)}.
   *
   * @param value desired index of last non-null element.
   */
  public void setLastSet(int value) {
    lastSet = value;
  }

  /**
   * Get the index of last non-null element in the vector.
   *
   * @return index of the last non-null element
   */
  public int getLastSet() {
    return lastSet;
  }

  /**
   * Get the starting position (offset) in the data stream for a given
   * element in the vector.
   *
   * @param index position of the element in the vector
   * @return starting offset for the element
   */
  public long getStartEnd(int index) {
    return offsetBuffer.getLong(index * OFFSET_WIDTH);
  }

  /**
   * Mark the particular position in the vector as non-null.
   *
   * @param index position of the element.
   */
  @Override
  public void setIndexDefined(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      validityBuffer = reallocBufferHelper(validityBuffer, false);
    }
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
  }

  /**
   * Sets the value length for an element.
   *
   * @param index   position of the element to set
   * @param length  length of the element
   */
  public void setValueLengthSafe(int index, int length) {
    assert index >= 0;
    handleSafe(index, length);
    fillHoles(index);
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    lastSet = index;
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of element to get
   * @return greater than 0 length for non-null element, 0 otherwise
   */
  public int getValueLength(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      return 0;
    }
    final int startOffset = getstartOffset(index);
    final int dataLength =
            offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
    return dataLength;
  }

  /**
   * Set the variable length element at the specified index to the supplied
   * byte array. This is same as using {@link #set(int, byte[], int, int)}
   * with start as 0 and length as value.length
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   */
  public void set(int index, byte[] value) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setBytes(index, value, 0, value.length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, byte[])} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   */
  public void setSafe(int index, byte[] value) {
    assert index >= 0;
    fillEmpties(index);
    handleSafe(index, value.length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setBytes(index, value, 0, value.length);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the supplied
   * byte array.
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   * @param start   start index in array of bytes
   * @param length  length of data in array of bytes
   */
  public void set(int index, byte[] value, int start, int length) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setBytes(index, value, start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, byte[], int, int)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   * @param start   start index in array of bytes
   * @param length  length of data in array of bytes
   */
  public void setSafe(int index, byte[] value, int start, int length) {
    assert index >= 0;
    fillEmpties(index);
    handleSafe(index, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    setBytes(index, value, start, length);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied ByteBuffer
   *
   * @param index   position of the element to set
   * @param value   ByteBuffer with data
   * @param start   start index in ByteBuffer
   * @param length  length of data in ByteBuffer
   */
  public void set(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    valueBuffer.setBytes(startOffset, value, start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, ByteBuffer, int, int)} except that it handles the
   * case where index and length of new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param value   ByteBuffer with data
   * @param start   start index in ByteBuffer
   * @param length  length of data in ByteBuffer
   */
  public void setSafe(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;
    fillEmpties(index);
    handleSafe(index, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int startOffset = getstartOffset(index);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    valueBuffer.setBytes(startOffset, value, start, length);
    lastSet = index;
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index   position of element
   */
  public void setNull(int index) {
    while (index >= getValidityBufferValueCapacity()) {
      validityBuffer = reallocBufferHelper(validityBuffer, false);
    }
    BitVectorHelper.setValidityBit(validityBuffer, index, 0);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void set(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;
    final int dataLength = end - start;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    final int startOffset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, buffer, start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;
    final int dataLength = end - start;
    fillEmpties(index);
    handleSafe(index, end);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    final int startOffset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + dataLength);
    valueBuffer.setBytes(startOffset, buffer, start, dataLength);
    lastSet = index;
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param start start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void set(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int startOffset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param start start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;
    fillEmpties(index);
    handleSafe(index, length);
    BitVectorHelper.setValidityBitToOne(validityBuffer, index);
    final int startOffset = offsetBuffer.getInt(index * OFFSET_WIDTH);
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = index;
  }


  /******************************************************************
   *                                                                *
   *                helper methods for setters                      *
   *                                                                *
   ******************************************************************/


  protected final void fillHoles(int index) {
    for (int i = lastSet + 1; i < index; i++) {
      setBytes(i, emptyByteArray, 0, emptyByteArray.length);
    }
    lastSet = index - 1;
  }

  protected final void setBytes(int index, byte[] value, int start, int length) {
    /* end offset of current last element in the vector. this will
     * be the start offset of new element we are trying to store.
     */
    final int startOffset = getstartOffset(index);
    /* set new end offset */
    offsetBuffer.setInt((index + 1) * OFFSET_WIDTH, startOffset + length);
    /* store the var length data in value buffer */
    valueBuffer.setBytes(startOffset, value, start, length);
  }

  protected final int getstartOffset(int index) {
    return offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  protected final void handleSafe(int index, int dataLength) {
    /*
     * IMPORTANT:
     * value buffer for variable length vectors moves independent
     * of the companion validity and offset buffers. This is in
     * contrast to what we have for fixed width vectors.
     *
     * Here there is no concept of getValueCapacity() in the
     * data stream. getValueCapacity() is applicable only to validity
     * and offset buffers.
     *
     * So even though we may have setup an initial capacity of 1024
     * elements in the vector, it is quite possible
     * that we need to reAlloc() the data buffer when we are setting
     * the 5th element in the vector simply because previous
     * variable length elements have exhausted the buffer capacity.
     * However, we really don't need to reAlloc() validity and
     * offset buffers until we try to set the 1025th element
     * This is why we do a separate check for safe methods to
     * determine which buffer needs reallocation.
     */
    while (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    final int startOffset = getstartOffset(index);
    while (valueBuffer.capacity() < (startOffset + dataLength)) {
      reallocDataBuffer();
    }
  }

  /**
   * Method used by Json Writer to read a variable width element from
   * the variable width vector and write to Json.
   *
   * This method should not be used externally.
   *
   * @param data buffer storing the variable width vector elements
   * @param offset buffer storing the offsets of variable width vector elements
   * @param index position of the element in the vector
   * @return array of bytes
   */
  public static byte[] get(final ArrowBuf data, final ArrowBuf offset, int index) {
    final int currentStartOffset = offset.getInt(index * OFFSET_WIDTH);
    final int dataLength =
            offset.getInt((index + 1) * OFFSET_WIDTH) - currentStartOffset;
    final byte[] result = new byte[dataLength];
    data.getBytes(currentStartOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Method used by Json Reader to explicitly set the offsets of the variable
   * width vector data. The method takes care of allocating the memory for
   * offsets if the caller hasn't done so.
   *
   * This method should not be used externally.
   *
   * @param buffer ArrowBuf to store offsets for variable width elements
   * @param allocator memory allocator
   * @param valueCount number of elements
   * @param index position of the element
   * @param value offset of the element
   * @return buffer holding the offsets
   */
  public static ArrowBuf set(ArrowBuf buffer, BufferAllocator allocator,
                             int valueCount, int index, int value) {
    if (buffer == null) {
      buffer = allocator.buffer(valueCount * OFFSET_WIDTH);
    }
    buffer.setInt(index * OFFSET_WIDTH, value);
    if (index == (valueCount - 1)) {
      buffer.writerIndex(valueCount * OFFSET_WIDTH);
    }

    return buffer;
  }
}
