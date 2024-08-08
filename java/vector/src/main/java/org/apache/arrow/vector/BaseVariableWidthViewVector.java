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
import static org.apache.arrow.vector.util.DataSizeRoundingUtil.roundUpToMultipleOf16;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

/**
 * BaseVariableWidthViewVector is a base class providing functionality for strings/bytes types in
 * view format.
 */
public abstract class BaseVariableWidthViewVector extends BaseValueVector
    implements VariableWidthFieldVector {
  // A single element of a view comprises 16 bytes
  public static final int ELEMENT_SIZE = 16;
  public static final int INITIAL_VIEW_VALUE_ALLOCATION = 4096;
  private static final int INITIAL_BYTE_COUNT = INITIAL_VIEW_VALUE_ALLOCATION * ELEMENT_SIZE;
  private static final int MAX_BUFFER_SIZE = (int) Math.min(MAX_ALLOCATION_SIZE, Integer.MAX_VALUE);
  private int lastValueCapacity;
  private long lastValueAllocationSizeInBytes;

  /*
   * Variable Width View Vector comprises the following format
   *
   * Short strings, length <= 12
   * | Bytes 0-3  | Bytes 4-15                            |
   * |------------|---------------------------------------|
   * | length     | data (padded with 0)                  |
   * |------------|---------------------------------------|
   *
   * Long strings, length > 12
   * | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
   * |------------|------------|------------|-------------|
   * | length     | prefix     | buf.index  | offset      |
   * |------------|------------|------------|-------------|
   *
   * */
  // 12 byte unsigned int to track inline views
  public static final int INLINE_SIZE = 12;
  // The first 4 bytes of view are allocated for length
  public static final int LENGTH_WIDTH = 4;
  // The second 4 bytes of view are allocated for prefix width
  public static final int PREFIX_WIDTH = 4;
  // The third 4 bytes of view are allocated for buffer index
  public static final int BUF_INDEX_WIDTH = 4;
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};
  protected ArrowBuf validityBuffer;
  // The view buffer is used to store the variable width view elements
  protected ArrowBuf viewBuffer;
  // The external buffer which stores the long strings
  protected List<ArrowBuf> dataBuffers;
  protected int initialDataBufferSize;
  protected int valueCount;
  protected int lastSet;
  protected final Field field;

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector
   * @param allocator The allocator to use for creating/resizing buffers
   */
  public BaseVariableWidthViewVector(Field field, final BufferAllocator allocator) {
    super(allocator);
    this.field = field;
    lastValueAllocationSizeInBytes = INITIAL_BYTE_COUNT;
    lastValueCapacity = INITIAL_VIEW_VALUE_ALLOCATION;
    valueCount = 0;
    lastSet = -1;
    validityBuffer = allocator.getEmpty();
    viewBuffer = allocator.getEmpty();
    dataBuffers = new ArrayList<>();
  }

  @Override
  public String getName() {
    return field.getName();
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

  /* TODO:
   * Implement TransferPair functionality
   * https://github.com/apache/arrow/issues/40932
   *
   */

  /**
   * Get buffer that manages the validity (NULL or NON-NULL nature) of elements in the vector.
   * Consider it as a buffer for internal bit vector data structure.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  /**
   * Get the buffer that stores the data for elements in the vector.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getDataBuffer() {
    return viewBuffer;
  }

  /**
   * Get the buffers that store the data for views in the vector.
   *
   * @return list of ArrowBuf
   */
  public List<ArrowBuf> getDataBuffers() {
    return dataBuffers;
  }

  /**
   * BaseVariableWidthViewVector doesn't support offset buffer.
   *
   * @return throws UnsupportedOperationException
   */
  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException(
        "Offset buffer is not supported in BaseVariableWidthViewVector");
  }

  /**
   * BaseVariableWidthViewVector doesn't support offset buffer.
   *
   * @return throws UnsupportedOperationException
   */
  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException(
        "Offset buffer is not supported in BaseVariableWidthViewVector");
  }

  /**
   * Get the memory address of buffer that manages the validity (NULL or NON-NULL nature) of
   * elements in the vector.
   *
   * @return starting address of the buffer
   */
  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  /**
   * Get the memory address of buffer that stores the data for elements in the vector.
   *
   * @return starting address of the buffer
   */
  @Override
  public long getDataBufferAddress() {
    return viewBuffer.memoryAddress();
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't allocate any memory for
   * the vector.
   *
   * @param valueCount desired number of elements in the vector
   */
  @Override
  public void setInitialCapacity(int valueCount) {
    final long size = (long) valueCount * ELEMENT_SIZE;
    checkDataBufferSize(size);
    lastValueAllocationSizeInBytes = (int) size;
    lastValueCapacity = valueCount;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't allocate any memory for
   * the vector.
   *
   * @param valueCount desired number of elements in the vector
   * @param density average number of bytes per variable width view element
   */
  @Override
  public void setInitialCapacity(int valueCount, double density) {
    final long size = (long) valueCount * ELEMENT_SIZE;
    initialDataBufferSize = (int) (valueCount * density);
    checkDataBufferSize(size);
    lastValueAllocationSizeInBytes = (int) size;
    lastValueCapacity = valueCount;
  }

  /**
   * Get the density of this ListVector.
   *
   * @return density
   */
  public double getDensity() {
    if (valueCount == 0) {
      return 0.0D;
    }
    final double totalListSize = getTotalValueLengthUpToIndex(valueCount);
    return totalListSize / valueCount;
  }

  /**
   * Get the current capacity which does not exceed either validity buffer or value buffer. Note:
   * Here the `getValueCapacity` has a relationship with the value buffer.
   *
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    final int validityCapacity = getValidityBufferValueCapacity();
    final int valueBufferCapacity = Math.max(capAtMaxInt(viewBuffer.capacity() / ELEMENT_SIZE), 0);
    return Math.min(valueBufferCapacity, validityCapacity);
  }

  private int getValidityBufferValueCapacity() {
    return capAtMaxInt(validityBuffer.capacity() * 8);
  }

  /** zero out the vector and the data in associated buffers. */
  public void zeroVector() {
    initValidityBuffer();
    viewBuffer.setZero(0, viewBuffer.capacity());
    clearDataBuffers();
  }

  /* zero out the validity buffer */
  private void initValidityBuffer() {
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  /** Reset the vector to initial state. Note that this method doesn't release any memory. */
  @Override
  public void reset() {
    zeroVector();
    lastSet = -1;
    valueCount = 0;
  }

  /** Close the vector and release the associated buffers. */
  @Override
  public void close() {
    clear();
  }

  /** Same as {@link #close()}. */
  @Override
  public void clear() {
    validityBuffer = releaseBuffer(validityBuffer);
    viewBuffer = releaseBuffer(viewBuffer);
    clearDataBuffers();
    lastSet = -1;
    valueCount = 0;
  }

  /** Release the data buffers and clear the list. */
  public void clearDataBuffers() {
    for (ArrowBuf buffer : dataBuffers) {
      releaseBuffer(buffer);
    }
    dataBuffers.clear();
  }

  /**
   * Get the inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   * @deprecated This API will be removed as the current implementations no longer support inner
   *     vectors.
   */
  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  /**
   * Initialize the children in schema for this Field. This operation is a NO-OP for scalar types
   * since they don't have any children.
   *
   * @param children the schema
   * @throws IllegalArgumentException if children is a non-empty list for scalar types.
   */
  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (!children.isEmpty()) {
      throw new IllegalArgumentException("primitive type vector cannot have children");
    }
  }

  /**
   * Get the inner child vectors.
   *
   * @return list of child vectors for complex types, empty list for scalar vector types
   */
  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Collections.emptyList();
  }

  /**
   * Load the buffers of this vector with provided source buffers. The caller manages the source
   * buffers and populates them before invoking this method.
   *
   * @param fieldNode the fieldNode indicating the value count
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    ArrowBuf bitBuf = ownBuffers.get(0);
    ArrowBuf viewBuf = ownBuffers.get(1);
    List<ArrowBuf> dataBufs = ownBuffers.subList(2, ownBuffers.size());

    this.clear();

    this.viewBuffer = viewBuf.getReferenceManager().retain(viewBuf, allocator);
    this.validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuf, allocator);

    for (ArrowBuf dataBuf : dataBufs) {
      this.dataBuffers.add(dataBuf.getReferenceManager().retain(dataBuf, allocator));
    }

    lastSet = fieldNode.getLength() - 1;
    valueCount = fieldNode.getLength();
  }

  /**
   * Get the buffers belonging to this vector.
   *
   * @return the inner buffers.
   */
  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(2 + dataBuffers.size());
    setReaderAndWriterIndex();
    result.add(validityBuffer);
    result.add(viewBuffer);
    // append data buffers
    result.addAll(dataBuffers);

    return result;
  }

  /** Set the reader and writer indexes for the inner buffers. */
  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    viewBuffer.readerIndex(0);
    if (valueCount == 0) {
      validityBuffer.writerIndex(0);
      viewBuffer.writerIndex(0);
    } else {
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      viewBuffer.writerIndex(valueCount * ELEMENT_SIZE);
    }
  }

  /** Same as {@link #allocateNewSafe()}. */
  @Override
  public void allocateNew() {
    allocateNew(lastValueAllocationSizeInBytes, lastValueCapacity);
  }

  /**
   * Allocate memory for the vector. We internally use a default value count of 4096 to allocate
   * memory for at least these many elements in the vector. See {@link #allocateNew(long, int)} for
   * allocating memory for specific number of elements in the vector.
   *
   * @return false if memory allocation fails, true otherwise.
   */
  @Override
  public boolean allocateNewSafe() {
    try {
      allocateNew(lastValueAllocationSizeInBytes, lastValueCapacity);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Allocate memory for the vector to support storing at least the provided number of elements in
   * the vector. This method must be called prior to using the ValueVector.
   *
   * @param totalBytes desired total memory capacity
   * @param valueCount the desired number of elements in the vector
   * @throws OutOfMemoryException if memory allocation fails
   */
  @Override
  public void allocateNew(long totalBytes, int valueCount) {
    assert totalBytes >= 0;

    checkDataBufferSize(totalBytes);

    /* we are doing a new allocation -- release the current buffers */
    clear();

    try {
      allocateBytes(totalBytes, valueCount);
    } catch (Exception e) {
      clear();
      throw e;
    }
  }

  @Override
  public void allocateNew(int valueCount) {
    allocateNew(lastValueAllocationSizeInBytes, valueCount);
  }

  /* Check if the data buffer size is within bounds. */
  private void checkDataBufferSize(long size) {
    if (size > MAX_BUFFER_SIZE || size < 0) {
      throw new OversizedAllocationException(
          "Memory required for vector "
              + "is ("
              + size
              + "), which is overflow or more than max allowed ("
              + MAX_BUFFER_SIZE
              + "). "
              + "You could consider using LargeVarCharVector/LargeVarBinaryVector for large strings/large bytes types");
    }
  }

  /* allocate the inner buffers */
  private void allocateBytes(final long valueBufferSize, final int valueCount) {
    /* allocate data buffer */
    viewBuffer = allocator.buffer(valueBufferSize);
    viewBuffer.readerIndex(0);

    validityBuffer = allocator.buffer((valueCount + 7) / 8);
    initValidityBuffer();

    lastValueCapacity = getValueCapacity();
    lastValueAllocationSizeInBytes = capAtMaxInt(viewBuffer.capacity());
  }

  /**
   * Resize the vector to increase the capacity. The internal behavior is to double the current
   * value capacity.
   */
  @Override
  public void reAlloc() {
    reallocViewBuffer();
    reallocViewDataBuffer();
    reallocValidityBuffer();
  }

  /**
   * Reallocate the view buffer. View Buffer stores the views for VIEWVARCHAR or VIEWVARBINARY
   * elements in the vector. The behavior is to double the size of buffer.
   *
   * @throws OversizedAllocationException if the desired new size is more than max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocViewBuffer() {
    long currentViewBufferCapacity = viewBuffer.capacity();

    long newAllocationSize = currentViewBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (lastValueAllocationSizeInBytes > 0) {
        newAllocationSize = lastValueAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_BYTE_COUNT * 2L;
      }
    }

    reallocViewBuffer(newAllocationSize);
  }

  /** Reallocate the data buffer associated with view buffer. */
  public void reallocViewDataBuffer() {
    long currentDataBufferCapacity = 0;
    if (!dataBuffers.isEmpty()) {
      currentDataBufferCapacity = dataBuffers.get(dataBuffers.size() - 1).capacity();
    }

    long newAllocationSize = currentDataBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (lastValueAllocationSizeInBytes > 0) {
        newAllocationSize = lastValueAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_BYTE_COUNT * 2L;
      }
    }

    reallocViewDataBuffer(newAllocationSize);
  }

  /**
   * Reallocate the view buffer to given size. View Buffer stores the views for VIEWVARCHAR or
   * VIEWVARBINARY elements in the vector. The actual allocated size may be larger than the request
   * one because it will round up the provided value to the nearest power of two.
   *
   * @param desiredAllocSize the desired new allocation size
   * @throws OversizedAllocationException if the desired new size is more than max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocViewBuffer(long desiredAllocSize) {
    if (desiredAllocSize == 0) {
      return;
    }
    long newAllocationSize = CommonUtil.nextPowerOfTwo(desiredAllocSize);
    assert newAllocationSize >= 1;

    checkDataBufferSize(newAllocationSize);
    // for each set operation, we have to allocate 16 bytes
    // here we are adjusting the desired allocation-based allocation size
    // to align with the 16bytes requirement.
    newAllocationSize = roundUpToMultipleOf16(newAllocationSize);

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, viewBuffer, 0, viewBuffer.capacity());

    viewBuffer.getReferenceManager().release();
    viewBuffer = newBuf;
    lastValueAllocationSizeInBytes = viewBuffer.capacity();
  }

  /**
   * Reallocate the data buffer for views.
   *
   * @param desiredAllocSize allocation size in bytes
   */
  public void reallocViewDataBuffer(long desiredAllocSize) {
    if (desiredAllocSize == 0) {
      return;
    }

    if (dataBuffers.isEmpty()) {
      return;
    }

    ArrowBuf currentBuf = dataBuffers.get(dataBuffers.size() - 1);
    if (currentBuf.capacity() - currentBuf.writerIndex() >= desiredAllocSize) {
      return;
    }

    final long newAllocationSize = CommonUtil.nextPowerOfTwo(desiredAllocSize);
    assert newAllocationSize >= 1;

    checkDataBufferSize(newAllocationSize);

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    dataBuffers.add(newBuf);
  }

  /** Reallocate Validity buffer. */
  public void reallocValidityBuffer() {
    int targetValidityCount = capAtMaxInt((validityBuffer.capacity() * 8) * 2);
    if (targetValidityCount == 0) {
      if (lastValueCapacity > 0) {
        targetValidityCount = lastValueCapacity;
      } else {
        targetValidityCount = 2 * INITIAL_VALUE_ALLOCATION;
      }
    }

    long validityBufferSize = computeValidityBufferSize(targetValidityCount);

    final ArrowBuf newValidityBuffer = allocator.buffer(validityBufferSize);
    newValidityBuffer.setBytes(0, validityBuffer, 0, validityBuffer.capacity());
    newValidityBuffer.setZero(
        validityBuffer.capacity(), newValidityBuffer.capacity() - validityBuffer.capacity());
    validityBuffer.getReferenceManager().release();
    validityBuffer = newValidityBuffer;

    lastValueCapacity = getValueCapacity();
  }

  private long computeValidityBufferSize(int valueCount) {
    return (valueCount + 7) / 8;
  }

  /**
   * Get the size (number of bytes) of underlying view buffer.
   *
   * @return number of bytes in the view buffer
   */
  @Override
  public int getByteCapacity() {
    return capAtMaxInt(viewBuffer.capacity());
  }

  @Override
  public int sizeOfValueBuffer() {
    throw new UnsupportedOperationException(
        "sizeOfValueBuffer is not supported for BaseVariableWidthViewVector");
  }

  /**
   * Get the size (number of bytes) of underlying elements in the view buffer.
   *
   * @return number of bytes used by data in the view buffer
   */
  public int sizeOfViewBufferElements() {
    if (valueCount == 0) {
      return 0;
    }
    int totalSize = 0;
    for (int i = 0; i < valueCount; i++) {
      totalSize += getValueLength(i);
    }
    return totalSize;
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this vector.
   *
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    return getBufferSizeFor(this.valueCount);
  }

  /**
   * Get the potential buffer size for a particular number of records.
   *
   * @param valueCount desired number of elements in the vector
   * @return estimated size of underlying buffers if the vector holds a given number of elements
   */
  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    final int viewBufferSize = valueCount * ELEMENT_SIZE;
    final int dataBufferSize = getDataBufferSize();
    return validityBufferSize + viewBufferSize + dataBufferSize;
  }

  private int getDataBufferSize() {
    int dataBufferSize = 0;
    for (ArrowBuf buf : dataBuffers) {
      dataBufferSize += (int) buf.writerIndex();
    }
    return dataBufferSize;
  }

  /**
   * Get information about how this field is materialized.
   *
   * @return the field corresponding to this vector
   */
  @Override
  public Field getField() {
    return field;
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the
   * reference counts for this buffer, so it only should be used for in-context access. Also note
   * that this buffer changes regularly, thus external classes shouldn't hold a reference to it
   * (unless they change it).
   *
   * @param clear Whether to clear vector before returning, the buffers will still be refcounted but
   *     the returned array will be the only reference to them
   * @return The underlying {@link ArrowBuf buffers} that is used by this vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    final ArrowBuf[] buffers;
    setReaderAndWriterIndex();
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      final int dataBufferSize = dataBuffers.size();
      // validity and view buffers
      final int fixedBufferSize = 2;
      buffers = new ArrowBuf[fixedBufferSize + dataBufferSize];
      buffers[0] = validityBuffer;
      buffers[1] = viewBuffer;
      for (int i = fixedBufferSize; i < fixedBufferSize + dataBufferSize; i++) {
        buffers[i] = dataBuffers.get(i - fixedBufferSize);
      }
    }
    if (clear) {
      for (final ArrowBuf buffer : buffers) {
        buffer.getReferenceManager().retain();
      }
      clear();
    }
    return buffers;
  }

  /** Validate the scalar values held by this vector. */
  public void validateScalars() {
    // No validation by default.
  }

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   *
   * @param field The field materialized by this vector.
   * @param allocator allocator for the target vector
   * @param callBack not used
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(field, allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @param callBack not used
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(ref, allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   *
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getName(), allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   *
   * @param field The field materialized by this vector.
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(Field field, BufferAllocator allocator);

  /**
   * Transfer this vector's data to another vector. The memory associated with this vector is
   * transferred to the allocator of target vector for accounting and management purposes.
   *
   * @param target destination vector for transfer
   */
  public void transferTo(BaseVariableWidthViewVector target) {
    compareTypes(target, "transferTo");
    target.clear();
    target.validityBuffer = transferBuffer(validityBuffer, target.allocator);
    target.viewBuffer = transferBuffer(viewBuffer, target.allocator);
    target.dataBuffers = new ArrayList<>(dataBuffers.size());
    for (int i = 0; i < dataBuffers.size(); i++) {
      target.dataBuffers.add(transferBuffer(dataBuffers.get(i), target.allocator));
    }

    target.setLastSet(this.lastSet);
    if (this.valueCount > 0) {
      target.setValueCount(this.valueCount);
    }
    clear();
  }

  /**
   * Slice this vector at desired index and length and transfer the corresponding data to the target
   * vector.
   *
   * @param startIndex start position of the split in source vector.
   * @param length length of the split.
   * @param target destination vector
   */
  public void splitAndTransferTo(int startIndex, int length, BaseVariableWidthViewVector target) {
    Preconditions.checkArgument(
        startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
        "Invalid parameters startIndex: %s, length: %s for valueCount: %s",
        startIndex,
        length,
        valueCount);
    compareTypes(target, "splitAndTransferTo");
    target.clear();
    if (length > 0) {
      splitAndTransferValidityBuffer(startIndex, length, target);
      splitAndTransferViewBufferAndDataBuffer(startIndex, length, target);
      target.setLastSet(length - 1);
      target.setValueCount(length);
    }
  }

  /* allocate validity buffer */
  private void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    initValidityBuffer();
  }

  /*
   * Transfer the validity.
   */
  private void splitAndTransferValidityBuffer(
      int startIndex, int length, BaseVariableWidthViewVector target) {
    if (length <= 0) {
      return;
    }

    final int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    final int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    final int byteSizeTarget = getValidityBufferSizeFromCount(length);
    final int offset = startIndex % 8;

    if (offset == 0) {
      // slice
      if (target.validityBuffer != null) {
        target.validityBuffer.getReferenceManager().release();
      }
      final ArrowBuf slicedValidityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
      target.validityBuffer = transferBuffer(slicedValidityBuffer, target.allocator);
      return;
    }

    /* Copy data
     * When the first bit starts from the middle of a byte (offset != 0),
     * copy data from src BitVector.
     * Each byte in the target is composed by a part in i-th byte,
     * another part in (i+1)-th byte.
     */
    target.allocateValidityBuffer(byteSizeTarget);

    for (int i = 0; i < byteSizeTarget - 1; i++) {
      byte b1 =
          BitVectorHelper.getBitsFromCurrentByte(this.validityBuffer, firstByteSource + i, offset);
      byte b2 =
          BitVectorHelper.getBitsFromNextByte(this.validityBuffer, firstByteSource + i + 1, offset);

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
      byte b1 =
          BitVectorHelper.getBitsFromCurrentByte(
              this.validityBuffer, firstByteSource + byteSizeTarget - 1, offset);
      byte b2 =
          BitVectorHelper.getBitsFromNextByte(
              this.validityBuffer, firstByteSource + byteSizeTarget, offset);

      target.validityBuffer.setByte(byteSizeTarget - 1, b1 + b2);
    } else {
      byte b1 =
          BitVectorHelper.getBitsFromCurrentByte(
              this.validityBuffer, firstByteSource + byteSizeTarget - 1, offset);
      target.validityBuffer.setByte(byteSizeTarget - 1, b1);
    }
  }

  /**
   * In split and transfer, the view buffer and the data buffer will be allocated. Then the values
   * will be copied from the source vector to the target vector. Allocation and setting are
   * preferred over transfer since the buf index and buf offset needs to be overwritten when large
   * strings are added.
   *
   * @param startIndex starting index
   * @param length number of elements to be copied
   * @param target target vector
   */
  private void splitAndTransferViewBufferAndDataBuffer(
      int startIndex, int length, BaseVariableWidthViewVector target) {
    if (length == 0) {
      return;
    }

    if (target.viewBuffer != null) {
      target.viewBuffer.getReferenceManager().release();
    }

    // allocate target view buffer
    target.viewBuffer = target.allocator.buffer(length * ELEMENT_SIZE);

    for (int i = startIndex; i < startIndex + length; i++) {
      final int stringLength = getValueLength(i);

      // keeping track of writing index in the target view buffer
      int writePosition = (i - startIndex) * ELEMENT_SIZE;
      // keeping track of reading index in the source view buffer
      int readPosition = i * ELEMENT_SIZE;

      // set length
      target.viewBuffer.setInt(writePosition, stringLength);

      if (stringLength <= INLINE_SIZE) {
        // handle inline buffer
        writePosition += LENGTH_WIDTH;
        readPosition += LENGTH_WIDTH;
        // set data by copying the required portion from the source buffer
        target.viewBuffer.setBytes(writePosition, viewBuffer, readPosition, stringLength);
      } else {
        // handle non-inline buffer
        final int readBufIndex =
            viewBuffer.getInt(((long) i * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
        final int readBufOffset =
            viewBuffer.getInt(
                ((long) i * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
        final ArrowBuf dataBuf = dataBuffers.get(readBufIndex);

        // allocate data buffer
        ArrowBuf currentDataBuf = target.allocateOrGetLastDataBuffer(stringLength);
        final long currentOffSet = currentDataBuf.writerIndex();

        writePosition += LENGTH_WIDTH;
        readPosition += LENGTH_WIDTH;
        // set prefix
        target.viewBuffer.setBytes(writePosition, viewBuffer, readPosition, PREFIX_WIDTH);
        writePosition += PREFIX_WIDTH;
        // set buf id
        target.viewBuffer.setInt(writePosition, target.dataBuffers.size() - 1);
        writePosition += BUF_INDEX_WIDTH;
        // set offset
        target.viewBuffer.setInt(writePosition, (int) currentOffSet);

        currentDataBuf.setBytes(currentOffSet, dataBuf, readBufOffset, stringLength);
        currentDataBuf.writerIndex(currentOffSet + stringLength);
      }
    }
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |                common getters and setters                      |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Get the number of elements that are null in the vector.
   *
   * @return the number of null elements.
   */
  @Override
  public int getNullCount() {
    return BitVectorHelper.getNullCount(validityBuffer, valueCount);
  }

  /**
   * Check if the given index is within the current value capacity of the vector.
   *
   * @param index position to check
   * @return true if the index is within the current value capacity
   */
  public boolean isSafe(int index) {
    return index < getValueCapacity();
  }

  /**
   * Check if an element at given index is null.
   *
   * @param index position of an element
   * @return true if an element at given index is null
   */
  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index position of an element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Get the value count of vector. This will always be zero unless setValueCount(int) has been
   * called prior to calling this.
   *
   * @return valueCount for the vector
   */
  @Override
  public int getValueCount() {
    return valueCount;
  }

  /**
   * Sets the value count for the vector.
   *
   * @param valueCount value count
   */
  @Override
  public void setValueCount(int valueCount) {
    assert valueCount >= 0;
    this.valueCount = valueCount;
    while (valueCount > getValueCapacity()) {
      reallocViewBuffer();
      reallocValidityBuffer();
    }
    lastSet = valueCount - 1;
    setReaderAndWriterIndex();
  }

  /**
   * Create holes in the vector upto the given index (exclusive). Holes will be created from the
   * current last-set position in the vector.
   *
   * @param index target index
   */
  @Override
  public void fillEmpties(int index) {
    handleSafe(index, EMPTY_BYTE_ARRAY.length);
    lastSet = index - 1;
  }

  /**
   * Set the index of the last non-null element in the vector. It is important to call this method
   * with appropriate value before calling {@link #setValueCount(int)}.
   *
   * @param value desired index of last non-null element.
   */
  @Override
  public void setLastSet(int value) {
    lastSet = value;
  }

  /**
   * Get the index of the last non-null element in the vector.
   *
   * @return index of the last non-null element
   */
  @Override
  public int getLastSet() {
    return lastSet;
  }

  /**
   * Mark the particular position in the vector as non-null.
   *
   * @param index position of the element.
   */
  @Override
  public void setIndexDefined(int index) {
    // We need to check and reallocate the validity buffer
    while (index >= getValueCapacity()) {
      reallocValidityBuffer();
    }
    BitVectorHelper.setBit(validityBuffer, index);
  }

  /**
   * Sets the value length for an element.
   *
   * @param index position of the element to set
   * @param length length of the element
   */
  @Override
  public void setValueLengthSafe(int index, int length) {
    assert index >= 0;
    handleSafe(index, length);
    lastSet = index;
  }

  /**
   * Get the length of the element at specified index.
   *
   * @param index position of an element to get
   * @return greater than length 0 for a non-null element, 0 otherwise
   */
  @Override
  public int getValueLength(int index) {
    assert index >= 0;
    if (index < 0 || index >= viewBuffer.capacity() / ELEMENT_SIZE) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + index);
    }
    if (isSet(index) == 0) {
      return 0;
    }
    return viewBuffer.getInt(((long) index * ELEMENT_SIZE));
  }

  /**
   * Set the variable length element at the specified index to the supplied byte array. This is same
   * as using {@link #set(int, byte[], int, int)} with start as Zero and length as #value.length
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  public void set(int index, byte[] value) {
    assert index >= 0;
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value, 0, value.length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, byte[])} except that it handles the case where index and length of a
   * new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  @Override
  public void setSafe(int index, byte[] value) {
    assert index >= 0;
    // check if the current index can be populated
    handleSafe(index, value.length);
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value, 0, value.length);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the supplied byte array.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   * @param start start index in an array of bytes
   * @param length length of data in an array of bytes
   */
  public void set(int index, byte[] value, int start, int length) {
    assert index >= 0;
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value, start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, byte[], int, int)} except that it handles the case where index and
   * length of a new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   * @param start start index in an array of bytes
   * @param length length of data in an array of bytes
   */
  public void setSafe(int index, byte[] value, int start, int length) {
    assert index >= 0;
    handleSafe(index, length);
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value, start, length);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the content in supplied ByteBuffer.
   *
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start index in ByteBuffer
   * @param length length of data in ByteBuffer
   */
  public void set(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value.array(), start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, ByteBuffer, int, int)} except that it handles the case where index and
   * length of a new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start index in ByteBuffer
   * @param length length of data in ByteBuffer
   */
  public void setSafe(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;
    handleSafe(index, length);
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value.array(), start, length);
    lastSet = index;
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index position of an element
   */
  @Override
  public void setNull(int index) {
    // We need to check and reallocate the validity buffer
    while (index >= getValueCapacity()) {
      reallocValidityBuffer();
    }
    BitVectorHelper.unsetBit(validityBuffer, index);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates whether the value
   * is NULL or not.
   *
   * @param index position of the new value
   * @param isSet Zero for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored in the vector
   */
  public void set(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;
    final int dataLength = end - start;
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    setBytes(index, buffer, start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case when index
   * is greater than or equal to current value capacity of the vector.
   *
   * @param index position of the new value
   * @param isSet Zero for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored in the vector
   */
  public void setSafe(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;
    final int dataLength = end - start;
    handleSafe(index, dataLength);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    setBytes(index, buffer, start, dataLength);
    lastSet = index;
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates whether the value
   * is NULL or not.
   *
   * @param index position of the new value
   * @param start start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored in the vector
   */
  public void set(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, buffer, start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case when index
   * is greater than or equal to current value capacity of the vector.
   *
   * @param index position of the new value
   * @param start start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored in the vector
   */
  public void setSafe(int index, int start, int length, ArrowBuf buffer) {
    assert index >= 0;
    handleSafe(index, length);
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, buffer, start, length);
    lastSet = index;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |                helper methods for setters                      |
  |                                                                |
  *----------------------------------------------------------------*/

  protected ArrowBuf allocateOrGetLastDataBuffer(int length) {
    long dataBufferSize;
    if (initialDataBufferSize > 0) {
      dataBufferSize = Math.max(initialDataBufferSize, length);
    } else {
      dataBufferSize = Math.max(lastValueAllocationSizeInBytes, length);
    }

    if (dataBuffers.isEmpty()
        || dataBuffers.get(dataBuffers.size() - 1).capacity()
                - dataBuffers.get(dataBuffers.size() - 1).writerIndex()
            < length) {
      ArrowBuf newBuf = allocator.buffer(dataBufferSize);
      dataBuffers.add(newBuf);
    }

    return dataBuffers.get(dataBuffers.size() - 1);
  }

  /**
   * This method is used to create a view buffer for a variable width vector. It handles both inline
   * and data buffers.
   *
   * <p>If the length of the value is less than or equal to {@link #INLINE_SIZE}, the value is
   * stored in the valueBuffer directly as an inline buffer. The valueBuffer stores the length of
   * the value followed by the value itself. If the length of the value is greater than {@link
   * #INLINE_SIZE}, a new buffer is allocated and added to dataBuffers to hold the value. The
   * viewBuffer in this case stores the length of the value, a prefix of the value, the index of the
   * new buffer in dataBuffers, and the offset of the value in the new buffer.
   *
   * @param index The index at which the new value will be inserted.
   * @param value The byte array that contains the data to be inserted.
   * @param start The start index in the byte array from where the data for the new value begins.
   * @param length The length of the data in the byte array that belongs to the new value.
   */
  protected final void setBytes(int index, byte[] value, int start, int length) {
    int writePosition = index * ELEMENT_SIZE;

    // to clear the memory segment of view being written to
    // this is helpful in case of overwriting the value
    viewBuffer.setZero(writePosition, ELEMENT_SIZE);

    if (length <= INLINE_SIZE) {
      // allocate inline buffer
      // set length
      viewBuffer.setInt(writePosition, length);
      writePosition += LENGTH_WIDTH;
      // set data
      viewBuffer.setBytes(writePosition, value, start, length);
    } else {
      // allocate data buffer
      ArrowBuf currentBuf = allocateOrGetLastDataBuffer(length);

      // set length
      viewBuffer.setInt(writePosition, length);
      writePosition += LENGTH_WIDTH;
      // set prefix
      viewBuffer.setBytes(writePosition, value, start, PREFIX_WIDTH);
      writePosition += PREFIX_WIDTH;
      // set buf id
      viewBuffer.setInt(writePosition, dataBuffers.size() - 1);
      writePosition += BUF_INDEX_WIDTH;
      // set offset
      viewBuffer.setInt(writePosition, (int) currentBuf.writerIndex());

      currentBuf.setBytes(currentBuf.writerIndex(), value, start, length);
      currentBuf.writerIndex(currentBuf.writerIndex() + length);
    }
  }

  /**
   * This method is used to create a view buffer for a variable width vector. Similar to {@link
   * #setBytes(int index, byte[] value, int start, int length)}
   *
   * @param index The index at which the new value will be inserted.
   * @param valueBuf The byte array that contains the data to be inserted.
   * @param start The start index in the byte array from where the data for the new value begins.
   * @param length The length of the data in the byte array that belongs to the new value.
   */
  protected final void setBytes(int index, ArrowBuf valueBuf, int start, int length) {
    int writePosition = index * ELEMENT_SIZE;

    // to clear the memory segment of view being written to
    // this is helpful in case of overwriting the value
    viewBuffer.setZero(writePosition, ELEMENT_SIZE);

    if (length <= INLINE_SIZE) {
      // allocate inline buffer
      // set length
      viewBuffer.setInt(writePosition, length);
      writePosition += LENGTH_WIDTH;
      // set data
      viewBuffer.setBytes(writePosition, valueBuf, start, length);
    } else {
      // allocate data buffer
      ArrowBuf currentBuf = allocateOrGetLastDataBuffer(length);

      // set length
      viewBuffer.setInt(writePosition, length);
      writePosition += LENGTH_WIDTH;
      // set prefix
      viewBuffer.setBytes(writePosition, valueBuf, start, PREFIX_WIDTH);
      writePosition += PREFIX_WIDTH;
      // set buf id
      viewBuffer.setInt(writePosition, dataBuffers.size() - 1);
      writePosition += BUF_INDEX_WIDTH;
      // set offset
      viewBuffer.setInt(writePosition, (int) currentBuf.writerIndex());

      currentBuf.setBytes(currentBuf.writerIndex(), valueBuf, start, length);
      currentBuf.writerIndex(currentBuf.writerIndex() + length);
    }
  }

  /**
   * Get the total length of the elements up to the given index.
   *
   * @param index The index of the element in the vector.
   * @return The total length up to the element at the given index.
   */
  public final int getTotalValueLengthUpToIndex(int index) {
    int totalLength = 0;
    for (int i = 0; i < index - 1; i++) {
      totalLength += getValueLength(i);
    }
    return totalLength;
  }

  protected final void handleSafe(int index, int dataLength) {
    final long lastSetCapacity = lastSet < 0 ? 0 : (long) index * ELEMENT_SIZE;
    final long targetCapacity = roundUpToMultipleOf16(lastSetCapacity + dataLength);
    // for views, we need each buffer with 16 byte alignment, so we need to check the last written
    // index
    // in the viewBuffer and allocate a new buffer which has 16 byte alignment for adding new
    // values.
    long writePosition = (long) index * ELEMENT_SIZE;
    if (viewBuffer.capacity() <= writePosition || viewBuffer.capacity() < targetCapacity) {
      /*
       * Everytime we want to increase the capacity of the viewBuffer, we need to make sure that the new capacity
       * meets 16 byte alignment.
       * If the targetCapacity is larger than the writePosition, we may not necessarily
       * want to allocate the targetCapacity to viewBuffer since when it is >={@link #INLINE_SIZE} either way
       * we are writing to the dataBuffer.
       */
      reallocViewBuffer(Math.max(writePosition, targetCapacity));
    }

    while (index >= getValueCapacity()) {
      reallocValidityBuffer();
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
    Preconditions.checkArgument(getMinorType() == from.getMinorType());
    if (from.isNull(fromIndex)) {
      BitVectorHelper.unsetBit(validityBuffer, thisIndex);
    } else {
      final int viewLength = from.getDataBuffer().getInt((long) fromIndex * ELEMENT_SIZE);
      BitVectorHelper.setBit(validityBuffer, thisIndex);
      final int start = thisIndex * ELEMENT_SIZE;
      final int copyStart = fromIndex * ELEMENT_SIZE;
      from.getDataBuffer().getBytes(start, viewBuffer, copyStart, ELEMENT_SIZE);
      if (viewLength > INLINE_SIZE) {
        final int bufIndex =
            from.getDataBuffer()
                .getInt(((long) fromIndex * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
        final int dataOffset =
            from.getDataBuffer()
                .getInt(
                    ((long) fromIndex * ELEMENT_SIZE)
                        + LENGTH_WIDTH
                        + PREFIX_WIDTH
                        + BUF_INDEX_WIDTH);
        final ArrowBuf dataBuf = ((BaseVariableWidthViewVector) from).dataBuffers.get(bufIndex);
        final ArrowBuf thisDataBuf = allocateOrGetLastDataBuffer(viewLength);
        thisDataBuf.setBytes(thisDataBuf.writerIndex(), dataBuf, dataOffset, viewLength);
        thisDataBuf.writerIndex(thisDataBuf.writerIndex() + viewLength);
      }
    }
    lastSet = thisIndex;
  }

  /**
   * Same as {@link #copyFrom(int, int, ValueVector)} except that it handles the case when the
   * capacity of the vector needs to be expanded before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    Preconditions.checkArgument(getMinorType() == from.getMinorType());
    if (from.isNull(fromIndex)) {
      handleSafe(thisIndex, 0);
      BitVectorHelper.unsetBit(validityBuffer, thisIndex);
    } else {
      final int viewLength = from.getDataBuffer().getInt((long) fromIndex * ELEMENT_SIZE);
      handleSafe(thisIndex, viewLength);
      BitVectorHelper.setBit(validityBuffer, thisIndex);
      final int start = thisIndex * ELEMENT_SIZE;
      final int copyStart = fromIndex * ELEMENT_SIZE;
      from.getDataBuffer().getBytes(start, viewBuffer, copyStart, ELEMENT_SIZE);
      if (viewLength > INLINE_SIZE) {
        final int bufIndex =
            from.getDataBuffer()
                .getInt(((long) fromIndex * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
        final int dataOffset =
            from.getDataBuffer()
                .getInt(
                    ((long) fromIndex * ELEMENT_SIZE)
                        + LENGTH_WIDTH
                        + PREFIX_WIDTH
                        + BUF_INDEX_WIDTH);
        final ArrowBuf dataBuf = ((BaseVariableWidthViewVector) from).dataBuffers.get(bufIndex);
        final ArrowBuf thisDataBuf = allocateOrGetLastDataBuffer(viewLength);
        thisDataBuf.setBytes(thisDataBuf.writerIndex(), dataBuf, dataOffset, viewLength);
        thisDataBuf.writerIndex(thisDataBuf.writerIndex() + viewLength);
      }
    }
    lastSet = thisIndex;
  }

  @Override
  public ArrowBufPointer getDataPointer(int index) {
    return getDataPointer(index, new ArrowBufPointer());
  }

  @Override
  public ArrowBufPointer getDataPointer(int index, ArrowBufPointer reuse) {
    if (isNull(index)) {
      reuse.set(null, 0, 0);
    } else {
      int length = getValueLength(index);
      if (length < INLINE_SIZE) {
        int start = index * ELEMENT_SIZE + LENGTH_WIDTH;
        reuse.set(viewBuffer, start, length);
      } else {
        final int bufIndex =
            viewBuffer.getInt(((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
        ArrowBuf dataBuf = dataBuffers.get(bufIndex);
        reuse.set(dataBuf, 0, length);
      }
    }
    return reuse;
  }

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    if (isNull(index)) {
      return ArrowBufPointer.NULL_HASH_CODE;
    }
    final int length = getValueLength(index);
    if (length < INLINE_SIZE) {
      int start = index * ELEMENT_SIZE + LENGTH_WIDTH;
      return ByteFunctionHelpers.hash(hasher, this.getDataBuffer(), start, start + length);
    } else {
      final int bufIndex =
          viewBuffer.getInt(((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      final int dataOffset =
          viewBuffer.getInt(
              ((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      ArrowBuf dataBuf = dataBuffers.get(bufIndex);
      return ByteFunctionHelpers.hash(hasher, dataBuf, dataOffset, dataOffset + length);
    }
  }

  /**
   * Retrieves the data of a variable-width element at a given index in the vector.
   *
   * <p>If the length of the data is greater than {@link #INLINE_SIZE}, the data is stored in an
   * inline buffer. The method retrieves the buffer index and data offset from the viewBuffer, and
   * then retrieves the data from the corresponding buffer in the dataBuffers list.
   *
   * <p>If the length of the data is less than or equal to {@link #INLINE_SIZE}, the data is stored
   * directly in the viewBuffer. The method retrieves the data directly from the viewBuffer.
   *
   * @param index position of the element in the vector
   * @return byte array containing the data of the element
   */
  protected byte[] getData(int index) {
    final int dataLength = getValueLength(index);
    byte[] result = new byte[dataLength];
    if (dataLength > INLINE_SIZE) {
      // data is in the data buffer
      // get buffer index
      final int bufferIndex =
          viewBuffer.getInt(((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      // get data offset
      final int dataOffset =
          viewBuffer.getInt(
              ((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      dataBuffers.get(bufferIndex).getBytes(dataOffset, result, 0, dataLength);
    } else {
      // data is in the view buffer
      viewBuffer.getBytes((long) index * ELEMENT_SIZE + BUF_INDEX_WIDTH, result, 0, dataLength);
    }
    return result;
  }

  protected void getData(int index, ReusableBuffer<?> buffer) {
    final int dataLength = getValueLength(index);
    if (dataLength > INLINE_SIZE) {
      // data is in the data buffer
      // get buffer index
      final int bufferIndex =
          viewBuffer.getInt(((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      // get data offset
      final int dataOffset =
          viewBuffer.getInt(
              ((long) index * ELEMENT_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      ArrowBuf dataBuf = dataBuffers.get(bufferIndex);
      buffer.set(dataBuf, dataOffset, dataLength);
    } else {
      // data is in the value buffer
      buffer.set(viewBuffer, ((long) index * ELEMENT_SIZE) + BUF_INDEX_WIDTH, dataLength);
    }
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  /**
   * Retrieves the export buffer count for the C Data Interface.
   *
   * <p>For Variadic types, an additional buffer is kept to store the size of each variadic buffer
   * since that information cannot be retrieved in the C Data import.
   *
   * <p>In the C Data Interface, the binary view import expects at least three buffers. The variadic
   * size buffer is merely allocated to determine the number of elements per each variadic buffer,
   * and it is not part of the imported data.
   *
   * <p>The count is set to 3 + dataBuffers.size(). Three is formed by validity, view, and variadic
   * size buffer.
   *
   * @return the number of buffers to be exported
   */
  @Override
  public int getExportedCDataBufferCount() {
    return 3 + dataBuffers.size();
  }

  /**
   * Get the data buffer of the vector. Note that an additional buffer is appended to store the size
   * of each variadic buffer's size.
   *
   * @param buffers list of buffers to be exported
   * @param buffersPtr buffer to store the pointers to the exported buffers
   * @param nullValue null value
   */
  @Override
  public void exportCDataBuffers(List<ArrowBuf> buffers, ArrowBuf buffersPtr, long nullValue) {
    exportBuffer(validityBuffer, buffers, buffersPtr, nullValue, true);
    exportBuffer(viewBuffer, buffers, buffersPtr, nullValue, true);

    // allocating additional space to keep the number of variadic buffers
    ArrowBuf variadicSizeBuffer = allocator.buffer((long) Long.BYTES * dataBuffers.size());
    // variadicSizeBuffer.setZero(0, variadicSizeBuffer.capacity());
    // export data buffers
    for (int i = 0; i < dataBuffers.size(); i++) {
      ArrowBuf dataBuf = dataBuffers.get(i);
      // calculate sizes for variadic size buffer
      variadicSizeBuffer.setLong((long) i * Long.BYTES, dataBuf.capacity());
      exportBuffer(dataBuf, buffers, buffersPtr, nullValue, true);
    }
    // export variadic size buffer
    exportBuffer(variadicSizeBuffer, buffers, buffersPtr, nullValue, false);
  }
}
