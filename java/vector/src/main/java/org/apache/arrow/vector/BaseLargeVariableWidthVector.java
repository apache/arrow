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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
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
 * BaseLargeVariableWidthVector is a base class providing functionality for large strings/large bytes types.
 */
public abstract class BaseLargeVariableWidthVector extends BaseValueVector
    implements VariableWidthVector, FieldVector, VectorDefinitionSetter {
  private static final int DEFAULT_RECORD_BYTE_COUNT = 12;
  private static final int INITIAL_BYTE_COUNT = INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT;
  private int lastValueCapacity;
  private long lastValueAllocationSizeInBytes;

  /* protected members */
  public static final int OFFSET_WIDTH = 8; /* 8 byte unsigned int to track offsets */
  protected static final byte[] emptyByteArray = new byte[]{};
  protected ArrowBuf validityBuffer;
  protected ArrowBuf valueBuffer;
  protected ArrowBuf offsetBuffer;
  protected int valueCount;
  protected int lastSet;
  protected final Field field;

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use for creating/resizing buffers
   */
  public BaseLargeVariableWidthVector(Field field, final BufferAllocator allocator) {
    super(allocator);
    this.field = field;
    lastValueAllocationSizeInBytes = INITIAL_BYTE_COUNT;
    // -1 because we require one extra slot for the offset array.
    lastValueCapacity = INITIAL_VALUE_ALLOCATION - 1;
    valueCount = 0;
    lastSet = -1;
    offsetBuffer = allocator.getEmpty();
    validityBuffer = allocator.getEmpty();
    valueBuffer = allocator.getEmpty();
  }

  @Override
  public String getName() {
    return field.getName();
  }

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
    checkDataBufferSize(size);
    computeAndCheckOffsetsBufferSize(valueCount);
    lastValueAllocationSizeInBytes = size;
    lastValueCapacity = valueCount;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   * @param valueCount desired number of elements in the vector
   * @param density average number of bytes per variable width element
   */
  @Override
  public void setInitialCapacity(int valueCount, double density) {
    long size = Math.max((long) (valueCount * density), 1L);
    checkDataBufferSize(size);
    computeAndCheckOffsetsBufferSize(valueCount);
    lastValueAllocationSizeInBytes = size;
    lastValueCapacity = valueCount;
  }

  /**
   * Get the density of this ListVector.
   * @return density
   */
  public double getDensity() {
    if (valueCount == 0) {
      return 0.0D;
    }
    final long startOffset = getStartOffset(0);
    final long endOffset = getStartOffset(valueCount);
    final double totalListSize = endOffset - startOffset;
    return totalListSize / valueCount;
  }

  /**
   * Get the current capacity which does not exceed either validity buffer or offset buffer.
   * Note: Here the `getValueCapacity` has no relationship with the value buffer.
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    final long offsetValueCapacity = Math.max(getOffsetBufferValueCapacity() - 1, 0);
    return capAtMaxInt(Math.min(offsetValueCapacity, getValidityBufferValueCapacity()));
  }

  private long getValidityBufferValueCapacity() {
    return validityBuffer.capacity() * 8;
  }

  private long getOffsetBufferValueCapacity() {
    return offsetBuffer.capacity() / OFFSET_WIDTH;
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
  @Override
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
   * Same as {@link #close()}.
   */
  @Override
  public void clear() {
    validityBuffer = releaseBuffer(validityBuffer);
    valueBuffer = releaseBuffer(valueBuffer);
    offsetBuffer = releaseBuffer(offsetBuffer);
    lastSet = -1;
    valueCount = 0;
  }

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   */
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
      throw new IllegalArgumentException("primitive type vector cannot have children");
    }
  }

  /**
   * Get the inner child vectors.
   * @return list of child vectors for complex types, empty list for scalar vector types
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

    validityBuffer.getReferenceManager().release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    offsetBuffer.getReferenceManager().release();
    offsetBuffer = offBuffer.getReferenceManager().retain(offBuffer, allocator);
    valueBuffer.getReferenceManager().release();
    valueBuffer = dataBuffer.getReferenceManager().retain(dataBuffer, allocator);

    lastSet = fieldNode.getLength() - 1;
    valueCount = fieldNode.getLength();
  }

  /**
   * Get the buffers belonging to this vector.
   * @return the inner buffers.
   */
  @Override
  public List<ArrowBuf> getFieldBuffers() {
    // before flight/IPC, we must bring the vector to a consistent state.
    // this is because, it is possible that the offset buffers of some trailing values
    // are not updated. this may cause some data in the data buffer being lost.
    // for details, please see TestValueVector#testUnloadVariableWidthVector.
    fillHoles(valueCount);

    List<ArrowBuf> result = new ArrayList<>(3);
    setReaderAndWriterIndex();
    result.add(validityBuffer);
    result.add(offsetBuffer);
    result.add(valueBuffer);

    return result;
  }

  /**
   * Export the buffers of the fields for C Data Interface. This method traverse the buffers and
   * export buffer and buffer's memory address into a list of buffers and a pointer to the list of buffers.
   */
  @Override
  public void exportCDataBuffers(List<ArrowBuf> buffers, ArrowBuf buffersPtr, long nullValue) {
    // before flight/IPC, we must bring the vector to a consistent state.
    // this is because, it is possible that the offset buffers of some trailing values
    // are not updated. this may cause some data in the data buffer being lost.
    // for details, please see TestValueVector#testUnloadVariableWidthVector.
    fillHoles(valueCount);

    exportBuffer(validityBuffer, buffers, buffersPtr, nullValue, true);

    if (offsetBuffer.capacity() == 0) {
      // Empty offset buffer is allowed for historical reason.
      // To export it through C Data interface, we need to allocate a buffer with one offset.
      // We set `retain = false` to explicitly not increase the ref count for the exported buffer.
      // The ref count of the newly created buffer (i.e., 1) already represents the usage
      // at imported side.
      exportBuffer(allocateOffsetBuffer(OFFSET_WIDTH), buffers, buffersPtr, nullValue, false);
    } else {
      exportBuffer(offsetBuffer, buffers, buffersPtr, nullValue, true);
    }

    exportBuffer(valueBuffer, buffers, buffersPtr, nullValue, true);
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
      final long lastDataOffset = getStartOffset(valueCount);
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      offsetBuffer.writerIndex((long) (valueCount + 1) * OFFSET_WIDTH);
      valueBuffer.writerIndex(lastDataOffset);
    }
  }

  /**
   * Same as {@link #allocateNewSafe()}.
   */
  @Override
  public void allocateNew() {
    allocateNew(lastValueAllocationSizeInBytes, lastValueCapacity);
  }

  /**
   * Allocate memory for the vector. We internally use a default value count
   * of 4096 to allocate memory for at least these many elements in the
   * vector. See {@link #allocateNew(long, int)} for allocating memory for specific
   * number of elements in the vector.
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
   * Allocate memory for the vector to support storing at least the provided number of
   * elements in the vector. This method must be called prior to using the ValueVector.
   *
   * @param totalBytes desired total memory capacity
   * @param valueCount the desired number of elements in the vector
   * @throws org.apache.arrow.memory.OutOfMemoryException if memory allocation fails
   */
  @Override
  public void allocateNew(long totalBytes, int valueCount) {
    assert totalBytes >= 0;

    checkDataBufferSize(totalBytes);
    computeAndCheckOffsetsBufferSize(valueCount);

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
    if (size > MAX_ALLOCATION_SIZE || size < 0) {
      throw new OversizedAllocationException("Memory required for vector " +
          " is (" + size + "), which is more than max allowed (" + MAX_ALLOCATION_SIZE + ")");
    }
  }

  /**
   * Compute the buffer size required for 'valueCount' offsets and validity, and check if it's
   * within bounds.
   */
  private long computeAndCheckOffsetsBufferSize(int valueCount) {
    /* to track the end offset of last data element in vector, we need
     * an additional slot in offset buffer.
     */
    final long size = computeCombinedBufferSize(valueCount + 1, OFFSET_WIDTH);
    if (size > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Memory required for vector capacity " +
          valueCount +
          " is (" + size + "), which is more than max allowed (" + MAX_ALLOCATION_SIZE + ")");
    }
    return size;
  }

  /* allocate the inner buffers */
  private void allocateBytes(final long valueBufferSize, final int valueCount) {
    /* allocate data buffer */
    long curSize = valueBufferSize;
    valueBuffer = allocator.buffer(curSize);
    valueBuffer.readerIndex(0);

    /* allocate offset buffer and validity buffer */
    DataAndValidityBuffers buffers = allocFixedDataAndValidityBufs(valueCount + 1, OFFSET_WIDTH);
    offsetBuffer = buffers.getDataBuf();
    validityBuffer = buffers.getValidityBuf();
    initOffsetBuffer();
    initValidityBuffer();

    lastValueCapacity = getValueCapacity();
    lastValueAllocationSizeInBytes = capAtMaxInt(valueBuffer.capacity());
  }

  /* allocate offset buffer */
  private ArrowBuf allocateOffsetBuffer(final long size) {
    ArrowBuf offsetBuffer = allocator.buffer(size);
    offsetBuffer.readerIndex(0);
    initOffsetBuffer();
    return offsetBuffer;
  }

  /* allocate validity buffer */
  private void allocateValidityBuffer(final long size) {
    validityBuffer = allocator.buffer(size);
    validityBuffer.readerIndex(0);
    initValidityBuffer();
  }

  /**
   * Resize the vector to increase the capacity. The internal behavior is to
   * double the current value capacity.
   */
  @Override
  public void reAlloc() {
    reallocDataBuffer();
    reallocValidityAndOffsetBuffers();
  }

  /**
   * Reallocate the data buffer. Data Buffer stores the actual data for
   * LARGEVARCHAR or LARGEVARBINARY elements in the vector. The behavior is to double
   * the size of buffer.
   * @throws OversizedAllocationException if the desired new size is more than
   *                                      max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocDataBuffer() {
    final long currentBufferCapacity = valueBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (lastValueAllocationSizeInBytes > 0) {
        newAllocationSize = lastValueAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_BYTE_COUNT * 2;
      }
    }
    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    checkDataBufferSize(newAllocationSize);

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, valueBuffer, 0, currentBufferCapacity);
    valueBuffer.getReferenceManager().release();
    valueBuffer = newBuf;
    lastValueAllocationSizeInBytes = valueBuffer.capacity();
  }

  /**
   * Reallocate the validity and offset buffers for this vector. Validity
   * buffer is used to track the NULL or NON-NULL nature of elements in
   * the vector and offset buffer is used to store the lengths of variable
   * width elements in the vector.
   *
   * <p>Note that data buffer for variable length vectors moves independent
   * of the companion validity and offset buffers. This is in
   * contrast to what we have for fixed width vectors.
   *
   * <p>So even though we may have setup an initial capacity of 1024
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
    int targetOffsetCount = capAtMaxInt((offsetBuffer.capacity() / OFFSET_WIDTH) * 2);
    if (targetOffsetCount == 0) {
      if (lastValueCapacity > 0) {
        targetOffsetCount = (lastValueCapacity + 1);
      } else {
        targetOffsetCount = 2 * (INITIAL_VALUE_ALLOCATION + 1);
      }
    }
    computeAndCheckOffsetsBufferSize(targetOffsetCount);

    DataAndValidityBuffers buffers = allocFixedDataAndValidityBufs(targetOffsetCount, OFFSET_WIDTH);
    final ArrowBuf newOffsetBuffer = buffers.getDataBuf();
    newOffsetBuffer.setBytes(0, offsetBuffer, 0, offsetBuffer.capacity());
    newOffsetBuffer.setZero(offsetBuffer.capacity(), newOffsetBuffer.capacity() - offsetBuffer.capacity());
    offsetBuffer.getReferenceManager().release();
    offsetBuffer = newOffsetBuffer;

    final ArrowBuf newValidityBuffer = buffers.getValidityBuf();
    newValidityBuffer.setBytes(0, validityBuffer, 0, validityBuffer.capacity());
    newValidityBuffer.setZero(validityBuffer.capacity(), newValidityBuffer.capacity() - validityBuffer.capacity());
    validityBuffer.getReferenceManager().release();
    validityBuffer = newValidityBuffer;

    lastValueCapacity = getValueCapacity();
  }

  /**
   * Get the size (number of bytes) of underlying data buffer.
   * @return number of bytes in the data buffer
   */
  @Override
  public int getByteCapacity() {
    return capAtMaxInt(valueBuffer.capacity());
  }

  @Override
  public int sizeOfValueBuffer() {
    if (valueCount == 0) {
      return 0;
    }
    return capAtMaxInt(getStartOffset(valueCount));
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this
   * vector.
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

    final long validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    final long offsetBufferSize = (long) (valueCount + 1) * OFFSET_WIDTH;
    /* get the end offset for this valueCount */
    final long dataBufferSize = getStartOffset(valueCount);
    return capAtMaxInt(validityBufferSize + offsetBufferSize + dataBufferSize);
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
        buffer.getReferenceManager().retain();
      }
      clear();
    }
    return buffers;
  }

  /**
   * Validate the scalar values held by this vector.
   */
  public void validateScalars() {
    // No validation by default.
  }

  /**
   * Construct a transfer pair of this vector and another vector of same type.
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
   * Construct a transfer pair of this vector and another vector of same type.
   * @param field The field materialized by this vector
   * @param allocator allocator for the target vector
   * @param callBack not used
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return getTransferPair(field, allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getName(), allocator);
  }


  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

  /**
   * Construct a transfer pair of this vector and another vector of same type.
   * @param field The field materialized by this vector
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(Field field, BufferAllocator allocator);

  /**
   * Transfer this vector's data to another vector. The memory associated
   * with this vector is transferred to the allocator of target vector
   * for accounting and management purposes.
   * @param target destination vector for transfer
   */
  public void transferTo(BaseLargeVariableWidthVector target) {
    compareTypes(target, "transferTo");
    target.clear();
    target.validityBuffer = transferBuffer(validityBuffer, target.allocator);
    target.valueBuffer = transferBuffer(valueBuffer, target.allocator);
    target.offsetBuffer = transferBuffer(offsetBuffer, target.allocator);
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
                                 BaseLargeVariableWidthVector target) {
    Preconditions.checkArgument(startIndex >= 0 && startIndex < valueCount,
        "Invalid startIndex: %s", startIndex);
    Preconditions.checkArgument(startIndex + length <= valueCount,
        "Invalid length: %s", length);
    compareTypes(target, "splitAndTransferTo");
    target.clear();
    splitAndTransferValidityBuffer(startIndex, length, target);
    splitAndTransferOffsetBuffer(startIndex, length, target);
    target.setLastSet(length - 1);
    if (length > 0) {
      target.setValueCount(length);
    }
  }

  /**
   * Transfer the offsets along with data. Unlike the data buffer, we cannot simply
   * slice the offset buffer for split and transfer. The reason is that offsets
   * in the target vector have to be adjusted and made relative to the staring
   * offset in source vector from the start index of split. This is why, we
   * need to explicitly allocate the offset buffer and set the adjusted offsets
   * in the target vector.
   */
  private void splitAndTransferOffsetBuffer(int startIndex, int length, BaseLargeVariableWidthVector target) {
    final long start = getStartOffset(startIndex);
    final long end = getStartOffset(startIndex + length);
    final long dataLength = end - start;
    target.offsetBuffer = target.allocateOffsetBuffer((long) (length + 1) * OFFSET_WIDTH);
    for (int i = 0; i < length + 1; i++) {
      final long relativeSourceOffset = getStartOffset(startIndex + i) - start;
      target.offsetBuffer.setLong((long) i * OFFSET_WIDTH, relativeSourceOffset);
    }
    final ArrowBuf slicedBuffer = valueBuffer.slice(start, dataLength);
    target.valueBuffer = transferBuffer(slicedBuffer, target.allocator);
  }

  /*
   * Transfer the validity.
   */
  private void splitAndTransferValidityBuffer(int startIndex, int length,
                                              BaseLargeVariableWidthVector target) {
    int firstByteSource = BitVectorHelper.byteIndex(startIndex);
    int lastByteSource = BitVectorHelper.byteIndex(valueCount - 1);
    int byteSizeTarget = getValidityBufferSizeFromCount(length);
    int offset = startIndex % 8;

    if (length > 0) {
      if (offset == 0) {
        // slice
        if (target.validityBuffer != null) {
          target.validityBuffer.getReferenceManager().release();
        }
        target.validityBuffer = validityBuffer.slice(firstByteSource, byteSizeTarget);
        target.validityBuffer.getReferenceManager().retain();
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
   * Check if the given index is within the current value capacity
   * of the vector.
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
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  /**
   * Get the value count of vector. This will always be zero unless
   * setValueCount(int) has been called prior to calling this.
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
   * @param valueCount   value count
   */
  @Override
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
   * Mark the particular position in the vector as non-null.
   *
   * @param index position of the element.
   */
  @Override
  public void setIndexDefined(int index) {
    // We need to check and realloc both validity and offset buffer
    while (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    BitVectorHelper.setBit(validityBuffer, index);
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
    final long startOffset = getStartOffset(index);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
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
    final long startOffset = getStartOffset(index);
    final int dataLength = (int) (getEndOffset(index) - startOffset);
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
    BitVectorHelper.setBit(validityBuffer, index);
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
    handleSafe(index, value.length);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
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
    BitVectorHelper.setBit(validityBuffer, index);
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
    handleSafe(index, length);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    setBytes(index, value, start, length);
    lastSet = index;
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied ByteBuffer.
   *
   * @param index   position of the element to set
   * @param value   ByteBuffer with data
   * @param start   start index in ByteBuffer
   * @param length  length of data in ByteBuffer
   */
  public void set(int index, ByteBuffer value, int start, int length) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final long startOffset = getStartOffset(index);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
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
    handleSafe(index, length);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final long startOffset = getStartOffset(index);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
    valueBuffer.setBytes(startOffset, value, start, length);
    lastSet = index;
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index   position of element
   */
  @Override
  public void setNull(int index) {
    // We need to check and realloc both validity and offset buffer
    while (index >= getValueCapacity()) {
      reallocValidityAndOffsetBuffers();
    }
    BitVectorHelper.unsetBit(validityBuffer, index);
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
  public void set(int index, int isSet, long start, long end, ArrowBuf buffer) {
    assert index >= 0;
    final long dataLength = end - start;
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    final long startOffset = offsetBuffer.getLong((long) index * OFFSET_WIDTH);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, end);
    valueBuffer.setBytes(startOffset, buffer, start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, long, long, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param isSet 0 for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int isSet, long start, long end, ArrowBuf buffer) {
    assert index >= 0;
    final long dataLength = end - start;
    handleSafe(index, (int) dataLength);
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    final long startOffset = offsetBuffer.getLong((long) index * OFFSET_WIDTH);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + dataLength);
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
  public void set(int index, long start, int length, ArrowBuf buffer) {
    assert index >= 0;
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final long startOffset = offsetBuffer.getLong((long) index * OFFSET_WIDTH);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, long, long, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param start start position of data in buffer
   * @param length length of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, long start, int length, ArrowBuf buffer) {
    assert index >= 0;
    handleSafe(index, length);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    final long startOffset = offsetBuffer.getLong((long) index * OFFSET_WIDTH);
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
    final ArrowBuf bb = buffer.slice(start, length);
    valueBuffer.setBytes(startOffset, bb);
    lastSet = index;
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |                helper methods for setters                      |
   |                                                                |
   *----------------------------------------------------------------*/


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
    final long startOffset = getStartOffset(index);
    /* set new end offset */
    offsetBuffer.setLong((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
    /* store the var length data in value buffer */
    valueBuffer.setBytes(startOffset, value, start, length);
  }

  /**
   * Gets the starting offset of a record, given its index.
   * @param index index of the record.
   * @return the starting offset of the record.
   */
  protected final long getStartOffset(int index) {
    return offsetBuffer.getLong((long) index * OFFSET_WIDTH);
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
    final long startOffset = lastSet < 0 ? 0L : getStartOffset(lastSet + 1);
    while (valueBuffer.capacity() < (startOffset + dataLength)) {
      reallocDataBuffer();
    }
  }

  /**
   * Method used by Json Writer to read a variable width element from
   * the variable width vector and write to Json.
   *
   * <p>This method should not be used externally.
   *
   * @param data buffer storing the variable width vector elements
   * @param offset buffer storing the offsets of variable width vector elements
   * @param index position of the element in the vector
   * @return array of bytes
   */
  public static byte[] get(final ArrowBuf data, final ArrowBuf offset, int index) {
    final long currentStartOffset = offset.getLong((long) index * OFFSET_WIDTH);
    final int dataLength =
        (int) (offset.getLong((long) (index + 1) * OFFSET_WIDTH) - currentStartOffset);
    final byte[] result = new byte[dataLength];
    data.getBytes(currentStartOffset, result, 0, dataLength);
    return result;
  }

  /**
   * Method used by Json Reader to explicitly set the offsets of the variable
   * width vector data. The method takes care of allocating the memory for
   * offsets if the caller hasn't done so.
   *
   * <p>This method should not be used externally.
   *
   * @param buffer ArrowBuf to store offsets for variable width elements
   * @param allocator memory allocator
   * @param valueCount number of elements
   * @param index position of the element
   * @param value offset of the element
   * @return buffer holding the offsets
   */
  public static ArrowBuf set(ArrowBuf buffer, BufferAllocator allocator,
                             int valueCount, int index, long value) {
    if (buffer == null) {
      buffer = allocator.buffer((long) valueCount * OFFSET_WIDTH);
    }
    buffer.setLong((long) index * OFFSET_WIDTH, value);
    if (index == (valueCount - 1)) {
      buffer.writerIndex((long) valueCount * OFFSET_WIDTH);
    }

    return buffer;
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular
   * position in this vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    if (from.isNull(fromIndex)) {
      fillHoles(thisIndex);
      BitVectorHelper.unsetBit(this.validityBuffer, thisIndex);
      final long copyStart = offsetBuffer.getLong((long) thisIndex * OFFSET_WIDTH);
      offsetBuffer.setLong((long) (thisIndex + 1) * OFFSET_WIDTH, copyStart);
    } else {
      final long start = from.getOffsetBuffer().getLong((long) fromIndex * OFFSET_WIDTH);
      final long end = from.getOffsetBuffer().getLong((long) (fromIndex + 1) * OFFSET_WIDTH);
      final long length = end - start;
      fillHoles(thisIndex);
      BitVectorHelper.setBit(this.validityBuffer, thisIndex);
      final long copyStart = getStartOffset(thisIndex);
      from.getDataBuffer().getBytes(start, this.valueBuffer, copyStart, (int) length);
      offsetBuffer.setLong((long) (thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
    }
    lastSet = thisIndex;
  }

  /**
   * Same as {@link #copyFrom(int, int, ValueVector)} except that
   * it handles the case when the capacity of the vector needs to be expanded
   * before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    if (from.isNull(fromIndex)) {
      handleSafe(thisIndex, 0);
      fillHoles(thisIndex);
      BitVectorHelper.unsetBit(this.validityBuffer, thisIndex);
      final long copyStart = offsetBuffer.getLong((long) thisIndex * OFFSET_WIDTH);
      offsetBuffer.setLong((long) (thisIndex + 1) * OFFSET_WIDTH, copyStart);
    } else {
      final long start = from.getOffsetBuffer().getLong((long) fromIndex * OFFSET_WIDTH);
      final long end = from.getOffsetBuffer().getLong((long) (fromIndex + 1) * OFFSET_WIDTH);
      final int length = (int) (end - start);
      handleSafe(thisIndex, length);
      fillHoles(thisIndex);
      BitVectorHelper.setBit(this.validityBuffer, thisIndex);
      final long copyStart = getStartOffset(thisIndex);
      from.getDataBuffer().getBytes(start, this.valueBuffer, copyStart, length);
      offsetBuffer.setLong((long) (thisIndex + 1) * OFFSET_WIDTH, copyStart + length);
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
      long offset = getStartOffset(index);
      int length = (int) (getEndOffset(index) - offset);
      reuse.set(valueBuffer, offset, length);
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
    final long start = getStartOffset(index);
    final long end = getEndOffset(index);
    return ByteFunctionHelpers.hash(hasher, this.getDataBuffer(), start, end);
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  protected final long getEndOffset(int index) {
    return offsetBuffer.getLong((long) (index + 1) * OFFSET_WIDTH);
  }
}
