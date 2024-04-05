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
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

/**
 * BaseVariableWidthViewVector is a base class providing functionality for strings/bytes types in view format.
 *
 */
public abstract class BaseVariableWidthViewVector extends AbstractVariableWidthVector {
  private static final int DEFAULT_RECORD_BYTE_COUNT = 16;
  public static final int INITIAL_VIEW_VALUE_ALLOCATION = 4096;
  // INITIAL_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT
  private static final int INITIAL_BYTE_COUNT = INITIAL_VIEW_VALUE_ALLOCATION * DEFAULT_RECORD_BYTE_COUNT;
  private static final int MAX_BUFFER_SIZE = (int) Math.min(MAX_ALLOCATION_SIZE, Integer.MAX_VALUE);
  private int lastValueCapacity;
  private long lastValueAllocationSizeInBytes;

  public static final int OFFSET_WIDTH = 4; /* 4 byte unsigned int to track offsets */
  /* protected members */
  protected static final int INLINE_SIZE = 12; /* 12 byte unsigned int to track inline views*/
  protected static final int VIEW_BUFFER_SIZE = 16; /* 16 byte default size for each view*/
  protected static final int LENGTH_WIDTH = 4; /* the first 4 bytes of view are allocated for length*/
  protected static final int PREFIX_WIDTH = 4; /* the second 4 bytes of view are allocated for prefix width*/
  protected static final int BUF_INDEX_WIDTH = 4; /* third 4 bytes of view are allocated for buffer index*/
  protected static final byte[] emptyByteArray = new byte[]{};
  protected ArrowBuf validityBuffer;
  protected ArrowBuf valueBuffer;
  protected ArrowBuf offsetBuffer;
  protected int valueCount;
  protected int lastSet;
  protected final Field field;
  protected List<ArrowBuf> dataBuffers;

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
    lastValueCapacity = INITIAL_VIEW_VALUE_ALLOCATION; // INITIAL_VALUE_ALLOCATION - 1;
    valueCount = 0;
    lastSet = -1;
    offsetBuffer = allocator.getEmpty();
    validityBuffer = allocator.getEmpty();
    valueBuffer = allocator.getEmpty();
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
   * Get buffer that manages the validity (NULL or NON-NULL nature) of
   * elements in the vector. Consider it as a buffer for internal bit vector
   * data structure.
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
    return valueBuffer;
  }

  /**
   * buffer that stores the offsets for elements
   * in the vector. This operation is not supported for fixed-width vectors.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getOffsetBuffer() {
    return offsetBuffer;
  }

  /**
   * Get the memory address of buffer that stores the offsets for elements
   * in the vector.
   *
   * @return starting address of the buffer
   */
  @Override
  public long getOffsetBufferAddress() {
    return offsetBuffer.memoryAddress();
  }

  /**
   * Get the memory address of buffer that manages the validity
   * (NULL or NON-NULL nature) of elements in the vector.
   *
   * @return starting address of the buffer
   */
  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  /**
   * Get the memory address of buffer that stores the data for elements
   * in the vector.
   *
   * @return starting address of the buffer
   */
  @Override
  public long getDataBufferAddress() {
    return valueBuffer.memoryAddress();
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   *
   * @param valueCount desired number of elements in the vector
   */
  @Override
  public void setInitialCapacity(int valueCount) {
    final long size = (long) valueCount * DEFAULT_RECORD_BYTE_COUNT;
    checkDataBufferSize(size);
    computeAndCheckOffsetsBufferSize(valueCount);
    lastValueAllocationSizeInBytes = (int) size;
    lastValueCapacity = valueCount;
  }

  /**
   * Sets the desired value capacity for the vector. This function doesn't
   * allocate any memory for the vector.
   *
   * @param valueCount desired number of elements in the vector
   * @param density average number of bytes per variable width element
   */
  @Override
  public void setInitialCapacity(int valueCount, double density) {
    // round up density to the nearest multiple of VIEW_BUFFER_SIZE
    density = Math.ceil(density / VIEW_BUFFER_SIZE) * VIEW_BUFFER_SIZE;

    // a minimum size of 16 bytes required to add an element
    long size = (long) (valueCount * density);
    // round up to the nearest multiple of VIEW_BUFFER_SIZE
    size = (size + VIEW_BUFFER_SIZE - 1) & -VIEW_BUFFER_SIZE;
    size = Math.max(size, VIEW_BUFFER_SIZE);
    checkDataBufferSize(size);
    computeAndCheckOffsetsBufferSize(valueCount);
    lastValueAllocationSizeInBytes = (int) size;
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
    final int startOffset = getStartOffset(0);
    final int endOffset = getStartOffset(valueCount);
    final double totalListSize = endOffset - startOffset;
    return totalListSize / valueCount;
  }

  /**
   * Get the current capacity which does not exceed either validity buffer or value buffer.
   * Note: Here the `getValueCapacity` has a relationship with the value buffer.
   *
   * @return number of elements that vector can hold.
   */
  @Override
  public int getValueCapacity() {
    final int validityCapacity = getValidityBufferValueCapacity();
    final int valueBufferCapacity = Math.max(capAtMaxInt(valueBuffer.capacity() / VIEW_BUFFER_SIZE), 0);
    return Math.min(valueBufferCapacity, validityCapacity);
  }

  private int getValidityBufferValueCapacity() {
    return capAtMaxInt(validityBuffer.capacity() * 8);
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
    clearDataBuffers();
    lastSet = -1;
    valueCount = 0;
  }

  /**
  * Release the data buffers and clear the list.
  */
  public void clearDataBuffers() {
    for (ArrowBuf buffer : dataBuffers) {
      buffer.getReferenceManager().release();
    }
    dataBuffers.clear();
  }

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  @Deprecated
  @Override
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
    // TODO: https://github.com/apache/arrow/issues/40931
    throw new UnsupportedOperationException("loadFieldBuffers is not supported for BaseVariableWidthViewVector");
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
    result.add(valueBuffer);
    // append data buffers
    result.addAll(dataBuffers);

    return result;
  }

  /**
   * Set the reader and writer indexes for the inner buffers.
   */
  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    valueBuffer.readerIndex(0);
    if (valueCount == 0) {
      validityBuffer.writerIndex(0);
      valueBuffer.writerIndex(0);
    } else {
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      valueBuffer.writerIndex(valueCount * VIEW_BUFFER_SIZE);
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
    if (size > MAX_BUFFER_SIZE || size < 0) {
      throw new OversizedAllocationException("Memory required for vector " +
          "is (" + size + "), which is overflow or more than max allowed (" + MAX_BUFFER_SIZE + "). " +
          "You could consider using LargeVarCharVector/LargeVarBinaryVector for large strings/large bytes types");
    }
  }

  /*
   * Compute the buffer size required for 'valueCount' offsets and validity, and check if it's
   * within bounds.
   */
  private long computeAndCheckOffsetsBufferSize(int valueCount) {
    /* to track the end offset of the last data element in vector, we need
     * an additional slot in offset buffer.
     */
    final long size = computeCombinedBufferSize(valueCount + 1, OFFSET_WIDTH);
    if (size > MAX_BUFFER_SIZE) {
      throw new OversizedAllocationException("Memory required for vector capacity " +
          valueCount +
          " is (" + size + "), which is more than max allowed (" + MAX_BUFFER_SIZE + ")");
    }
    return size;
  }

  /* allocate the inner buffers */
  private void allocateBytes(final long valueBufferSize, final int valueCount) {
    /* allocate data buffer */
    valueBuffer = allocator.buffer(valueBufferSize);
    valueBuffer.readerIndex(0);

    validityBuffer = allocator.buffer((valueCount + 7) / 8);
    initValidityBuffer();

    lastValueCapacity = getValueCapacity();
    lastValueAllocationSizeInBytes = capAtMaxInt(valueBuffer.capacity());
  }

  /**
   * Resize the vector to increase the capacity. The internal behavior is to
   * double the current value capacity.
   */
  @Override
  public void reAlloc() {
    reallocViewBuffer();
    reallocViewReferenceBuffer();
    reallocValidityBufferOnly();
  }

  /**
   * Reallocate the data buffer. Data Buffer stores the actual data for
   * VARCHAR or VARBINARY elements in the vector. The behavior is to double
   * the size of buffer.
   * @throws OversizedAllocationException if the desired new size is more than
   *                                      max allowed
   * @throws OutOfMemoryException if the internal memory allocation fails
   */
  public void reallocViewBuffer() {
    // TODO: here we should only allocate the views buffer not the reference buffer
    //  if we are to decide the allocation size, we must consider the valueBuffer (viewBuffer)
    //  instead of considering the dataBuffers last element's capacity.
    long currentViewBufferCapacity = valueBuffer.capacity();
    // if (!dataBuffers.isEmpty()) {
    //   currentViewBufferCapacity = dataBuffers.get(dataBuffers.size() - 1).capacity();
    // }

    long newAllocationSize = currentViewBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (lastValueAllocationSizeInBytes > 0) {
        newAllocationSize = lastValueAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_BYTE_COUNT * 2L;
      }
    }

    reallocViewBuffer(newAllocationSize);
    // reallocViewReferenceBuffer(newAllocationSize);
  }

  /**
   * Reallocate the data buffer for reference buffer.
   */
  public void reallocViewReferenceBuffer() {
    long currentReferenceBufferCapacity = 0;
    if (!dataBuffers.isEmpty()) {
      currentReferenceBufferCapacity = dataBuffers.get(dataBuffers.size() - 1).capacity();
    }

    long newAllocationSize = currentReferenceBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (lastValueAllocationSizeInBytes > 0) {
        newAllocationSize = lastValueAllocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_BYTE_COUNT * 2L;
      }
    }

    reallocViewReferenceBuffer(newAllocationSize);
  }

  /**
   * Reallocate the data buffer to given size. Data Buffer stores the actual data for
   * VARCHAR or VARBINARY elements in the vector. The actual allocated size may be larger
   * than the request one because it will round up the provided value to the nearest
   * power of two.
   *
   * @param desiredAllocSize the desired new allocation size
   * @throws OversizedAllocationException if the desired new size is more than
   *                                      max allowed
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
    newAllocationSize += VIEW_BUFFER_SIZE;

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, valueBuffer, 0, valueBuffer.capacity());

    valueBuffer.getReferenceManager().release();
    valueBuffer = newBuf;
    lastValueAllocationSizeInBytes = valueBuffer.capacity();
  }

  /**
   * Reallocate the data buffer for reference buffer.
   *
   * @param desiredAllocSize allocation size in bytes
   */
  public void reallocViewReferenceBuffer(long desiredAllocSize) {
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

  /**
  *  Reallocate Validity buffer.
  */
  public void reallocValidityBufferOnly() {
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
    newValidityBuffer.setZero(validityBuffer.capacity(), newValidityBuffer.capacity() - validityBuffer.capacity());
    validityBuffer.getReferenceManager().release();
    validityBuffer = newValidityBuffer;

    lastValueCapacity = getValueCapacity();
  }

  private long computeValidityBufferSize(int valueCount) {
    return (valueCount + 7) / 8;
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
    int totalLength = 0;
    for (int i = 0; i < valueCount; i++) {
      totalLength += getLength(i);
    }
    return totalLength;
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

    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    final int offsetBufferSize = (valueCount + 1) * OFFSET_WIDTH;
    /* get the end offset for this valueCount */
    final int dataBufferSize = offsetBuffer.getInt((long) valueCount * OFFSET_WIDTH);
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
   * impact the reference counts for this buffer, so it only should be used for in-context
   * access. Also note that this buffer changes regularly, thus
   * external classes shouldn't hold a reference to it (unless they change it).
   * <p>
   * Note: This method only returns validityBuffer, offsetBuffer and valueBuffer.
   * But it doesn't return the reference buffers.
   * <p>
   * TODO: Implement a strategy to retrieve the reference buffers.
   * <a href="https://github.com/apache/arrow/issues/40930">Reference buffer retrieval.</a>
   *
   * @param clear Whether to clear vector before returning, the buffers will still be refcounted
   *              but the returned array will be the only reference to them
   * @return The underlying {@link ArrowBuf buffers} that is used by this
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
   * Construct a transfer pair of this vector and another vector of the same type.
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
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(getName(), allocator);
  }

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(String ref, BufferAllocator allocator);

  /**
   * Construct a transfer pair of this vector and another vector of the same type.
   * @param field The field materialized by this vector.
   * @param allocator allocator for the target vector
   * @return TransferPair
   */
  @Override
  public abstract TransferPair getTransferPair(Field field, BufferAllocator allocator);

  /**
   * Transfer this vector's data to another vector.
   * The memory associated with this vector is transferred to the allocator of target vector
   * for accounting and management purposes.
   * @param target destination vector for transfer
   */
  public void transferTo(BaseVariableWidthViewVector target) {
    throw new UnsupportedOperationException("trasferTo function not supported!");
  }

  /**
   * Slice this vector at desired index and length and transfer the
   * corresponding data to the target vector.
   * @param startIndex start position of the split in source vector.
   * @param length length of the split.
   * @param target destination vector
   */
  public void splitAndTransferTo(int startIndex, int length,
                                 BaseVariableWidthViewVector target) {
    throw new UnsupportedOperationException("splitAndTransferTo function not supported!");
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
   * @return true if the index is within the current value capacity
   */
  public boolean isSafe(int index) {
    return index < getValueCapacity();
  }

  /**
   * Check if an element at given index is null.
   *
   * @param index  position of an element
   * @return true if an element at given index is null
   */
  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index  position of an element
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
      // reallocValidityAndOffsetBuffers();
      reallocViewBuffer();
      reallocValidityBufferOnly();
    }
    fillHoles(valueCount);
    lastSet = valueCount - 1;
    setReaderAndWriterIndex();
  }

  /**
   * Create holes in the vector upto the given index (exclusive).
   * Holes will be created from the current last-set position in
   * the vector.
   *
   * @param index target index
   */
  @Override
  public void fillEmpties(int index) {
    handleSafe(index, emptyByteArray.length);
    fillHoles(index);
    lastSet = index - 1;
  }

  /**
   * Set the index of the last non-null element in the vector.
   * It is important to call this method with appropriate value
   * before calling {@link #setValueCount(int)}.
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
    // We need to check and reallocate both validity and offset buffer
    while (index >= getValueCapacity()) {
      reallocValidityBufferOnly();
    }
    BitVectorHelper.setBit(validityBuffer, index);
  }

  /**
   * Sets the value length for an element.
   *
   * @param index   position of the element to set
   * @param length  length of the element
   */
  @Override
  public void setValueLengthSafe(int index, int length) {
    assert index >= 0;
    handleSafe(index, length);
    fillHoles(index);
    final int startOffset = getStartOffset(index);
    offsetBuffer.setInt((index + 1) * ((long) OFFSET_WIDTH), startOffset + length);
    lastSet = index;
  }

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index position of an element to get
   * @return greater than length 0 for a non-null element, 0 otherwise
   */
  @Override
  public int getValueLength(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      return 0;
    }
    return getLength(index);
  }

  /**
   * Set the variable length element at the specified index to the supplied
   * byte array. This is same as using {@link #set(int, byte[], int, int)}
   * with start as Zero and length as #value.length
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
   * case where index and length of a new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   */
  @Override
  public void setSafe(int index, byte[] value) {
    assert index >= 0;
    // check if the current index can be populated
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
   * @param start   start index in an array of bytes
   * @param length  length of data in an array of bytes
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
   * case where index and length of a new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param value   array of bytes to write
   * @param start   start index in an array of bytes
   * @param length  length of data in an array of bytes
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
    setBytes(index, value.array(), start, length);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, ByteBuffer, int, int)} except that it handles the
   * case where index and length of a new element are beyond the existing
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
    // We need to check and reallocate both validity and offset buffer
    while (index >= getValueCapacity()) {
      reallocValidityBufferOnly();
    }
    BitVectorHelper.unsetBit(validityBuffer, index);
  }

  /**
   * Store the given value at a particular position in the vector. isSet indicates
   * whether the value is NULL or not.
   * @param index position of the new value
   * @param isSet Zero for NULL value, 1 otherwise
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
    byte[] data = new byte[dataLength];
    buffer.getBytes(start, data, 0, dataLength);
    setBytes(index, data, start, dataLength);
    lastSet = index;
  }

  /**
   * Same as {@link #set(int, int, int, int, ArrowBuf)} except that it handles the case
   * when index is greater than or equal to current value capacity of the
   * vector.
   * @param index position of the new value
   * @param isSet Zero for NULL value, 1 otherwise
   * @param start start position of data in buffer
   * @param end end position of data in buffer
   * @param buffer data buffer containing the variable width element to be stored
   *               in the vector
   */
  public void setSafe(int index, int isSet, int start, int end, ArrowBuf buffer) {
    assert index >= 0;
    final int dataLength = end - start;
    handleSafe(index, dataLength);
    fillHoles(index);
    BitVectorHelper.setValidityBit(validityBuffer, index, isSet);
    byte[] data = new byte[dataLength];
    buffer.getBytes(start, data, 0, dataLength);
    setBytes(index, data, 0, dataLength);
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
    BitVectorHelper.setBit(validityBuffer, index);
    byte[] data = new byte[length];
    buffer.getBytes(start, data, 0, length);
    setBytes(index, data, start, length);
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
    handleSafe(index, length);
    fillHoles(index);
    BitVectorHelper.setBit(validityBuffer, index);
    byte[] data = new byte[length];
    buffer.getBytes(start, data, 0, length);
    setBytes(index, data, start, length);
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

  /**
   * Get the length of the view.
   * @param index The index of the element in the vector.
   * @return The length of the element at the given index.
   */
  public final int getLength(int index) {
    if (index < 0 || index >= valueBuffer.capacity() / VIEW_BUFFER_SIZE) {
      throw new IndexOutOfBoundsException("Index out of bounds: " + index);
    }
    if (isSet(index) == 0) {
      return 0;
    }
    return valueBuffer.getInt(((long) index * VIEW_BUFFER_SIZE));
  }

  /**
   * This method is used to create a view buffer for a variable width vector.
   * It handles both inline and reference buffers.
   * <p>
   * If the length of the value is less than or equal to {@link #INLINE_SIZE}, the value is stored in the valueBuffer
   * directly as an inline buffer.
   * The valueBuffer stores the length of the value followed by the value itself.
   * If the length of the value is greater than {@link #INLINE_SIZE}, a new buffer is allocated and added to dataBuffers
   * to hold the value.
   * The valueBuffer in this case stores the length of the value, a prefix of the value, the index of the
   * new buffer in dataBuffers, and the offset of the value in the new buffer.
   *
   * @param allocator The allocator used to create new ArrowBuf instances.
   * @param index The index at which the new value will be inserted.
   * @param value The byte array that contains the data to be inserted.
   * @param start The start index in the byte array from where the data for the new value begins.
   * @param length The length of the data in the byte array that belongs to the new value.
   * @param valueBuffer The ArrowBuf instance that will hold the data of the new value.
   * @param dataBuffers The list of ArrowBuf instances that hold the data of all values in the vector.
   */
  protected void createViewBuffer(
      BufferAllocator allocator,
      int index,
      byte[] value,
      int start,
      int length,
      ArrowBuf valueBuffer,
      List<ArrowBuf> dataBuffers) {
    int writePosition = index * VIEW_BUFFER_SIZE;
    if (value.length <= INLINE_SIZE) {
      // inline buffer
      // set length
      valueBuffer.setInt(writePosition, length);
      writePosition += LENGTH_WIDTH;
      // set data
      valueBuffer.setBytes(writePosition, value, start, length);
    } else {
      // reference buffer
      if (dataBuffers.isEmpty()) {
        // the first data buffer needs to be added
        ArrowBuf newDataBuf = allocator.buffer(lastValueAllocationSizeInBytes);
        // set length
        valueBuffer.setInt(writePosition, length);
        writePosition += LENGTH_WIDTH;
        // set prefix
        valueBuffer.setBytes(writePosition, value, start, PREFIX_WIDTH);
        writePosition += PREFIX_WIDTH;
        // set buf id
        valueBuffer.setInt(writePosition, /*first buffer*/0);
        writePosition += BUF_INDEX_WIDTH;
        // set offset
        valueBuffer.setInt(writePosition, 0);
        newDataBuf.setBytes(0, value, 0, length);
        newDataBuf.writerIndex(length);
        dataBuffers.add(newDataBuf);
      } else {
        // insert to the last buffer in the data buffers or allocate new if the last buffer isn't enough
        // set lengths
        valueBuffer.setInt(writePosition, length);
        writePosition += LENGTH_WIDTH;
        // set prefix
        valueBuffer.setBytes(writePosition, value, start, PREFIX_WIDTH);
        writePosition += PREFIX_WIDTH;
        // set buf id
        int currentBufId = dataBuffers.size() - 1;
        ArrowBuf currentBuf = dataBuffers.get(currentBufId);
        if (currentBuf.capacity() - currentBuf.writerIndex() >= length) {
          // current buffer is enough
          // set buf indexes
          valueBuffer.setInt(writePosition, currentBufId);
          writePosition += BUF_INDEX_WIDTH;
          // set offset
          valueBuffer.setInt(writePosition, (int) currentBuf.writerIndex());
          currentBuf.setBytes(currentBuf.writerIndex(), value, start, length);
          currentBuf.writerIndex(currentBuf.writerIndex() + length);
          dataBuffers.set(currentBufId, currentBuf);
        } else {
          // current buffer is not enough
          // allocate new buffer
          ArrowBuf newBuf = allocator.buffer(lastValueAllocationSizeInBytes);
          // set buf index
          valueBuffer.setInt(writePosition, dataBuffers.size());
          writePosition += BUF_INDEX_WIDTH;
          // set offset
          valueBuffer.setInt(writePosition, 0);
          newBuf.setBytes(0, value, start, length);
          newBuf.writerIndex(newBuf.writerIndex() + length);
          dataBuffers.add(newBuf);
        }
      }
    }
  }

  protected final void setBytes(int index, byte[] value, int start, int length) {
    /* End offset of the current last element in the vector.
     * This will be the start offset of a new element we are trying to store.
     */
    // final int startOffset = getStartOffset(index);
    /* set new end offset */
    // offsetBuffer.setInt((long) (index + 1) * OFFSET_WIDTH, startOffset + length);
    /* store the var length data in value buffer */
    /*check whether the buffer is inline or reference buffer*/
    createViewBuffer(allocator, index, value, start, length, valueBuffer, dataBuffers);
  }

  /**
   * Get the start offset of the element at the given index.
   * @param index The index of the element in the vector.
   * @return The start offset of the element at the given index.
   */
  public final int getStartOffset(int index) {
    // return offsetBuffer.getInt((long) index * OFFSET_WIDTH);
    int totalLength = 0;
    for (int i = 0; i < index - 1; i++) {
      totalLength += getLength(i);
    }
    return totalLength;
  }

  private static long roundUpToMultipleOf16(long num) {
    return (num + 15) & ~15;
  }

  protected final void handleSafe(int index, int dataLength) {
    final long startOffset = lastSet < 0 ? 0 : getStartOffset(lastSet + 1);
    final long targetCapacity = roundUpToMultipleOf16(startOffset + dataLength);
    // for views, we need each buffer with 16 byte alignment, so we need to check the last written index
    // in the valueBuffer and allocate a new buffer which has 16 byte alignment for adding new values.
    long writePosition = (long) index * VIEW_BUFFER_SIZE;
    if (valueBuffer.capacity() <= writePosition || valueBuffer.capacity() < targetCapacity) {
      /*
      * Everytime we want to increase the capacity of the valueBuffer, we need to make sure that the new capacity
      * meets 16 byte alignment.
      * If the targetCapacity is larger than the writePosition, we may not necessarily
      * want to allocate the targetCapacity to valueBuffer since when it is >={@link #INLINE_SIZE} either way
      * we are writing to the dataBuffer.
      */
      reallocViewBuffer(Math.max(writePosition, targetCapacity));
    }

    while (index >= getValueCapacity()) {
      // reallocValidityAndOffsetBuffers();
      reallocValidityBufferOnly();
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
    final int currentStartOffset = offset.getInt((long) index * OFFSET_WIDTH);
    final int dataLength =
            offset.getInt((long) (index + 1) * OFFSET_WIDTH) - currentStartOffset;
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
                             int valueCount, int index, int value) {
    if (buffer == null) {
      buffer = allocator.buffer((long) valueCount * OFFSET_WIDTH);
    }
    buffer.setInt((long) index * OFFSET_WIDTH, value);
    if (index == (valueCount - 1)) {
      buffer.writerIndex((long) valueCount * OFFSET_WIDTH);
    }

    return buffer;
  }

  /**
   * Copy a cell value from a particular index in source vector to a particular position in this
   * vector.
   * TODO: Improve functionality to support copying views.
   * <a href="https://github.com/apache/arrow/issues/40933">Enhance CopyFrom</a>
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFrom(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException("copyFrom is not supported for VariableWidthVector");
  }

  /**
   * Same as {@link #copyFrom(int, int, ValueVector)} except that it handles the case when the
   * capacity of the vector needs to be expanded before copy.
   * TODO: Improve functionality to support copying views.
   * <a href="https://github.com/apache/arrow/issues/40933">Enhance CopyFrom</a>
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  @Override
  public void copyFromSafe(int fromIndex, int thisIndex, ValueVector from) {
    throw new UnsupportedOperationException("copyFromSafe is not supported for VariableWidthVector");
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
      int length = getLength(index);
      if (length < INLINE_SIZE) {
        int start = index * VIEW_BUFFER_SIZE + LENGTH_WIDTH;
        reuse.set(valueBuffer, start, length);
      } else {
        final int bufIndex =
            valueBuffer.getInt(((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
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
    final int length = getLength(index);
    if (length < INLINE_SIZE) {
      int start = index * VIEW_BUFFER_SIZE + LENGTH_WIDTH;
      return ByteFunctionHelpers.hash(hasher, this.getDataBuffer(), start, start + length);
    } else {
      final int bufIndex =
          valueBuffer.getInt(((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      final int dataOffset =
          valueBuffer.getInt(
              ((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      ArrowBuf dataBuf = dataBuffers.get(bufIndex);
      return ByteFunctionHelpers.hash(hasher, dataBuf, dataOffset, dataOffset + length);
    }
  }

  /**
   * Retrieves the data of a variable-width element at a given index in the vector.
   *
   * <p>
   * If the length of the data is greater than {@link #INLINE_SIZE}, the data is stored in an inline buffer.
   * The method retrieves the buffer index and data offset from the valueBuffer, and then retrieves the data from the
   * corresponding buffer in the dataBuffers list.
   * <p>
   * If the length of the data is less than or equal to {@link #INLINE_SIZE}, the data is stored directly in the
   * valueBuffer.
   * The method retrieves the data directly from the valueBuffer.
   *
   * @param index position of the element in the vector
   * @return byte array containing the data of the element
   */
  protected byte[] getData(int index) {
    final int dataLength = getLength(index);
    byte[] result = new byte[dataLength];
    if (dataLength > INLINE_SIZE) {
      // data is in the reference buffer
      // get buffer index
      final int bufferIndex =
              valueBuffer.getInt(((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH);
      // get data offset
      final int dataOffset =
              valueBuffer.getInt(
                      ((long) index * VIEW_BUFFER_SIZE) + LENGTH_WIDTH + PREFIX_WIDTH + BUF_INDEX_WIDTH);
      dataBuffers.get(bufferIndex).getBytes(dataOffset, result, 0, dataLength);
    } else {
      // data is in the value buffer
      valueBuffer.getBytes(
              (long) index * VIEW_BUFFER_SIZE + BUF_INDEX_WIDTH, result, 0, dataLength);
    }
    return result;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }
}
