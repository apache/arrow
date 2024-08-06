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
package org.apache.arrow.vector.complex;

import static java.util.Collections.singletonList;
import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.UnionLargeListViewReader;
import org.apache.arrow.vector.complex.impl.UnionLargeListViewWriter;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A large list view vector contains lists of a specific type of elements. Its structure contains 3
 * elements.
 *
 * <ol>
 *   <li>A validity buffer.
 *   <li>An offset buffer, that denotes lists starting positions.
 *   <li>A size buffer, that denotes sizes of the lists.
 *   <li>A child data vector that contains the elements of lists.
 * </ol>
 *
 * This is the LargeListView variant of listview, it has a 64-bit wide offset
 *
 * <p>WARNING: Currently Arrow in Java doesn't support 64-bit vectors. This class follows the
 * expected behaviour of a LargeList but doesn't actually support allocating a 64-bit vector. It has
 * little use until 64-bit vectors are supported and should be used with caution. todo review
 * checkedCastToInt usage in this class. Once int64 indexed vectors are supported these checks
 * aren't needed.
 */
public class LargeListViewVector extends BaseLargeRepeatedValueViewVector
    implements PromotableVector, ValueIterableVector<List<?>> {

  protected ArrowBuf validityBuffer;
  protected UnionLargeListViewReader reader;
  private CallBack callBack;
  protected Field field;
  protected int validityAllocationSizeInBytes;

  public static LargeListViewVector empty(String name, BufferAllocator allocator) {
    return new LargeListViewVector(
        name, allocator, FieldType.nullable(ArrowType.LargeListView.INSTANCE), null);
  }

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack A schema change callback.
   */
  public LargeListViewVector(
      String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    this(new Field(name, fieldType, null), allocator, callBack);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param callBack A schema change callback.
   */
  public LargeListViewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field.getName(), allocator, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.field = field;
    this.callBack = callBack;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    checkArgument(
        children.size() == 1,
        "ListViews have one child Field. Found: %s",
        children.isEmpty() ? "none" : children);

    Field field = children.get(0);
    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(field.getFieldType());
    checkArgument(
        addOrGetVector.isCreated(), "Child vector already existed: %s", addOrGetVector.getVector());

    addOrGetVector.getVector().initializeChildrenFromFields(field.getChildren());
    this.field = new Field(this.field.getName(), this.field.getFieldType(), children);
  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(numRecords);
    super.setInitialCapacity(numRecords);
  }

  /**
   * Specialized version of setInitialCapacity() for LargeListViewVector. This is used by some
   * callers when they want to explicitly control and be conservative about memory allocated for
   * inner data vector. This is very useful when we are working with memory constraints for a query
   * and have a fixed amount of memory reserved for the record batch. In such cases, we are likely
   * to face OOM or related problems when we reserve memory for a record batch with value count x
   * and do setInitialCapacity(x) such that each vector allocates only what is necessary and not the
   * default amount, but the multiplier forces the memory requirement to go beyond what was needed.
   *
   * @param numRecords value count
   * @param density density of LargeListViewVector. Density is the average size of a list per
   *     position in the LargeListViewVector. For example, a density value of 10 implies each
   *     position in the list vector has a list of 10 values. A density value of 0.1 implies out of
   *     10 positions in the list vector, 1 position has a list of size 1, and the remaining
   *     positions are null (no lists) or empty lists. This helps in tightly controlling the memory
   *     we provision for inner data vector.
   */
  @Override
  public void setInitialCapacity(int numRecords, double density) {
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(numRecords);
    super.setInitialCapacity(numRecords, density);
  }

  /**
   * Specialized version of setInitialTotalCapacity() for LargeListViewVector. This is used by some
   * callers when they want to explicitly control and be conservative about memory allocated for
   * inner data vector. This is very useful when we are working with memory constraints for a query
   * and have a fixed amount of memory reserved for the record batch. In such cases, we are likely
   * to face OOM or related problems when we reserve memory for a record batch with value count x
   * and do setInitialCapacity(x) such that each vector allocates only what is necessary and not the
   * default amount, but the multiplier forces the memory requirement to go beyond what was needed.
   *
   * @param numRecords value count
   * @param totalNumberOfElements the total number of elements to allow for in this vector across
   *     all records.
   */
  @Override
  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(numRecords);
    super.setInitialTotalCapacity(numRecords, totalNumberOfElements);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return singletonList(getDataVector());
  }

  /**
   * Load the buffers associated with this Field.
   *
   * @param fieldNode the fieldNode
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 3) {
      throw new IllegalArgumentException(
          "Illegal buffer count, expected " + 3 + ", got: " + ownBuffers.size());
    }

    ArrowBuf bitBuffer = ownBuffers.get(0);
    ArrowBuf offBuffer = ownBuffers.get(1);
    ArrowBuf szBuffer = ownBuffers.get(2);

    validityBuffer.getReferenceManager().release();
    validityBuffer = BitVectorHelper.loadValidityBuffer(fieldNode, bitBuffer, allocator);
    offsetBuffer.getReferenceManager().release();
    offsetBuffer = offBuffer.getReferenceManager().retain(offBuffer, allocator);
    sizeBuffer.getReferenceManager().release();
    sizeBuffer = szBuffer.getReferenceManager().retain(szBuffer, allocator);

    validityAllocationSizeInBytes = checkedCastToInt(validityBuffer.capacity());
    offsetAllocationSizeInBytes = offsetBuffer.capacity();
    sizeAllocationSizeInBytes = sizeBuffer.capacity();

    valueCount = fieldNode.getLength();
  }

  /** Set the reader and writer indexes for the inner buffers. */
  private void setReaderAndWriterIndex() {
    validityBuffer.readerIndex(0);
    offsetBuffer.readerIndex(0);
    sizeBuffer.readerIndex(0);
    if (valueCount == 0) {
      validityBuffer.writerIndex(0);
      offsetBuffer.writerIndex(0);
      sizeBuffer.writerIndex(0);
    } else {
      validityBuffer.writerIndex(getValidityBufferSizeFromCount(valueCount));
      offsetBuffer.writerIndex((long) valueCount * OFFSET_WIDTH);
      sizeBuffer.writerIndex((long) valueCount * SIZE_WIDTH);
    }
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(2);
    setReaderAndWriterIndex();
    result.add(validityBuffer);
    result.add(offsetBuffer);
    result.add(sizeBuffer);

    return result;
  }

  /**
   * Export the buffers of the fields for C Data Interface. This method traverses the buffers and
   * export buffer and buffer's memory address into a list of buffers and a pointer to the list of
   * buffers.
   */
  @Override
  public void exportCDataBuffers(List<ArrowBuf> buffers, ArrowBuf buffersPtr, long nullValue) {
    throw new UnsupportedOperationException("exportCDataBuffers Not implemented yet");
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating memory");
    }
  }

  @Override
  public boolean allocateNewSafe() {
    boolean success = false;
    try {
      /* release the current buffers, hence this is a new allocation
       * Note that, the `clear` method call below is releasing validityBuffer
       * calling the superclass clear method which is releasing the associated buffers
       * (sizeBuffer and offsetBuffer).
       */
      clear();
      /* allocate validity buffer */
      allocateValidityBuffer(validityAllocationSizeInBytes);
      /* allocate offset, data and sizes buffer */
      success = super.allocateNewSafe();
    } finally {
      if (!success) {
        clear();
      }
    }
    return success;
  }

  protected void allocateValidityBuffer(final long size) {
    final int curSize = (int) size;
    validityBuffer = allocator.buffer(curSize);
    validityBuffer.readerIndex(0);
    validityAllocationSizeInBytes = curSize;
    validityBuffer.setZero(0, validityBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    /* reallocate the validity buffer */
    reallocValidityBuffer();
    /* reallocate the offset, size, and data */
    super.reAlloc();
  }

  protected void reallocValidityAndSizeAndOffsetBuffers() {
    reallocateBuffers();
    reallocValidityBuffer();
  }

  private void reallocValidityBuffer() {
    final int currentBufferCapacity = checkedCastToInt(validityBuffer.capacity());
    long newAllocationSize = getNewAllocationSize(currentBufferCapacity);

    final ArrowBuf newBuf = allocator.buffer(newAllocationSize);
    newBuf.setBytes(0, validityBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    validityBuffer.getReferenceManager().release(1);
    validityBuffer = newBuf;
    validityAllocationSizeInBytes = (int) newAllocationSize;
  }

  private long getNewAllocationSize(int currentBufferCapacity) {
    long newAllocationSize = currentBufferCapacity * 2L;
    if (newAllocationSize == 0) {
      if (validityAllocationSizeInBytes > 0) {
        newAllocationSize = validityAllocationSizeInBytes;
      } else {
        newAllocationSize = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION) * 2L;
      }
    }
    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }
    return newAllocationSize;
  }

  @Override
  public void copyFromSafe(int inIndex, int outIndex, ValueVector from) {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support copyFromSafe operation yet.");
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support copyFrom operation yet.");
  }

  @Override
  public FieldVector getDataVector() {
    return vector;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return getTransferPair(field, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support getTransferPair(String, BufferAllocator, CallBack) yet");
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support getTransferPair(Field, BufferAllocator, CallBack) yet");
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support makeTransferPair(ValueVector) yet");
  }

  @Override
  public long getValidityBufferAddress() {
    return validityBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    return offsetBuffer.memoryAddress();
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return validityBuffer;
  }

  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    return offsetBuffer;
  }

  public ArrowBuf getSizeBuffer() {
    return sizeBuffer;
  }

  public long getSizeBufferAddress() {
    return sizeBuffer.memoryAddress();
  }

  /**
   * Get the hash code for the element at the given index.
   *
   * @param index position of the element
   * @return hash code for the element at the given index
   */
  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  /**
   * Get the hash code for the element at the given index.
   *
   * @param index position of the element
   * @param hasher hasher to use
   * @return hash code for the element at the given index
   */
  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    if (isSet(index) == 0) {
      return ArrowBufPointer.NULL_HASH_CODE;
    }
    int hash = 0;
    final int start = offsetBuffer.getInt((long) index * OFFSET_WIDTH);
    final int end = sizeBuffer.getInt((long) index * OFFSET_WIDTH);
    for (int i = start; i < end; i++) {
      hash = ByteFunctionHelpers.combineHash(hash, vector.hashCode(checkedCastToInt(i), hasher));
    }
    return hash;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FieldReader getReaderImpl() {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support getReaderImpl operation yet.");
  }

  @Override
  public UnionListReader getReader() {
    throw new UnsupportedOperationException(
        "LargeListViewVector does not support getReader operation yet.");
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this vector.
   *
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSize() {
    if (valueCount == 0) {
      return 0;
    }
    final int offsetBufferSize = valueCount * OFFSET_WIDTH;
    final int sizeBufferSize = valueCount * SIZE_WIDTH;
    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);
    return offsetBufferSize + sizeBufferSize + validityBufferSize + vector.getBufferSize();
  }

  /**
   * Get the size (number of bytes) of underlying buffers used by this.
   *
   * @param valueCount the number of values to assume this vector contains
   * @return size of underlying buffers.
   */
  @Override
  public int getBufferSizeFor(int valueCount) {
    if (valueCount == 0) {
      return 0;
    }
    final int validityBufferSize = getValidityBufferSizeFromCount(valueCount);

    return super.getBufferSizeFor(valueCount) + validityBufferSize;
  }

  /**
   * Get the field associated with the list view vector.
   *
   * @return the field
   */
  @Override
  public Field getField() {
    if (field.getChildren().contains(getDataVector().getField())) {
      return field;
    }
    field =
        new Field(
            field.getName(),
            field.getFieldType(),
            Collections.singletonList(getDataVector().getField()));
    return field;
  }

  /**
   * Get the minor type for the vector.
   *
   * @return the minor type
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.LARGELISTVIEW;
  }

  /** Clear the vector data. */
  @Override
  public void clear() {
    // calling superclass clear method which is releasing the sizeBufer and offsetBuffer
    super.clear();
    validityBuffer = releaseBuffer(validityBuffer);
  }

  /** Release the buffers associated with this vector. */
  @Override
  public void reset() {
    super.reset();
    validityBuffer.setZero(0, validityBuffer.capacity());
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
    setReaderAndWriterIndex();
    final ArrowBuf[] buffers;
    if (getBufferSize() == 0) {
      buffers = new ArrowBuf[0];
    } else {
      List<ArrowBuf> list = new ArrayList<>();
      // the order must be validity, offset and size buffers
      list.add(validityBuffer);
      list.add(offsetBuffer);
      list.add(sizeBuffer);
      list.addAll(Arrays.asList(vector.getBuffers(clear)));
      buffers = list.toArray(new ArrowBuf[list.size()]);
    }
    if (clear) {
      for (ArrowBuf buffer : buffers) {
        buffer.getReferenceManager().retain();
      }
      clear();
    }
    return buffers;
  }

  /**
   * Get the element in the list view vector at a particular index.
   *
   * @param index position of the element
   * @return Object at given position
   */
  @Override
  public List<?> getObject(int index) {
    if (isSet(index) == 0) {
      return null;
    }
    final List<Object> vals = new JsonStringArrayList<>();
    final int start = offsetBuffer.getInt(index * OFFSET_WIDTH);
    final int end = start + sizeBuffer.getInt((index) * SIZE_WIDTH);
    final ValueVector vv = getDataVector();
    for (int i = start; i < end; i++) {
      vals.add(vv.getObject(checkedCastToInt(i)));
    }

    return vals;
  }

  /**
   * Check if an element at given index is null.
   *
   * @param index position of an element
   * @return true if an element at given index is null, false otherwise
   */
  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  /**
   * Check if an element at given index is an empty list.
   *
   * @param index position of an element
   * @return true if an element at given index is an empty list or NULL, false otherwise
   */
  @Override
  public boolean isEmpty(int index) {
    if (isNull(index)) {
      return true;
    } else {
      return sizeBuffer.getInt(index * SIZE_WIDTH) == 0;
    }
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index position of the element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

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
   * Get the value capacity by considering validity and offset capacity. Note that the size buffer
   * capacity is not considered here since it has the same capacity as the offset buffer.
   *
   * @return the value capacity
   */
  @Override
  public int getValueCapacity() {
    return getValidityAndOffsetValueCapacity();
  }

  private int getValidityAndSizeValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity(), 0);
    final int sizeValueCapacity = Math.max(getSizeBufferValueCapacity(), 0);
    return Math.min(offsetValueCapacity, sizeValueCapacity);
  }

  private int getValidityAndOffsetValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity(), 0);
    return Math.min(offsetValueCapacity, getValidityBufferValueCapacity());
  }

  private int getValidityBufferValueCapacity() {
    return capAtMaxInt(validityBuffer.capacity() * 8);
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index the value to change
   */
  @Override
  public void setNull(int index) {
    while (index >= getValidityAndSizeValueCapacity()) {
      reallocValidityAndSizeAndOffsetBuffers();
    }

    offsetBuffer.setInt(index * OFFSET_WIDTH, 0);
    sizeBuffer.setInt(index * SIZE_WIDTH, 0);
    BitVectorHelper.unsetBit(validityBuffer, index);
  }

  /**
   * Start new value in the ListView vector.
   *
   * @param index index of the value to start
   * @return offset of the new value
   */
  @Override
  public int startNewValue(int index) {
    while (index >= getValidityAndSizeValueCapacity()) {
      reallocValidityAndSizeAndOffsetBuffers();
    }

    if (index > 0) {
      final int prevOffset = getLengthOfChildVectorByIndex(index);
      offsetBuffer.setInt(index * OFFSET_WIDTH, prevOffset);
    }

    BitVectorHelper.setBit(validityBuffer, index);
    return offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  /**
   * Validate the invariants of the offset and size buffers. 0 <= offsets[i] <= length of the child
   * array 0 <= offsets[i] + size[i] <= length of the child array
   *
   * @param offset the offset at a given index
   * @param size the size at a given index
   */
  private void validateInvariants(int offset, int size) {
    if (offset < 0) {
      throw new IllegalArgumentException("Offset cannot be negative");
    }

    if (size < 0) {
      throw new IllegalArgumentException("Size cannot be negative");
    }

    // 0 <= offsets[i] <= length of the child array
    if (offset > this.vector.getValueCount()) {
      throw new IllegalArgumentException("Offset is out of bounds.");
    }

    // 0 <= offsets[i] + size[i] <= length of the child array
    if (offset + size > this.vector.getValueCount()) {
      throw new IllegalArgumentException("Offset + size <= length of the child array.");
    }
  }

  /**
   * Set the offset at the given index. Make sure to use this function after updating `field` vector
   * and using `setValidity`
   *
   * @param index index of the value to set
   * @param value value to set
   */
  public void setOffset(int index, int value) {
    validateInvariants(value, sizeBuffer.getInt(index * SIZE_WIDTH));

    offsetBuffer.setInt(index * OFFSET_WIDTH, value);
  }

  /**
   * Set the size at the given index. Make sure to use this function after using `setOffset`.
   *
   * @param index index of the value to set
   * @param value value to set
   */
  public void setSize(int index, int value) {
    validateInvariants(offsetBuffer.getInt(index * SIZE_WIDTH), value);

    sizeBuffer.setInt(index * SIZE_WIDTH, value);
  }

  /**
   * Set the validity at the given index.
   *
   * @param index index of the value to set
   * @param value value to set (0 for unset and 1 for a set)
   */
  public void setValidity(int index, int value) {
    if (value == 0) {
      BitVectorHelper.unsetBit(validityBuffer, index);
    } else {
      BitVectorHelper.setBit(validityBuffer, index);
    }
  }

  /**
   * Sets the value count for the vector.
   *
   * <p>Important note: The underlying vector does not support 64-bit allocations yet. This may
   * throw if attempting to hold larger than what a 32-bit vector can store.
   *
   * @param valueCount value count
   */
  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    if (valueCount > 0) {
      while (valueCount > getValidityAndSizeValueCapacity()) {
        /* check if validity and offset buffers need to be re-allocated */
        reallocValidityAndSizeAndOffsetBuffers();
      }
    }
    /* valueCount for the data vector is the current end offset */
    final long childValueCount = (valueCount == 0) ? 0 : getLengthOfChildVector();
    /* set the value count of data vector and this will take care of
     * checking whether data buffer needs to be reallocated.
     * TODO: revisit when 64-bit vectors are supported
     */
    Preconditions.checkArgument(
        childValueCount <= Integer.MAX_VALUE && childValueCount >= 0,
        "LargeListViewVector doesn't yet support 64-bit allocations: %s",
        childValueCount);
    vector.setValueCount((int) childValueCount);
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    AddOrGetResult<T> result = super.addOrGetVector(fieldType);
    invalidateReader();
    return result;
  }

  @Override
  public UnionVector promoteToUnion() {
    UnionVector vector = new UnionVector("$data$", allocator, /* field type*/ null, callBack);
    replaceDataVector(vector);
    invalidateReader();
    if (callBack != null) {
      callBack.doWork();
    }
    return vector;
  }

  private void invalidateReader() {
    reader = null;
  }

  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  public UnionLargeListViewWriter getWriter() {
    return new UnionLargeListViewWriter(this);
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }

  /**
   * Get the density of this LargeListViewVector.
   *
   * @return density
   */
  public double getDensity() {
    if (valueCount == 0) {
      return 0.0D;
    }
    final double totalListSize = getLengthOfChildVector();
    return totalListSize / valueCount;
  }

  /** Validating LargeListViewVector creation based on the specification guideline. */
  @Override
  public void validate() {
    for (int i = 0; i < valueCount; i++) {
      final int offset = offsetBuffer.getInt((long) i * OFFSET_WIDTH);
      final int size = sizeBuffer.getInt((long) i * SIZE_WIDTH);
      validateInvariants(offset, size);
    }
  }

  /**
   * End the current value.
   *
   * @param index index of the value to end
   * @param size number of elements in the list that was written
   */
  public void endValue(int index, int size) {
    sizeBuffer.setInt((long) index * SIZE_WIDTH, size);
  }
}
