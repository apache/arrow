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

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
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

public class ListViewVector extends BaseRepeatedValueViewVector implements PromotableVector {

  protected ArrowBuf validityBuffer;
  protected UnionListReader reader;
  private CallBack callBack;
  protected Field field;
  protected int validityAllocationSizeInBytes;

  /**
   * The maximum index that is actually set.
   */
  protected int lastSet;

  public static ListViewVector empty(String name, BufferAllocator allocator) {
    return new ListViewVector(name, allocator, FieldType.nullable(ArrowType.ListView.INSTANCE), null);
  }

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param fieldType The type of this list.
   * @param callBack A schema change callback.
   */
  public ListViewVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    this(new Field(name, fieldType, null), allocator, callBack);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param callBack A schema change callback.
   */
  public ListViewVector(Field field, BufferAllocator allocator, CallBack callBack) {
    super(field.getName(), allocator, callBack);
    this.validityBuffer = allocator.getEmpty();
    this.field = field;
    this.callBack = callBack;
    this.validityAllocationSizeInBytes = getValidityBufferSizeFromCount(INITIAL_VALUE_ALLOCATION);
    this.lastSet = -1;
  }



  @Override
  public void initializeChildrenFromFields(List<Field> children) {

  }

  @Override
  public void setInitialCapacity(int numRecords) {
    validityAllocationSizeInBytes = getValidityBufferSizeFromCount(numRecords);
    super.setInitialCapacity(numRecords);
  }

  @Override
  public void setInitialCapacity(int numRecords, double density) {

  }

  @Override
  public void setInitialTotalCapacity(int numRecords, int totalNumberOfElements) {
    super.setInitialTotalCapacity(numRecords, totalNumberOfElements);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return singletonList(getDataVector());
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {

  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return null;
  }

  @Override
  public void exportCDataBuffers(List<ArrowBuf> buffers, ArrowBuf buffersPtr, long nullValue) {

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
      /* release the current buffers, hence this is a new allocation */
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
    reallocOffsetBuffer();
    reallocValidityBuffer();
    reallocSizeBuffer();
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
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {

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
    throw new UnsupportedOperationException();
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    throw new UnsupportedOperationException();
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

  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return 0;
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FieldReader getReaderImpl() {
    throw new UnsupportedOperationException();
  }

  @Override
  public UnionListReader getReader() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBufferSize() {
    return 0;
  }

  @Override
  public int getBufferSizeFor(int valueCount) {
    return 0;
  }

  @Override
  public Field getField() {
    if (field.getChildren().contains(getDataVector().getField())) {
      return field;
    }
    field = new Field(field.getName(), field.getFieldType(), Collections.singletonList(getDataVector().getField()));
    return field;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.LIST;
  }

  @Override
  public void clear() {
    super.clear();
    validityBuffer = releaseBuffer(validityBuffer);
    lastSet = -1;
  }

  @Override
  public void reset() {
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

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
      vals.add(vv.getObject(i));
    }

    return vals;
  }

  @Override
  public boolean isNull(int index) {
    return (isSet(index) == 0);
  }

  @Override
  public boolean isEmpty(int index) {
    return false;
  }

  /**
   * Same as {@link #isNull(int)}.
   *
   * @param index  position of the element
   * @return 1 if element at given index is not null, 0 otherwise
   */
  public int isSet(int index) {
    final int byteIndex = index >> 3;
    final byte b = validityBuffer.getByte(byteIndex);
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  private int getValidityAndSizeValueCapacity() {
    final int offsetValueCapacity = Math.max(getOffsetBufferValueCapacity(), 0);
    final int sizeValueCapacity = Math.max(getSizeBufferValueCapacity(), 0);
    return Math.min(offsetValueCapacity, sizeValueCapacity);
  }

  private int getValidityBufferValueCapacity() {
    return capAtMaxInt(validityBuffer.capacity() * 8);
  }

  @Override
  public void setNull(int index) {

  }

  /**
   * Start new value in the ListView vector.
   * There are a few cases that are handled in this function.
   * There are two main scenarios that need to be considered.
   * The first scenario is simple insertion where indices are continuously updated.
   * The other scenario is the event of non-continuous writing,
   * the offset buffer needs to be updated.
   *
   * @param index index of the value to start
   * @return offset of the new value
   */
  @Override
  public int startNewValue(int index) {
    while (index >= getValidityAndSizeValueCapacity()) {
      reallocValidityAndSizeAndOffsetBuffers();
    }

    if (lastSet >= index) {
      lastSet = index - 1;
    }

    if (index == 0) {
      offsetBuffer.setInt(0, 0);
    } else if (index > lastSet) {
      /* when skipping indices, we need to update the offset buffer */
      /* setting offset from lastSet + 1 to index (included) */
      for (int i = lastSet + 1; i <= index; i++) {
        final int prevOffSet = offsetBuffer.getInt((i - 1L) * OFFSET_WIDTH);
        final int prevSize = sizeBuffer.getInt((i - 1L) * SIZE_WIDTH);
        final int currOffSet = prevOffSet + prevSize;
        offsetBuffer.setInt(i * OFFSET_WIDTH, currOffSet);
      }
    } else {
      final int prevOffset = offsetBuffer.getInt((index - 1) * OFFSET_WIDTH);
      final int prevSize = sizeBuffer.getInt((index - 1) * SIZE_WIDTH);
      final int currOffSet = prevOffset + prevSize;
      offsetBuffer.setInt(index * OFFSET_WIDTH, currOffSet);
    }

    BitVectorHelper.setBit(validityBuffer, index);
    lastSet = index;
    return offsetBuffer.getInt(index * OFFSET_WIDTH);
  }

  /**
   * Constructing a ListViewVector when the offsets, sizes and field vector are available.
   * <p>
   * Steps taken follow the workflow used in creating a ListViewVector with the API
   * used in ListVector.
   *
   * @param offSets new offSets to be set
   * @param sizes new sizes to be set
   * @param elementFieldVec new elements to be appended to the field vector
   */
  public void setNewValues(
      List<Integer> offSets, List<Integer> sizes, FieldVector elementFieldVec) {
    // Null checks
    Objects.requireNonNull(offSets, "Offsets cannot be null");
    Objects.requireNonNull(sizes, "Sizes cannot be null");
    Objects.requireNonNull(elementFieldVec, "Element Field Vector cannot be null");

    while (offSets.size() >= getValidityAndSizeValueCapacity()) {
      reallocValidityAndSizeAndOffsetBuffers();
    }

    /* TODO: add the validation method
     * 0 <= offsets[i] <= length of the child array
     * 0 <= offsets[i] + size[i] <= length of the child array
     */
    if (offSets.size() != sizes.size()) {
      throw new IllegalArgumentException("Offsets and sizes must be of the same size." +
          " Offsets size: " + offSets.size() + ", Sizes size: " + sizes.size());
    }

    UnionListViewWriter writer = this.getWriter();
    int lastSet = this.getLastSet();
    int listViewValueCount = this.getValueCount();
    int newListViewValueCount = listViewValueCount + offSets.size();
    lastSet += offSets.size();

    // set validity bit
    final ArrowBuf validityBuffer = this.getValidityBuffer();
    for (int i = listViewValueCount; i < newListViewValueCount; i++) {
      if (sizes.get(i - listViewValueCount) != null) {
        // only set validity bit if size is not 0
        BitVectorHelper.setBit(validityBuffer, i);
      }
    }

    final ArrowBuf offSetBuffer = this.getOffsetBuffer();
    final ArrowBuf sizeBuffer = this.getSizeBuffer();

    // set offset and size buffers
    for (int i = listViewValueCount; i < newListViewValueCount; i++) {
      writer.setPosition(i);
      offSetBuffer.setInt(i * OFFSET_WIDTH,
          offSets.get(i - listViewValueCount));
      // we use null to determine a list with size 0 and no list
      if (sizes.get(i - listViewValueCount) != null) {
        sizeBuffer.setInt(i * SIZE_WIDTH,
            sizes.get(i - listViewValueCount));
      } else {
        sizeBuffer.setInt(i * SIZE_WIDTH,
            0);
      }
    }

    // updating field vector
    if (this.getDataVector() instanceof NullVector) {
      this.vector = DEFAULT_DATA_VECTOR;
      this.vector.allocateNew();
    }

    if (this.getDataVector() instanceof NullVector) {
      this.vector = elementFieldVec;
    } else {
      FieldVector dataVec = this.getDataVector();
      for (int i = 0; i < elementFieldVec.getValueCount(); i++) {
        dataVec.copyFromSafe(i, i + dataVec.getValueCount(), elementFieldVec);
      }
      dataVec.setValueCount(dataVec.getValueCount() + elementFieldVec.getValueCount());
    }
    writer.setPosition(newListViewValueCount);
    this.setLastSet(lastSet);
    this.setValueCount(newListViewValueCount);
  }

  private int getLengthOfChildVector() {
    int length = 0;
    for (int i = 0; i < lastSet + 1; i++) {
      length += sizeBuffer.getInt(i * SIZE_WIDTH);
    }
    return length;
  }

  private void setValuesInBuffer(ArrowBuf srcBuf, ArrowBuf destBuf, long bufWidth, int length) {
    for (int i = 0; i < length; i++) {
      destBuf.setInt(i * bufWidth, srcBuf.getInt(i * bufWidth));
    }
  }

  /**
   * Constructing a ListViewVector when the offsets, sizes and field vector are available.
   * <p>
   * Steps taken follow the workflow used in creating a ListViewVector with the API
   * used in ListVector.
   *
   * @param offSetBuffer new offSet buffer to be set
   * @param sizeBuffer new size buffer to be set
   * @param validityBuffer new validity buffer to be set
   * @param elementFieldVec new elements to be appended to the field vector
   * @param valueCount number of lists to be set
   */
  public void set(ArrowBuf offSetBuffer, ArrowBuf sizeBuffer, ArrowBuf validityBuffer,
      FieldVector elementFieldVec, int valueCount) {
    // Null checks
    Objects.requireNonNull(offSetBuffer, "Offset buffer cannot be null");
    Objects.requireNonNull(sizeBuffer, "Size buffer cannot be null");
    Objects.requireNonNull(validityBuffer, "Validity buffer cannot be null");
    Objects.requireNonNull(elementFieldVec, "Element Field Vector cannot be null");

    // clear buffers
    this.offsetBuffer.clear();
    this.sizeBuffer.clear();
    this.validityBuffer.clear();
    // clear child vector
    this.vector.clear();
    // allocate memory
    this.vector.allocateNew();

    // set buffers
    setValuesInBuffer(offSetBuffer, this.offsetBuffer, OFFSET_WIDTH, valueCount);
    setValuesInBuffer(sizeBuffer, this.sizeBuffer, SIZE_WIDTH, valueCount);
    setValuesInBuffer(validityBuffer, this.validityBuffer, 1, valueCount);

    // set child vector
    this.vector = elementFieldVec;
    this.vector.setValueCount(elementFieldVec.getValueCount());

    this.lastSet = valueCount - 1;
    this.setValueCount(valueCount);
    this.getWriter().setPosition(valueCount);
  }

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
    final int childValueCount = (valueCount == 0) ? 0 : getLengthOfChildVector();
    /* set the value count of data vector and this will take care of
     * checking whether data buffer needs to be reallocated.
     */
    vector.setValueCount(childValueCount);
  }

  @Override
  public int getElementStartIndex(int index) {
    return 0;
  }

  @Override
  public int getElementEndIndex(int index) {
    return 0;
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

  protected void invalidateReader() {
    reader = null;
  }

  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers");
  }

  public UnionListViewWriter getWriter() {
    return new UnionListViewWriter(this);
  }

  public int getLastSet() {
    return lastSet;
  }

  public void setLastSet(int newLastSet) {
    lastSet = newLastSet;
  }

  @Override
  public int getValueCount() {
    return valueCount;
  }
}
