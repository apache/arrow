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

import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A run-end encoded vector contains only two child vectors: a run_end vector of type int and a
 * values vector of any type. There are no buffers associated with the parent vector.
 */
public class RunEndEncodedVector extends BaseValueVector implements FieldVector {
  public static final FieldVector DEFAULT_VALUE_VECTOR = ZeroVector.INSTANCE;
  public static final FieldVector DEFAULT_RUN_END_VECTOR = ZeroVector.INSTANCE;

  public static RunEndEncodedVector empty(String name, BufferAllocator allocator) {
    return new RunEndEncodedVector(
        name, allocator, FieldType.notNullable(ArrowType.RunEndEncoded.INSTANCE), null);
  }

  protected final CallBack callBack;
  protected Field field;
  protected FieldVector runEndsVector;
  protected FieldVector valuesVector;
  protected int valueCount;

  /**
   * Constructs a new instance.
   *
   * @param name The name of the instance.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param fieldType The type of the array that is run-end encoded.
   * @param callBack A schema change callback.
   */
  public RunEndEncodedVector(
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
  public RunEndEncodedVector(Field field, BufferAllocator allocator, CallBack callBack) {
    this(field, allocator, DEFAULT_RUN_END_VECTOR, DEFAULT_VALUE_VECTOR, callBack);
  }

  /**
   * Constructs a new instance.
   *
   * @param field The field materialized by this vector.
   * @param allocator The allocator to use for allocating/reallocating buffers.
   * @param runEndsVector The vector represents run ends. Only Zero vector or type int vector with
   *     size 16, 32 is allowed
   * @param valuesVector The vector represents values
   * @param callBack A schema change callback.
   */
  public RunEndEncodedVector(
      Field field,
      BufferAllocator allocator,
      FieldVector runEndsVector,
      FieldVector valuesVector,
      CallBack callBack) {
    super(allocator);
    this.field = field;
    this.callBack = callBack;
    this.valueCount = 0;
    this.runEndsVector = runEndsVector;
    this.valuesVector = valuesVector;
  }

  /** ValueVector interface */

  /**
   * Allocate new buffers. ValueVector implements logic to determine how much to allocate.
   *
   * @throws OutOfMemoryException Thrown if no memory can be allocated.
   */
  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException("Failure while allocating memory");
    }
  }

  /**
   * Allocates new buffers. ValueVector implements logic to determine how much to allocate.
   *
   * @return Returns true if allocation was successful.
   */
  @Override
  public boolean allocateNewSafe() {
    initializeChildrenFromFields(field.getChildren());
    for (FieldVector v : getChildrenFromFields()) {
      boolean isAllocated = v.allocateNewSafe();
      if (!isAllocated) {
        v.clear();
        return false;
      }
    }
    return true;
  }

  /**
   * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's
   * buffer with new buffer, and release old one
   */
  @Override
  public void reAlloc() {
    for (FieldVector v : getChildrenFromFields()) {
      v.reAlloc();
    }
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  protected FieldReader getReaderImpl() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Set the initial record capacity.
   *
   * @param numRecords the initial record capacity.
   */
  @Override
  public void setInitialCapacity(int numRecords) {}

  /**
   * Returns the maximum number of values that can be stored in this vector instance.
   *
   * @return the maximum number of values that can be stored in this vector instance.
   */
  @Override
  public int getValueCapacity() {
    return getChildrenFromFields().stream()
        .mapToInt(item -> item != null ? item.getValueCapacity() : 0)
        .min()
        .orElseThrow(NoSuchElementException::new);
  }

  /** Alternative to clear(). Allows use as an AutoCloseable in try-with-resources. */
  @Override
  public void close() {
    for (FieldVector v : getChildrenFromFields()) {
      v.close();
    }
  }

  /**
   * Release any owned ArrowBuf and reset the ValueVector to the initial state. If the vector has
   * any child vectors, they will also be cleared.
   */
  @Override
  public void clear() {
    for (FieldVector v : getChildrenFromFields()) {
      v.clear();
    }
  }

  /**
   * Reset the ValueVector to the initial state without releasing any owned ArrowBuf. Buffer
   * capacities will remain unchanged and any previous data will be zeroed out. This includes
   * buffers for data, validity, offset, etc. If the vector has any child vectors, they will also be
   * reset.
   */
  @Override
  public void reset() {
    for (FieldVector v : getChildrenFromFields()) {
      v.reset();
    }
    valueCount = 0;
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

  @Override
  public MinorType getMinorType() {
    return MinorType.RUNENDENCODED;
  }

  /**
   * To transfer quota responsibility.
   *
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    throw new UnsupportedOperationException(
        "RunEndEncodedVector does not support getTransferPair(BufferAllocator)");
  }

  /**
   * To transfer quota responsibility.
   *
   * @param ref the name of the vector
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  /**
   * To transfer quota responsibility.
   *
   * @param field the Field object used by the target vector
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return getTransferPair(field, allocator, null);
  }

  /**
   * To transfer quota responsibility.
   *
   * @param ref the name of the vector
   * @param allocator the target allocator
   * @param callBack A schema change callback.
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException(
        "RunEndEncodedVector does not support getTransferPair(String, BufferAllocator, CallBack)");
  }

  /**
   * To transfer quota responsibility.
   *
   * @param field the Field object used by the target vector
   * @param allocator the target allocator
   * @param callBack A schema change callback.
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    throw new UnsupportedOperationException(
        "RunEndEncodedVector does not support getTransferPair(Field, BufferAllocator, CallBack)");
  }

  /**
   * Makes a new transfer pair used to transfer underlying buffers.
   *
   * @param target the target for the transfer
   * @return a new {@link org.apache.arrow.vector.util.TransferPair transfer pair} that is used to
   *     transfer underlying buffers into the target vector.
   */
  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    throw new UnsupportedOperationException(
        "RunEndEncodedVector does not support makeTransferPair(ValueVector)");
  }

  /**
   * Get a reader for this vector.
   *
   * @return a {@link org.apache.arrow.vector.complex.reader.FieldReader field reader} that supports
   *     reading values from this vector.
   */
  @Override
  public FieldReader getReader() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Get a writer for this vector.
   *
   * @return a {@link org.apache.arrow.vector.complex.writer.FieldWriter field writer} that supports
   *     writing values to this vector.
   */
  public FieldWriter getWriter() {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  /**
   * Get the number of bytes used by this vector.
   *
   * @return the number of bytes that is used by this vector instance.
   */
  @Override
  public int getBufferSize() {
    int bufferSize = 0;
    for (FieldVector v : getChildrenFromFields()) {
      bufferSize += v.getBufferSize();
    }
    return bufferSize;
  }

  /**
   * Returns the number of bytes that is used by this vector if it holds the given number of values.
   * The result will be the same as if setValueCount() were called, followed by calling
   * getBufferSize(), but without any of the closing side-effects that setValueCount() implies wrt
   * finishing off the population of a vector. Some operations might wish to use this to determine
   * how much memory has been used by a vector so far, even though it is not finished being
   * populated.
   *
   * @param valueCount the number of values to assume this vector contains
   * @return the buffer size if this vector is holding valueCount values
   */
  @Override
  public int getBufferSizeFor(int valueCount) {
    return 0;
  }

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the
   * reference counts for this buffer so it only should be used for in-context access. Also note
   * that this buffer changes regularly thus external classes shouldn't hold a reference to it
   * (unless they change it).
   *
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted;
   *     but the returned array will be the only reference to them
   * @return The underlying {@link ArrowBuf buffers} that is used by this vector instance.
   */
  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    return new ArrowBuf[0];
  }

  /**
   * Gets the underlying buffer associated with validity vector.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getValidityBuffer() {
    throw new UnsupportedOperationException(
        "Run-end encoded vectors do not have a validity buffer.");
  }

  /**
   * Gets the underlying buffer associated with data vector.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getDataBuffer() {
    throw new UnsupportedOperationException("Run-end encoded vectors do not have a data buffer.");
  }

  /**
   * Gets the underlying buffer associated with offset vector.
   *
   * @return buffer
   */
  @Override
  public ArrowBuf getOffsetBuffer() {
    throw new UnsupportedOperationException("Run-end encoded vectors do not have a offset buffer.");
  }

  /**
   * Gets the number of values.
   *
   * @return number of values in the vector
   */
  @Override
  public int getValueCount() {
    return valueCount;
  }

  /** Set number of values in the vector. */
  @Override
  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
  }

  /**
   * Get friendly type object from the vector.
   *
   * @param index index of object to get
   * @return friendly type object
   */
  @Override
  public Object getObject(int index) {
    checkIndex(index);
    int physicalIndex = getPhysicalIndex(index);
    return valuesVector.getObject(physicalIndex);
  }

  /**
   * Get the run end of giving index.
   *
   * @param index index of the run end to get
   * @return the run end of giving index
   */
  public int getRunEnd(int index) {
    checkIndex(index);
    int physicalIndex = getPhysicalIndex(index);
    return (int) ((BaseIntVector) runEndsVector).getValueAsLong(physicalIndex);
  }

  /**
   * Returns number of null elements in the vector.
   *
   * @return number of null elements
   */
  @Override
  public int getNullCount() {
    // Null count is always 0 for run-end encoded array
    return 0;
  }

  /**
   * Check whether an element in the vector is null.
   *
   * @param index index to check for null
   * @return true if element is null
   */
  @Override
  public boolean isNull(int index) {
    int physicalIndex = getPhysicalIndex(runEndsVector, index);
    return valuesVector.isNull(physicalIndex);
  }

  /** Returns hashCode of element in index with the default hasher. */
  @Override
  public int hashCode(int index) {
    return hashCode(index, null);
  }

  /** Returns hashCode of element in index with the given hasher. */
  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    int hash = 0;
    for (FieldVector v : getChildrenFromFields()) {
      if (index < v.getValueCount()) {
        hash = ByteFunctionHelpers.combineHash(hash, v.hashCode(index, hasher));
      }
    }
    return hash;
  }

  /**
   * Accept a generic {@link VectorVisitor} and return the result.
   *
   * @param <OUT> the output result type.
   * @param <IN> the input data together with visitor.
   */
  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
    return visitor.visit(this, value);
  }

  /**
   * Gets the name of the vector.
   *
   * @return the name of the vector.
   */
  @Override
  public String getName() {
    return this.field.getName();
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.<ValueVector>unmodifiableCollection(getChildrenFromFields()).iterator();
  }

  /** FieldVector interface */

  /**
   * Initializes the child vectors to be later loaded with loadBuffers.
   *
   * @param children the schema containing the run_ends column first and the values column second
   */
  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    checkArgument(
        children.size() == 2,
        "Run-end encoded vectors must have two child Fields. Found: %s",
        children.isEmpty() ? "none" : children);
    checkArgument(
        Arrays.asList(
                MinorType.SMALLINT.getType(), MinorType.INT.getType(), MinorType.BIGINT.getType())
            .contains(children.get(0).getType()),
        "The first field represents the run-end vector and must be of type int "
            + "with size 16, 32, or 64 bits. Found: %s",
        children.get(0).getType());
    runEndsVector = (BaseIntVector) children.get(0).createVector(allocator);
    valuesVector = children.get(1).createVector(allocator);
    field = new Field(field.getName(), field.getFieldType(), children);
  }

  /**
   * The returned list is the same size as the list passed to initializeChildrenFromFields.
   *
   * @return the children according to schema (empty for primitive types)
   */
  @Override
  public List<FieldVector> getChildrenFromFields() {
    return Arrays.asList(runEndsVector, valuesVector);
  }

  /**
   * Loads data in the vectors. (ownBuffers must be the same size as getFieldVectors())
   *
   * @param fieldNode the fieldNode
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (!ownBuffers.isEmpty()) {
      throw new UnsupportedOperationException(
          "Run-end encoded vectors do not have any associated buffers.");
    }
  }

  /**
   * Get the buffers of the fields, (same size as getFieldVectors() since it is their content).
   *
   * @return the buffers containing the data for this vector (ready for reading)
   */
  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return List.of();
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
    throw new UnsupportedOperationException("There are no inner vectors. Use getFieldBuffers().");
  }

  /**
   * Gets the starting address of the underlying buffer associated with validity vector.
   *
   * @return buffer address
   */
  @Override
  public long getValidityBufferAddress() {
    throw new UnsupportedOperationException(
        "Run-end encoded vectors do not have a validity buffer.");
  }

  /**
   * Gets the starting address of the underlying buffer associated with data vector.
   *
   * @return buffer address
   */
  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException("Run-end encoded vectors do not have a data buffer.");
  }

  /**
   * Gets the starting address of the underlying buffer associated with offset vector.
   *
   * @return buffer address
   */
  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException(
        "Run-end encoded vectors do not have an offset buffer.");
  }

  /**
   * Set the element at the given index to null.
   *
   * @param index the value to change
   */
  @Override
  public void setNull(int index) {
    throw new UnsupportedOperationException(
        "Run-end encoded vectors do not have a validity buffer.");
  }

  public FieldVector getRunEndsVector() {
    return runEndsVector;
  }

  public FieldVector getValuesVector() {
    return valuesVector;
  }

  private void checkIndex(int logicalIndex) {
    if (logicalIndex < 0 || logicalIndex >= valueCount) {
      throw new IndexOutOfBoundsException(
          String.format("index: %s, expected range (0, %s)", logicalIndex, valueCount));
    }
  }

  /**
   * The physical index is the index of the first value that is larger than logical index. e.g. if
   * run_ends is [1,2,3], the physical index of logical index from 0 to 5 is [0, 1, 1, 2, 2, 2]
   */
  public int getPhysicalIndex(int logicalIndex) {
    return getPhysicalIndex(runEndsVector, logicalIndex);
  }

  static int getPhysicalIndex(FieldVector runEndVector, int logicalIndex) {
    if (runEndVector == null || runEndVector.getValueCount() == 0) {
      return -1;
    }

    int low = 0;
    int high = runEndVector.getValueCount() - 1;
    int result = -1;

    while (low <= high) {
      int mid = low + (high - low) / 2;
      long valueAsLong = ((BaseIntVector) runEndVector).getValueAsLong(mid);
      if (valueAsLong > logicalIndex) {
        result = mid;
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }

    return result;
  }
}
