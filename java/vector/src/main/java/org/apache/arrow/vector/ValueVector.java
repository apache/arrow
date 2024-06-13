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

import java.io.Closeable;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.util.ValueVectorUtility;

/**
 * An abstraction that is used to store a sequence of values in an individual column.
 *
 * <p>A {@link ValueVector value vector} stores underlying data in-memory in a columnar fashion that
 * is compact and efficient. The column whose data is stored, is referred by {@link #getField()}.
 *
 * <p>It is important that vector is allocated before attempting to read or write.
 *
 * <p>There are a few "rules" around vectors:
 *
 * <ul>
 *   <li>values need to be written in order (e.g. index 0, 1, 2, 5)
 *   <li>null vectors start with all values as null before writing anything
 *   <li>for variable width types, the offset vector should be all zeros before writing
 *   <li>you must call setValueCount before a vector can be read
 *   <li>you should never write to a vector once it has been read.
 * </ul>
 *
 * <p>Please note that the current implementation doesn't enforce those rules, hence we may find few
 * places that deviate from these rules (e.g. offset vectors in Variable Length and Repeated vector)
 *
 * <p>This interface "should" strive to guarantee this order of operation:
 *
 * <blockquote>
 *
 * allocate &gt; mutate &gt; setvaluecount &gt; access &gt; clear (or allocate to start the process
 * over).
 *
 * </blockquote>
 */
public interface ValueVector extends Closeable, Iterable<ValueVector> {
  /**
   * Allocate new buffers. ValueVector implements logic to determine how much to allocate.
   *
   * @throws OutOfMemoryException Thrown if no memory can be allocated.
   */
  void allocateNew() throws OutOfMemoryException;

  /**
   * Allocates new buffers. ValueVector implements logic to determine how much to allocate.
   *
   * @return Returns true if allocation was successful.
   */
  boolean allocateNewSafe();

  /**
   * Allocate new buffer with double capacity, and copy data into the new buffer. Replace vector's
   * buffer with new buffer, and release old one
   */
  void reAlloc();

  BufferAllocator getAllocator();

  /**
   * Set the initial record capacity.
   *
   * @param numRecords the initial record capacity.
   */
  void setInitialCapacity(int numRecords);

  /**
   * Returns the maximum number of values that can be stored in this vector instance.
   *
   * @return the maximum number of values that can be stored in this vector instance.
   */
  int getValueCapacity();

  /** Alternative to clear(). Allows use as an AutoCloseable in try-with-resources. */
  @Override
  void close();

  /**
   * Release any owned ArrowBuf and reset the ValueVector to the initial state. If the vector has
   * any child vectors, they will also be cleared.
   */
  void clear();

  /**
   * Reset the ValueVector to the initial state without releasing any owned ArrowBuf. Buffer
   * capacities will remain unchanged and any previous data will be zeroed out. This includes
   * buffers for data, validity, offset, etc. If the vector has any child vectors, they will also be
   * reset.
   */
  void reset();

  /**
   * Get information about how this field is materialized.
   *
   * @return the field corresponding to this vector
   */
  Field getField();

  MinorType getMinorType();

  /**
   * To transfer quota responsibility.
   *
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  TransferPair getTransferPair(BufferAllocator allocator);

  /**
   * To transfer quota responsibility.
   *
   * @param ref the name of the vector
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  TransferPair getTransferPair(String ref, BufferAllocator allocator);

  /**
   * To transfer quota responsibility.
   *
   * @param field the Field object used by the target vector
   * @param allocator the target allocator
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  TransferPair getTransferPair(Field field, BufferAllocator allocator);

  /**
   * To transfer quota responsibility.
   *
   * @param ref the name of the vector
   * @param allocator the target allocator
   * @param callBack A schema change callback.
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack);

  /**
   * To transfer quota responsibility.
   *
   * @param field the Field object used by the target vector
   * @param allocator the target allocator
   * @param callBack A schema change callback.
   * @return a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new
   *     target vector of the same type.
   */
  TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack);

  /**
   * Makes a new transfer pair used to transfer underlying buffers.
   *
   * @param target the target for the transfer
   * @return a new {@link org.apache.arrow.vector.util.TransferPair transfer pair} that is used to
   *     transfer underlying buffers into the target vector.
   */
  TransferPair makeTransferPair(ValueVector target);

  /**
   * Get a reader for this vector.
   *
   * @return a {@link org.apache.arrow.vector.complex.reader.FieldReader field reader} that supports
   *     reading values from this vector.
   */
  FieldReader getReader();

  /**
   * Get the number of bytes used by this vector.
   *
   * @return the number of bytes that is used by this vector instance.
   */
  int getBufferSize();

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
  int getBufferSizeFor(int valueCount);

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
  ArrowBuf[] getBuffers(boolean clear);

  /**
   * Gets the underlying buffer associated with validity vector.
   *
   * @return buffer
   */
  ArrowBuf getValidityBuffer();

  /**
   * Gets the underlying buffer associated with data vector.
   *
   * @return buffer
   */
  ArrowBuf getDataBuffer();

  /**
   * Gets the underlying buffer associated with offset vector.
   *
   * @return buffer
   */
  ArrowBuf getOffsetBuffer();

  /**
   * Gets the number of values.
   *
   * @return number of values in the vector
   */
  int getValueCount();

  /** Set number of values in the vector. */
  void setValueCount(int valueCount);

  /**
   * Get friendly type object from the vector.
   *
   * @param index index of object to get
   * @return friendly type object
   */
  Object getObject(int index);

  /**
   * Returns number of null elements in the vector.
   *
   * @return number of null elements
   */
  int getNullCount();

  /**
   * Check whether an element in the vector is null.
   *
   * @param index index to check for null
   * @return true if element is null
   */
  boolean isNull(int index);

  /** Returns hashCode of element in index with the default hasher. */
  int hashCode(int index);

  /** Returns hashCode of element in index with the given hasher. */
  int hashCode(int index, ArrowBufHasher hasher);

  /**
   * Copy a cell value from a particular index in source vector to a particular position in this
   * vector.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  void copyFrom(int fromIndex, int thisIndex, ValueVector from);

  /**
   * Same as {@link #copyFrom(int, int, ValueVector)} except that it handles the case when the
   * capacity of the vector needs to be expanded before copy.
   *
   * @param fromIndex position to copy from in source vector
   * @param thisIndex position to copy to in this vector
   * @param from source vector
   */
  void copyFromSafe(int fromIndex, int thisIndex, ValueVector from);

  /**
   * Accept a generic {@link VectorVisitor} and return the result.
   *
   * @param <OUT> the output result type.
   * @param <IN> the input data together with visitor.
   */
  <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value);

  /**
   * Gets the name of the vector.
   *
   * @return the name of the vector.
   */
  String getName();

  default void validate() {
    ValueVectorUtility.validate(this);
  }

  default void validateFull() {
    ValueVectorUtility.validateFull(this);
  }
}
