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

import java.io.Closeable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

import io.netty.buffer.ArrowBuf;

/**
 * An abstraction that is used to store a sequence of values in an individual column.
 *
 * A {@link ValueVector value vector} stores underlying data in-memory in a columnar fashion that is compact and
 * efficient. The column whose data is stored, is referred by {@link #getField()}.
 *
 * It is important that vector is allocated before attempting to read or write.
 *
 * There are a few "rules" around vectors:
 *
 * <ul>
 *   <li>values need to be written in order (e.g. index 0, 1, 2, 5)</li>
 *   <li>null vectors start with all values as null before writing anything</li>
 *   <li>for variable width types, the offset vector should be all zeros before writing</li>
 *   <li>you must call setValueCount before a vector can be read</li>
 *   <li>you should never write to a vector once it has been read.</li>
 * </ul>
 *
 * Please note that the current implementation doesn't enfore those rules, hence we may find few places that
 * deviate from these rules (e.g. offset vectors in Variable Length and Repeated vector)
 *
 * This interface "should" strive to guarantee this order of operation:
 * <blockquote>
 * allocate > mutate > setvaluecount > access > clear (or allocate to start the process over).
 * </blockquote>
 */
public interface ValueVector extends Closeable, Iterable<ValueVector> {
  /**
   * Allocate new buffers. ValueVector implements logic to determine how much to allocate.
   * @throws OutOfMemoryException Thrown if no memory can be allocated.
   */
  void allocateNew() throws OutOfMemoryException;

  /**
   * Allocates new buffers. ValueVector implements logic to determine how much to allocate.
   * @return Returns true if allocation was successful.
   */
  boolean allocateNewSafe();

  BufferAllocator getAllocator();

  /**
   * Set the initial record capacity
   * @param numRecords the initial record capacity.
   */
  void setInitialCapacity(int numRecords);

  /**
   * Returns the maximum number of values that can be stored in this vector instance.
   */
  int getValueCapacity();

  /**
   * Alternative to clear(). Allows use as an AutoCloseable in try-with-resources.
   */
  @Override
  void close();

  /**
   * Release the underlying ArrowBuf and reset the ValueVector to empty.
   */
  void clear();

  /**
   * Get information about how this field is materialized.
   */
  Field getField();

  MinorType getMinorType();

  /**
   * Returns a {@link org.apache.arrow.vector.util.TransferPair transfer pair}, creating a new target vector of
   * the same type.
   */
  TransferPair getTransferPair(BufferAllocator allocator);

  TransferPair getTransferPair(String ref, BufferAllocator allocator);

  TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack);

  /**
   * Returns a new {@link org.apache.arrow.vector.util.TransferPair transfer pair} that is used to transfer underlying
   * buffers into the target vector.
   */
  TransferPair makeTransferPair(ValueVector target);

  /**
   * Returns an {@link org.apache.arrow.vector.ValueVector.Accessor accessor} that is used to read from this vector
   * instance.
   */
  Accessor getAccessor();

  /**
   * Returns an {@link org.apache.arrow.vector.ValueVector.Mutator mutator} that is used to write to this vector
   * instance.
   */
  Mutator getMutator();

  /**
   * Returns a {@link org.apache.arrow.vector.complex.reader.FieldReader field reader} that supports reading values
   * from this vector.
   */
  FieldReader getReader();

  /**
   * Returns the number of bytes that is used by this vector instance.
   */
  int getBufferSize();

  /**
   * Returns the number of bytes that is used by this vector if it holds the given number
   * of values. The result will be the same as if Mutator.setValueCount() were called, followed
   * by calling getBufferSize(), but without any of the closing side-effects that setValueCount()
   * implies wrt finishing off the population of a vector. Some operations might wish to use
   * this to determine how much memory has been used by a vector so far, even though it is
   * not finished being populated.
   *
   * @param valueCount the number of values to assume this vector contains
   * @return the buffer size if this vector is holding valueCount values
   */
  int getBufferSizeFor(int valueCount);

  /**
   * Return the underlying buffers associated with this vector. Note that this doesn't impact the reference counts for
   * this buffer so it only should be used for in-context access. Also note that this buffer changes regularly thus
   * external classes shouldn't hold a reference to it (unless they change it).
   * @param clear Whether to clear vector before returning; the buffers will still be refcounted;
   *   but the returned array will be the only reference to them
   *
   * @return The underlying {@link io.netty.buffer.ArrowBuf buffers} that is used by this vector instance.
   */
  ArrowBuf[] getBuffers(boolean clear);

  /**
   * An abstraction that is used to read from this vector instance.
   */
  interface Accessor {
    /**
     * Get the Java Object representation of the element at the specified position. Useful for testing.
     *
     * @param index
     *          Index of the value to get
     */
    Object getObject(int index);

    /**
     * Returns the number of values that is stored in this vector.
     */
    int getValueCount();

    /**
     * Returns true if the value at the given index is null, false otherwise.
     */
    boolean isNull(int index);

    /**
     * Returns the number of null values
     */
    int getNullCount();
  }

  /**
   * An abstraction that is used to write into this vector instance.
   */
  interface Mutator {
    /**
     * Sets the number of values that is stored in this vector to the given value count.
     *
     * @param valueCount  value count to set.
     */
    void setValueCount(int valueCount);

    /**
     * Resets the mutator to pristine state.
     */
    void reset();

    /**
     * @deprecated  this has nothing to do with value vector abstraction and should be removed.
     */
    @Deprecated
    void generateTestData(int values);
  }
}
