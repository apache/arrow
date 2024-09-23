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

import static org.apache.arrow.vector.NullCheckingForGet.NULL_CHECKING_ENABLED;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.vector.complex.impl.ViewVarBinaryReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableViewVarBinaryHolder;
import org.apache.arrow.vector.holders.ViewVarBinaryHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.TransferPair;

/**
 * ViewVarBinaryVector implements a variable width view vector of binary values which could be NULL.
 * A validity buffer (bit vector) is maintained to track which elements in the vector are null.
 */
public final class ViewVarBinaryVector extends BaseVariableWidthViewVector
    implements ValueIterableVector<byte[]> {

  /**
   * Instantiate a ViewVarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public ViewVarBinaryVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.VIEWVARBINARY.getType()), allocator);
  }

  /**
   * Instantiate a ViewVarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarBinaryVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a ViewVarBinaryVector. This doesn't allocate any memory for the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarBinaryVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new ViewVarBinaryReaderImpl(ViewVarBinaryVector.this);
  }

  /**
   * Get a minor type for this vector. The vector holds values belonging to a particular type.
   *
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VIEWVARBINARY;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Get the variable length element at specified index as a byte array.
   *
   * @param index position of an element to get
   * @return array of bytes for a non-null element, null otherwise
   */
  public byte[] get(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }
    return getData(index);
  }

  /**
   * Read the value at the given position to the given output buffer. The caller is responsible for
   * checking for nullity first.
   *
   * @param index position of an element.
   * @param buffer the buffer to write into.
   */
  @Override
  public void read(int index, ReusableBuffer<?> buffer) {
    getData(index, buffer);
  }

  /**
   * Get the variable length element at a specified index as a byte array.
   *
   * @param index position of an element to get
   * @return byte array for a non-null element, null otherwise
   */
  @Override
  public byte[] getObject(int index) {
    return get(index);
  }

  /**
   * Get the variable length element at specified index and sets the state in provided holder.
   *
   * @param index position of an element to get
   * @param holder data holder to be populated by this function
   */
  public void get(int index, NullableViewVarBinaryHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40936
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value setter methods                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, ViewVarBinaryHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40936
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Same as {@link #set(int, ViewVarBinaryHolder)} except that it handles the case where index and
   * length of a new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, ViewVarBinaryHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40936
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Set the variable length element at the specified index to the data buffer supplied in the
   * holder.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void set(int index, NullableViewVarBinaryHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40936
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /**
   * Same as {@link #set(int, NullableViewVarBinaryHolder)} except that it handles the case where
   * index and length of a new element are beyond the existing capacity of the vector.
   *
   * @param index position of the element to set
   * @param holder holder that carries data buffer.
   */
  public void setSafe(int index, NullableViewVarBinaryHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40936
    throw new UnsupportedOperationException("Unsupported operation");
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |                      vector transfer                           |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Construct a TransferPair comprising this and a target vector of the same type.
   *
   * @param ref name of the target vector
   * @param allocator allocator for the target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return new TransferImpl(ref, allocator);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return new TransferImpl(field, allocator);
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param to target vector
   * @return {@link TransferPair}
   */
  @Override
  public TransferPair makeTransferPair(ValueVector to) {
    return new TransferImpl((ViewVarBinaryVector) to);
  }

  private class TransferImpl implements TransferPair {
    ViewVarBinaryVector to;

    public TransferImpl(String ref, BufferAllocator allocator) {
      to = new ViewVarBinaryVector(ref, field.getFieldType(), allocator);
    }

    public TransferImpl(Field field, BufferAllocator allocator) {
      to = new ViewVarBinaryVector(field, allocator);
    }

    public TransferImpl(ViewVarBinaryVector to) {
      this.to = to;
    }

    @Override
    public ViewVarBinaryVector getTo() {
      return to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      splitAndTransferTo(startIndex, length, to);
    }

    @Override
    public void copyValueSafe(int fromIndex, int toIndex) {
      to.copyFromSafe(fromIndex, toIndex, ViewVarBinaryVector.this);
    }
  }
}
