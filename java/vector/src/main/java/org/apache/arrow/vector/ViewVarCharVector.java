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
import org.apache.arrow.vector.complex.impl.ViewVarCharReaderImpl;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableViewVarCharHolder;
import org.apache.arrow.vector.holders.ViewVarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.arrow.vector.validate.ValidateUtil;

/**
 * ViewVarCharVector implements a view of a variable width vector of VARCHAR
 * values which could be NULL. A validity buffer (bit vector) is maintained
 * to track which elements in the vector are null. A viewBuffer keeps track
 * of all values in the vector, and an external data buffer is kept to keep longer
 * strings (>12).
 */
public final class ViewVarCharVector extends BaseVariableWidthViewVector {

  /**
   * Instantiate a ViewVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(String name, BufferAllocator allocator) {
    this(name, FieldType.nullable(MinorType.VARCHAR.getType()), allocator);
  }

  /**
   * Instantiate a ViewVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(String name, FieldType fieldType, BufferAllocator allocator) {
    this(new Field(name, fieldType, null), allocator);
  }

  /**
   * Instantiate a ViewVarCharVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  public ViewVarCharVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
  }

  @Override
  protected FieldReader getReaderImpl() {
    return new ViewVarCharReaderImpl(ViewVarCharVector.this);
  }

  /**
   * Get a minor type for this vector. The vector holds values belonging
   * to a particular type.
   * @return {@link org.apache.arrow.vector.types.Types.MinorType}
   */
  @Override
  public MinorType getMinorType() {
    return MinorType.VIEWVARCHAR;
  }

  /*----------------------------------------------------------------*
  |                                                                |
  |          vector value retrieval methods                        |
  |                                                                |
  *----------------------------------------------------------------*/

  /**
   * Get the variable length element at specified index as a byte array.
   *
   * @param index   position of an element to get
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
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of an element to get
   * @return Text object for a non-null element, null otherwise
   */
  @Override
  public Text getObject(int index) {
    assert index >= 0;
    if (NULL_CHECKING_ENABLED && isSet(index) == 0) {
      return null;
    }

    final Text result = new Text();
    read(index, result);
    return result;
  }

  /**
   * Read the value at the given position to the given output buffer.
   * The caller is responsible for checking for nullity first.
   *
   * @param index position of an element.
   * @param buffer the buffer to write into.
   */
  @Override
  public void read(int index, ReusableBuffer<?> buffer) {
    getData(index, buffer);
  }

  /**
   * Get the variable length element at specified index and sets the state
   * in provided holder.
   *
   * @param index   position of an element to get
   * @param holder  data holder to be populated by this function
   */
  public void get(int index, NullableViewVarCharHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40937
    throw new UnsupportedOperationException("NullableViewVarCharHolder get operation not supported");
  }


  /*----------------------------------------------------------------*
   |                                                                |
   |          vector value setter methods                           |
   |                                                                |
   *----------------------------------------------------------------*/


  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, ViewVarCharHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40937
    throw new UnsupportedOperationException("ViewVarCharHolder set operation not supported");
  }

  /**
   * Same as {@link #set(int, ViewVarCharHolder)} except that it handles the
   * case where index and length of a new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, ViewVarCharHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40937
    throw new UnsupportedOperationException("ViewVarCharHolder setSafe operation not supported");
  }

  /**
   * Set the variable length element at the specified index to the data
   * buffer supplied in the holder.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void set(int index, NullableViewVarCharHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40937
    throw new UnsupportedOperationException("NullableViewVarCharHolder set operation not supported");
  }

  /**
   * Same as {@link #set(int, NullableViewVarCharHolder)} except that it handles the
   * case where index and length of a new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set
   * @param holder  holder that carries data buffer.
   */
  public void setSafe(int index, NullableViewVarCharHolder holder) {
    // TODO: https://github.com/apache/arrow/issues/40937
    throw new UnsupportedOperationException("NullableViewVarCharHolder setSafe operation not supported");
  }

  /**
   * Set the variable length element at the specified index to the
   * content in supplied Text.
   *
   * @param index   position of the element to set
   * @param text    Text object with data
   */
  public void set(int index, Text text) {
    set(index, text.getBytes(), 0, (int) text.getLength());
  }

  /**
   * Same as {@link #set(int, NullableViewVarCharHolder)} except that it handles the
   * case where index and length of a new element are beyond the existing
   * capacity of the vector.
   *
   * @param index   position of the element to set.
   * @param text    Text object with data
   */
  public void setSafe(int index, Text text) {
    setSafe(index, text.getBytes(), 0, (int) text.getLength());
  }

  @Override
  public void validateScalars() {
    for (int i = 0; i < getValueCount(); ++i) {
      byte[] value = get(i);
      if (value != null) {
        ValidateUtil.validateOrThrow(Text.validateUTF8NoThrow(value),
            "Non-UTF-8 data in VarCharVector at position " + i + ".");
      }
    }
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
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    // TODO: https://github.com/apache/arrow/issues/40932
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support getTransferPair(String, BufferAllocator)");
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param field The field materialized by this vector.
   * @param allocator allocator for the target vector
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    // TODO: https://github.com/apache/arrow/issues/40932
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support getTransferPair(Field, BufferAllocator)");
  }

  /**
   * Construct a TransferPair with a desired target vector of the same type.
   *
   * @param target the target for the transfer
   * @return {@link TransferPair} (UnsupportedOperationException)
   */
  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    // TODO: https://github.com/apache/arrow/issues/40932
    throw new UnsupportedOperationException(
        "ViewVarCharVector does not support makeTransferPair(ValueVector)");
  }
}
