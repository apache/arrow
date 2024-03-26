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

import java.nio.ByteBuffer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReusableBuffer;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.arrow.vector.VectorDefinitionSetter;

public abstract class AbstractVariableWidthVector extends BaseValueVector
    implements VariableWidthVector, FieldVector, VectorDefinitionSetter {

  protected AbstractVariableWidthVector(BufferAllocator allocator) {
    super(allocator);
  }

  /**
   * Set the variable length element at the specified index to the supplied byte array.
   * @param index position of the element to set
   * @param value array of bytes with data
  */
  public abstract void set(int index, byte[] value);

  /**
   * Set the variable length element at the specified index to the supplied byte array.
   * @param index position of the element to set
   * @param value array of bytes with data
   * @param start start position in the array
   * @param length length of the data to write
   */
  public abstract void set(int index, byte[] value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied ByteBuffer.
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start position in the ByteBuffer
   * @param length length of the data to write
   */
  public abstract void set(int index, ByteBuffer value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied byte array, and it
   * handles the case where index and length of new element are beyond the existing capacity of the
   * vector.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  public abstract void setSafe(int index, byte[] value);

  /**
   * Set the variable length element at the specified index to the supplied byte array, and it
   * handles the case where index and length of new element are beyond the existing capacity.
   * @param index position of the element to set
   * @param value array of bytes with data
   * @param start start position in the array
   * @param length length of the data to write
   */
  public abstract void setSafe(int index, byte[] value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied ByteBuffer, and it
   * handles the case where index and length of new element are beyond the existing capacity.
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start position in the ByteBuffer
   * @param length length of the data to write
   */
  public abstract void setSafe(int index, ByteBuffer value, int start, int length);

  /**
   * Get the variable length element at the specified index.
   * @param index position of the element to get
   * @return byte array with the data
  */
  public abstract byte[] get(int index);

  /**
  * Get the variable length element at the specified index using a ReusableBuffer.
   * @param index position of the element to get
   * @param buffer ReusableBuffer to write the data to
  */
  public abstract void read(int index, ReusableBuffer<?> buffer);

  /**
   * Get the index of last non-null element in the vector.
   *
   * @return index of the last non-null element
   */
  public abstract int getLastSet();

  /**
   * Set the index of last non-null element in the vector.
   * @param value desired index of last non-null element
   */
  public abstract void setLastSet(int value);

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index   position of element to get
   * @return greater than 0 length for non-null element, 0 otherwise
   */
  public abstract int getValueLength(int index);

  /**
   * Create holes in the vector upto the given index (exclusive).
   * Holes will be created from the current last set position in
   * the vector.
   *
   * @param index target index
   */
  public abstract void fillEmpties(int index);

  /**
   * Sets the value length for an element.
   *
   * @param index   position of the element to set
   * @param length  length of the element
   */
  public abstract void setValueLengthSafe(int index, int length);
}
