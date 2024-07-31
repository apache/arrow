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
import org.apache.arrow.memory.ReusableBuffer;

/** A base interface for common functionalities in variable width vectors. */
public interface VariableWidthFieldVector
    extends VariableWidthVector, FieldVector, VectorDefinitionSetter {

  /**
   * Set the variable length element at the specified index to the supplied byte array.
   *
   * @param index position of the element to set
   * @param value array of bytes with data
   */
  void set(int index, byte[] value);

  /**
   * Set the variable length element at the specified index to the supplied byte array.
   *
   * @param index position of the element to set
   * @param value array of bytes with data
   * @param start start position in the array
   * @param length length of the data to write
   */
  void set(int index, byte[] value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied ByteBuffer.
   *
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start position in the ByteBuffer
   * @param length length of the data to write
   */
  void set(int index, ByteBuffer value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied byte array, and it
   * handles the case where index and length of a new element are beyond the existing capacity of
   * the vector.
   *
   * @param index position of the element to set
   * @param value array of bytes to write
   */
  void setSafe(int index, byte[] value);

  /**
   * Set the variable length element at the specified index to the supplied byte array, and it
   * handles the case where index and length of a new element are beyond the existing capacity.
   *
   * @param index position of the element to set
   * @param value array of bytes with data
   * @param start start position in the array
   * @param length length of the data to write
   */
  void setSafe(int index, byte[] value, int start, int length);

  /**
   * Set the variable length element at the specified index to the supplied ByteBuffer, and it
   * handles the case where index and length of a new element are beyond the existing capacity.
   *
   * @param index position of the element to set
   * @param value ByteBuffer with data
   * @param start start position in the ByteBuffer
   * @param length length of the data to write
   */
  void setSafe(int index, ByteBuffer value, int start, int length);

  /**
   * Get the variable length element at the specified index.
   *
   * @param index position of the element to get
   * @return byte array with the data
   */
  byte[] get(int index);

  /**
   * Get the variable length element at the specified index using a ReusableBuffer.
   *
   * @param index position of the element to get
   * @param buffer ReusableBuffer to write the data to
   */
  void read(int index, ReusableBuffer<?> buffer);

  /**
   * Get the index of the last non-null element in the vector.
   *
   * @return index of the last non-null element
   */
  int getLastSet();

  /**
   * Set the index of the last non-null element in the vector.
   *
   * @param value desired index of last non-null element
   */
  void setLastSet(int value);

  /**
   * Get the variable length element at specified index as Text.
   *
   * @param index position of an element to get
   * @return greater than length 0 for a non-null element, 0 otherwise
   */
  int getValueLength(int index);

  /**
   * Create holes in the vector upto the given index (exclusive). Holes will be created from the
   * current last-set position in the vector.
   *
   * @param index target index
   */
  void fillEmpties(int index);

  /**
   * Sets the value length for an element.
   *
   * @param index position of the element to set
   * @param length length of the element
   */
  void setValueLengthSafe(int index, int length);
}
