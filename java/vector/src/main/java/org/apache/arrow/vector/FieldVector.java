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

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * A vector corresponding to a Field in the schema.
 * It has inner vectors backed by buffers (validity, offsets, data, ...)
 */
public interface FieldVector extends ValueVector {

  /**
   * Initializes the child vectors
   * to be later loaded with loadBuffers.
   *
   * @param children the schema
   */
  void initializeChildrenFromFields(List<Field> children);

  /**
   * The returned list is the same size as the list passed to initializeChildrenFromFields.
   *
   * @return the children according to schema (empty for primitive types)
   */
  List<FieldVector> getChildrenFromFields();

  /**
   * Loads data in the vectors.
   * (ownBuffers must be the same size as getFieldVectors())
   *
   * @param fieldNode  the fieldNode
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers);

  /**
   * Get the buffers of the fields, (same size as getFieldVectors() since it is their content).
   *
   * @return the buffers containing the data for this vector (ready for reading)
   */
  List<ArrowBuf> getFieldBuffers();

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  @Deprecated
  List<BufferBacked> getFieldInnerVectors();

  /**
   * Gets the starting address of the underlying buffer associated with validity vector.
   *
   * @return buffer address
   */
  long getValidityBufferAddress();

  /**
   * Gets the starting address of the underlying buffer associated with data vector.
   *
   * @return buffer address
   */
  long getDataBufferAddress();

  /**
   * Gets the starting address of the underlying buffer associated with offset vector.
   *
   * @return buffer address
   */
  long getOffsetBufferAddress();

  /**
   * Set the element at the given index to null.
   *
   * @param index the value to change
   */
  void setNull(int index);
}
