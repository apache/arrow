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

import java.util.List;

import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;

import io.netty.buffer.ArrowBuf;

/**
 * A vector corresponding to a Field in the schema
 * It has inner vectors backed by buffers (validity, offsets, data, ...)
 */
public interface FieldVector extends ValueVector {

  /**
   * Initializes the child vectors
   * to be later loaded with loadBuffers
   *
   * @param children the schema
   */
  void initializeChildrenFromFields(List<Field> children);

  /**
   * the returned list is the same size as the list passed to initializeChildrenFromFields
   *
   * @return the children according to schema (empty for primitive types)
   */
  List<FieldVector> getChildrenFromFields();

  /**
   * loads data in the vectors
   * (ownBuffers must be the same size as getFieldVectors())
   *
   * @param fieldNode  the fieldNode
   * @param ownBuffers the buffers for this Field (own buffers only, children not included)
   */
  void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers);

  /**
   * (same size as getFieldVectors() since it is their content)
   *
   * @return the buffers containing the data for this vector (ready for reading)
   */
  List<ArrowBuf> getFieldBuffers();

  /**
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  List<BufferBacked> getFieldInnerVectors();
}
