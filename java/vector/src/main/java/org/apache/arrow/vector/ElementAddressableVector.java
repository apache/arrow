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

import org.apache.arrow.memory.util.ArrowBufPointer;

/**
 * Vector for which each data element resides in a continuous memory region,
 * so it can be pointed to by an {@link org.apache.arrow.memory.util.ArrowBufPointer}.
 */
public interface ElementAddressableVector extends ValueVector {

  /**
   * Gets the pointer for the data at the given index.
   * @param index the index for the data.
   * @return the pointer to the data.
   */
  ArrowBufPointer getDataPointer(int index);

  /**
   * Gets the pointer for the data at the given index.
   * @param index the index for the data.
   * @param reuse the data pointer to fill, this avoids creating a new pointer object.
   * @return the pointer to the data, it should be the same one as the input parameter
   */
  ArrowBufPointer getDataPointer(int index, ArrowBufPointer reuse);
}
