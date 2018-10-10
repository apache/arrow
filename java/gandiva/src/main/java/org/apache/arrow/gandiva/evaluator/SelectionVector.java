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

package org.apache.arrow.gandiva.evaluator;

import org.apache.arrow.gandiva.ipc.GandivaTypes.SelectionVectorType;

import io.netty.buffer.ArrowBuf;

/**
 * A selection vector contains the indexes of "selected" records in a row batch. It is backed by an
 * arrow buffer.
 * Client manages the lifecycle of the arrow buffer - to release the reference.
 */
public abstract class SelectionVector {
  private int recordCount;
  private ArrowBuf buffer;

  public SelectionVector(ArrowBuf buffer) {
    this.buffer = buffer;
  }

  public final ArrowBuf getBuffer() {
    return this.buffer;
  }

  /*
   * The maximum number of records that the selection vector can hold.
   */
  public final int getMaxRecords() {
    return buffer.capacity() / getRecordSize();
  }

  /*
   * The number of records held by the selection vector.
   */
  public final int getRecordCount() {
    return this.recordCount;
  }

  /*
   * Set the number of records in the selection vector.
   */
  final void setRecordCount(int recordCount) {
    if (recordCount * getRecordSize() > buffer.capacity()) {
      throw new IllegalArgumentException("recordCount " + recordCount +
          " of size " + getRecordSize() +
          " exceeds buffer capacity " + buffer.capacity());
    }

    this.recordCount = recordCount;
  }

  /*
   * Get the value at specified index.
   */
  public abstract int getIndex(int index);

  /*
   * Get the record size of the selection vector itself.
   */
  abstract int getRecordSize();

  abstract SelectionVectorType getType();

  final void checkReadBounds(int index) {
    if (index >= this.recordCount) {
      throw new IllegalArgumentException("index " + index + " is >= recordCount " + recordCount);
    }
  }

}
