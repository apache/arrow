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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;

/**
 * A zero length vector of any type.
 */
public final class ZeroVector extends NullVector {
  public static final ZeroVector INSTANCE = new ZeroVector();

  /**
   * Instantiate a ZeroVector.
   *
   * @param name name of the vector
   */
  public ZeroVector(String name) {
    super(name);
  }

  /**
   * Instantiate a ZeroVector.
   *
   * @param name      name of the vector
   * @param fieldType type of Field materialized by this vector.
   */
  public ZeroVector(String name, FieldType fieldType) {
    super(name, fieldType);
  }

  /**
   * Instantiate a ZeroVector.
   *
   * @param field field materialized by this vector.
   */
  public ZeroVector(Field field) {
    super(field);
  }

  @Deprecated
  public ZeroVector() {
  }

  @Override
  public int getValueCount() {
    return 0;
  }

  @Override
  public void setValueCount(int valueCount) {
  }

  @Override
  public int getNullCount() {
    return 0;
  }

  @Override
  public boolean isNull(int index) {
    throw new IndexOutOfBoundsException();
  }

  @Override
  public int hashCode(int index) {
    return 0;
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    return ArrowBufPointer.NULL_HASH_CODE;
  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return defaultPair;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return defaultPair;
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return defaultPair;
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return defaultPair;
  }

  private final TransferPair defaultPair = new TransferPair() {
    @Override
    public void transfer() {
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
    }

    @Override
    public ValueVector getTo() {
      return ZeroVector.this;
    }

    @Override
    public void copyValueSafe(int from, int to) {
    }
  };
}
