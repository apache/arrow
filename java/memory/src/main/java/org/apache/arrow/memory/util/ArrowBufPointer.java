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

package org.apache.arrow.memory.util;

import java.nio.ByteOrder;

import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * Pointer to a memory region within an {@link io.netty.buffer.ArrowBuf}.
 * It will be used as the basis for calculating hash code within a vector, and equality determination.
 */
public final class ArrowBufPointer {

  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  /**
   * The hash code when the arrow buffer is null.
   */
  public static final int NULL_HASH_CODE = 0;

  private ArrowBuf buf;

  private int offset;

  private int length;

  private int hashCode = NULL_HASH_CODE;

  private final ArrowBufHasher hasher;

  /**
   * A flag indicating if the underlying memory region has changed.
   */
  private boolean hashCodeChanged = false;

  /**
   * The default constructor.
   */
  public ArrowBufPointer() {
    this(SimpleHasher.INSTANCE);
  }

  /**
   * Constructs an arrow buffer pointer with the specified hasher.
   * @param hasher the hasher to use.
   */
  public ArrowBufPointer(ArrowBufHasher hasher) {
    Preconditions.checkNotNull(hasher);
    this.hasher = hasher;
  }

  /**
   * Constructs an Arrow buffer pointer.
   * @param buf the underlying {@link ArrowBuf}, which can be null.
   * @param offset the start off set of the memory region pointed to.
   * @param length the length off set of the memory region pointed to.
   */
  public ArrowBufPointer(ArrowBuf buf, int offset, int length) {
    this(buf, offset, length, SimpleHasher.INSTANCE);
  }

  /**
   * Constructs an Arrow buffer pointer.
   * @param buf the underlying {@link ArrowBuf}, which can be null.
   * @param offset the start off set of the memory region pointed to.
   * @param length the length off set of the memory region pointed to.
   * @param hasher the hasher used to calculate the hash code.
   */
  public ArrowBufPointer(ArrowBuf buf, int offset, int length, ArrowBufHasher hasher) {
    Preconditions.checkNotNull(hasher);
    this.hasher = hasher;
    set(buf, offset, length);
  }

  /**
   * Sets this pointer.
   * @param buf the underlying {@link ArrowBuf}, which can be null.
   * @param offset the start off set of the memory region pointed to.
   * @param length the length off set of the memory region pointed to.
   */
  public void set(ArrowBuf buf, int offset, int length) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;

    hashCodeChanged = true;
  }

  /**
   * Gets the underlying buffer, or null if the underlying data is invalid or null.
   * @return the underlying buffer, if any, or null if the underlying data is invalid or null.
   */
  public ArrowBuf getBuf() {
    return buf;
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    if (!hasher.equals(((ArrowBufPointer) o).hasher)) {
      // note that the hasher is incorporated in equality determination
      // this is to avoid problems in cases where two Arrow buffer pointers are not equal
      // while having different hashers and equal hash codes.
      return false;
    }

    ArrowBufPointer other = (ArrowBufPointer) o;
    if (buf == null || other.buf == null) {
      if (buf == null && other.buf == null) {
        return true;
      } else {
        return false;
      }
    }

    return ByteFunctionHelpers.equal(buf, offset, offset + length,
            other.buf, other.offset, other.offset + other.length) != 0;
  }

  @Override
  public int hashCode() {
    if (!hashCodeChanged) {
      return hashCode;
    }

    // re-compute the hash code
    if (buf == null) {
      hashCode = NULL_HASH_CODE;
    } else {
      hashCode = hasher.hashCode(buf, offset, length);
    }

    hashCodeChanged = false;
    return hashCode;
  }
}
