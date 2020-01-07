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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import sun.misc.Unsafe;

/**
 * Utilities for memory related operations.
 */
public class MemoryUtil {

  /**
   * The unsafe object from which to access the off-heap memory.
   */
  public static final Unsafe UNSAFE;

  /**
   * The start offset of array data relative to the start address of the array object.
   */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  /**
   * The offset of the address field with the {@link java.nio.ByteBuffer} object.
   */
  static final long BYTE_BUFFER_ADDRESS_OFFSET;

  static {
    // get the unsafe object
    try {
      Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      UNSAFE = (Unsafe) unsafeField.get(null);
    } catch (Throwable e) {
      throw new Error("Failed to get the unsafe object.", e);
    }

    BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    try {
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      BYTE_BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(addressField);
    } catch (NoSuchFieldException e) {
      throw new Error("Failed to get java.nio.Buffer#address field.", e);
    }
  }

  /**
   * Given a {@link ByteBuf}, gets the address the underlying memory space.
   *
   * @param buf the byte buffer.
   * @return address of the underlying memory.
   */
  public static long getByteBufferAddress(ByteBuffer buf) {
    return UNSAFE.getLong(buf, BYTE_BUFFER_ADDRESS_OFFSET);
  }

  private MemoryUtil() {
  }
}
