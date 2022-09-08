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

package org.apache.arrow.c;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Utility functions for working with native memory.
 */
public final class NativeUtil {
  public static final byte NULL = 0;
  static final int MAX_STRING_LENGTH = Short.MAX_VALUE;

  private NativeUtil() {
  }

  /**
   * Convert a pointer to a null terminated string into a Java String.
   * 
   * @param cstringPtr pointer to C string
   * @return Converted string
   */
  public static String toJavaString(long cstringPtr) {
    if (cstringPtr == NULL) {
      return null;
    }
    ByteBuffer reader = MemoryUtil.directBuffer(cstringPtr, MAX_STRING_LENGTH).order(ByteOrder.nativeOrder());

    int length = 0;
    while (reader.get() != NULL) {
      length++;
    }
    byte[] bytes = new byte[length];
    // Force use of base class rewind() to avoid breaking change of ByteBuffer.rewind in JDK9+
    ((ByteBuffer) ((Buffer) reader).rewind()).get(bytes);
    return new String(bytes, 0, length, StandardCharsets.UTF_8);
  }

  /**
   * Convert a native array pointer (void**) to Java array of pointers.
   * 
   * @param arrayPtr Array pointer
   * @param size     Array size
   * @return Array of pointer values as longs
   */
  public static long[] toJavaArray(long arrayPtr, int size) {
    if (arrayPtr == NULL) {
      return null;
    }
    if (size < 0) {
      throw new IllegalArgumentException("Invalid native array size");
    }

    long[] result = new long[size];
    ByteBuffer reader = MemoryUtil.directBuffer(arrayPtr, Long.BYTES * size).order(ByteOrder.nativeOrder());
    for (int i = 0; i < size; i++) {
      result[i] = reader.getLong();
    }
    return result;
  }

  /**
   * Convert Java string to a null terminated string.
   * 
   * @param allocator Buffer allocator for allocating the native string
   * @param string    Input String to convert
   * @return Buffer with a null terminated string or null if the input is null
   */
  public static ArrowBuf toNativeString(BufferAllocator allocator, String string) {
    if (string == null) {
      return null;
    }

    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
    ArrowBuf buffer = allocator.buffer(bytes.length + 1);
    buffer.writeBytes(bytes);
    buffer.writeByte(NULL);
    return buffer;
  }

  /**
   * Close a buffer if it's not null.
   * 
   * @param buf Buffer to close
   */
  public static void closeBuffer(ArrowBuf buf) {
    if (buf != null) {
      buf.close();
    }
  }

  /**
   * Get the address of a buffer or {@value #NULL} if the input buffer is null.
   * 
   * @param buf Buffer to get the address of
   * @return Memory addresss or {@value #NULL}
   */
  public static long addressOrNull(ArrowBuf buf) {
    if (buf == null) {
      return NULL;
    }
    return buf.memoryAddress();
  }

  /**
   * Get the address of a C Data Interface struct or {@value #NULL} if the input
   * struct is null.
   * 
   * @param struct C Data Interface struct to get the address of
   * @return Memory addresss or {@value #NULL}
   */
  public static long addressOrNull(BaseStruct struct) {
    if (struct == null) {
      return NULL;
    }
    return struct.memoryAddress();
  }

}
