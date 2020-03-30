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

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

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
    try {
      // try to get the unsafe object
      final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          try {
            final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            Throwable cause = trySetAccessible(unsafeField, false);
            if (cause != null) {
              return cause;
            }
            return unsafeField.get(null);
          } catch (Throwable e) {
            return e;
          }
        }
      });

      if (maybeUnsafe instanceof Throwable) {
        throw (Throwable) maybeUnsafe;
      }

      UNSAFE = (Unsafe) maybeUnsafe;

      // get the offset of the data inside a byte array object
      BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

      // get the offset of the address field in a java.nio.Buffer object
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      BYTE_BUFFER_ADDRESS_OFFSET = UNSAFE.objectFieldOffset(addressField);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to initialize MemoryUtil.", e);
    }
  }

  /**
   * Given a {@link ByteBuffer}, gets the address the underlying memory space.
   *
   * @param buf the byte buffer.
   * @return address of the underlying memory.
   */
  public static long getByteBufferAddress(ByteBuffer buf) {
    return UNSAFE.getLong(buf, BYTE_BUFFER_ADDRESS_OFFSET);
  }

  private MemoryUtil() {
  }

  /**
   * Try to call {@link AccessibleObject#setAccessible(boolean)} but will catch any {@link SecurityException} and
   * {@link java.lang.reflect.InaccessibleObjectException} and return it.
   * The caller must check if it returns {@code null} and if not handle the returned exception.
   */
  private static Throwable trySetAccessible(AccessibleObject object, boolean checkAccessible) {
    if (checkAccessible && javaVersion() > 9) {
      return new UnsupportedOperationException("Reflective setAccessible(true) disabled");
    }
    try {
      object.setAccessible(true);
      return null;
    } catch (SecurityException e) {
      return e;
    } catch (RuntimeException e) {
      if ("java.lang.reflect.InaccessibleObjectException".equals(e.getClass().getName())) {
        return e;
      }
      throw e;
    }
  }

  private static int javaVersion() {
    return majorVersion(System.getProperty("java.specification.version", "1.6"));
  }

  private static int majorVersion(final String javaSpecVersion) {
    final String[] components = javaSpecVersion.split("\\.");
    final int[] version = new int[components.length];
    for (int i = 0; i < components.length; i++) {
      version[i] = Integer.parseInt(components[i]);
    }

    if (version[0] == 1) {
      assert version[1] >= 6;
      return version[1];
    } else {
      return version[0];
    }
  }
}
