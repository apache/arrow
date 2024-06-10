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

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.checkerframework.checker.nullness.qual.Nullable;

import sun.misc.Unsafe;


/**
 * Utilities for memory related operations.
 */
public class MemoryUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryUtil.class);

  private static final @Nullable Constructor<?> DIRECT_BUFFER_CONSTRUCTOR;
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

  /**
   * If the native byte order is little-endian.
   */
  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  // Java 1.8, 9, 11, 17, 21 becomes 1, 9, 11, 17, and 21.
  @SuppressWarnings("StringSplitter")
  private static final int majorVersion =
      Integer.parseInt(System.getProperty("java.specification.version").split("\\D+")[0]);

  static {
    try {
      // try to get the unsafe object
      final Object maybeUnsafe = AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override
        @SuppressWarnings({"nullness:argument", "nullness:return"})
        // incompatible argument for parameter obj of Field.get
        // incompatible types in return
        public Object run() {
          try {
            final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
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

      Constructor<?> directBufferConstructor;
      long address = -1;
      final ByteBuffer direct = ByteBuffer.allocateDirect(1);
      try {

        final Object maybeDirectBufferConstructor =
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  final Constructor<?> constructor = (majorVersion >= 21) ?
                      direct.getClass().getDeclaredConstructor(long.class, long.class) :
                      direct.getClass().getDeclaredConstructor(long.class, int.class);
                  constructor.setAccessible(true);
                  logger.debug("Constructor for direct buffer found and made accessible");
                  return constructor;
                } catch (NoSuchMethodException e) {
                  logger.debug("Cannot get constructor for direct buffer allocation", e);
                  return e;
                } catch (SecurityException e) {
                  logger.debug("Cannot get constructor for direct buffer allocation", e);
                  return e;
                }
              }
            });

        if (maybeDirectBufferConstructor instanceof Constructor<?>) {
          address = UNSAFE.allocateMemory(1);
          // try to use the constructor now
          try {
            ((Constructor<?>) maybeDirectBufferConstructor).newInstance(address, 1);
            directBufferConstructor = (Constructor<?>) maybeDirectBufferConstructor;
            logger.debug("direct buffer constructor: available");
          } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logger.warn("unable to instantiate a direct buffer via constructor", e);
            directBufferConstructor = null;
          }
        } else {
          logger.debug(
              "direct buffer constructor: unavailable",
              (Throwable) maybeDirectBufferConstructor);
          directBufferConstructor = null;
        }
      } finally {
        if (address != -1) {
          UNSAFE.freeMemory(address);
        }
      }
      DIRECT_BUFFER_CONSTRUCTOR = directBufferConstructor;
    } catch (Throwable e) {
      // This exception will get swallowed, but it's necessary for the static analysis that ensures
      // the static fields above get initialized
      final RuntimeException failure = new RuntimeException(
          "Failed to initialize MemoryUtil. You must start Java with " +
              "`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` " +
              "(See https://arrow.apache.org/docs/java/install.html)", e);
      failure.printStackTrace();
      throw failure;
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
   * Create nio byte buffer.
   */
  public static ByteBuffer directBuffer(long address, int capacity) {
    if (DIRECT_BUFFER_CONSTRUCTOR != null) {
      if (capacity < 0) {
        throw new IllegalArgumentException("Capacity is negative, has to be positive or 0");
      }
      try {
        return (ByteBuffer) DIRECT_BUFFER_CONSTRUCTOR.newInstance(address, capacity);
      } catch (Throwable cause) {
        throw new Error(cause);
      }
    }
    throw new UnsupportedOperationException(
        "sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available");
  }

  @SuppressWarnings("nullness:argument") //to handle null assignment on third party dependency: Unsafe
  public static void copyMemory(@Nullable Object srcBase, long srcOffset,
                                @Nullable Object destBase, long destOffset,
                                long bytes) {
    UNSAFE.copyMemory(srcBase, srcOffset, destBase, destOffset, bytes);
  }
}
