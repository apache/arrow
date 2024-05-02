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

import static java.lang.invoke.MethodType.*;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utilities for memory related operations. */
public class MemoryUtil {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MemoryUtil.class);

  private static final @Nullable Constructor<?> DIRECT_BUFFER_CONSTRUCTOR;

  /** The start offset of array data relative to the start address of the array object. */
  private static final long BYTE_ARRAY_BASE_OFFSET;

  /** The offset of the address field with the {@link java.nio.ByteBuffer} object. */
  private static final long BYTE_BUFFER_ADDRESS_OFFSET;

  /** If the native byte order is little-endian. */
  public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  // Java 1.8, 9, 11, 17, 21 becomes 1, 9, 11, 17, and 21.
  @SuppressWarnings("StringSplitter")
  private static final int majorVersion =
      Integer.parseInt(System.getProperty("java.specification.version").split("\\D+")[0]);

  private static final MethodHandle COPY_MEMORY_MH;
  private static final MethodHandle COPY_FROMTO_MEMORY_MH;
  private static final MethodHandle GET_BYTE_MH;
  private static final MethodHandle PUT_BYTE_MH;
  private static final MethodHandle GET_SHORT_MH;
  private static final MethodHandle PUT_SHORT_MH;
  private static final MethodHandle GET_INT_MH;
  private static final MethodHandle PUT_INT_MH;
  private static final MethodHandle GET_LONG_MH;
  private static final MethodHandle PUT_LONG_MH;
  private static final MethodHandle SET_MEMORY_MH;
  private static final MethodHandle GET_INT_FROM_BYTE_ARRAY_MH;
  private static final MethodHandle GET_LONG_FROM_BYTE_ARRAY_MH;
  private static final MethodHandle ALLOCATE_MEMORY_MH;
  private static final MethodHandle FREE_MEMORY_MH;
  private static final MethodHandle GET_LONG_FROM_BYTE_BUFFER_MH;

  static {
    try {
      // try to get the unsafe object
      final Class<?> unsafeClazz = Class.forName("sun.misc.Unsafe");
      final Object unsafe =
          AccessController.doPrivileged(
              new PrivilegedAction<Object>() {
                @Override
                @SuppressWarnings({"nullness:argument", "nullness:return"})
                // incompatible argument for parameter obj of Field.get
                // incompatible types in return
                public Object run() {
                  try {
                    final Field unsafeField = unsafeClazz.getDeclaredField("theUnsafe");
                    unsafeField.setAccessible(true);
                    return unsafeField.get(null);
                  } catch (Throwable e) {
                    return e;
                  }
                }
              });

      if (unsafe instanceof Throwable) {
        throw (Throwable) unsafe;
      }

      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      COPY_MEMORY_MH = copyMemoryMethod(lookup, unsafeClazz, unsafe);
      COPY_FROMTO_MEMORY_MH = copyFromToMemoryMethod(lookup, unsafeClazz, unsafe);
      GET_BYTE_MH = getByteMethod(lookup, unsafeClazz, unsafe);
      PUT_BYTE_MH = putByteMethod(lookup, unsafeClazz, unsafe);
      GET_SHORT_MH = getShortMethod(lookup, unsafeClazz, unsafe);
      PUT_SHORT_MH = putShortMethod(lookup, unsafeClazz, unsafe);
      GET_INT_MH = getIntMethod(lookup, unsafeClazz, unsafe);
      PUT_INT_MH = putIntMethod(lookup, unsafeClazz, unsafe);
      GET_LONG_MH = getLongMethod(lookup, unsafeClazz, unsafe);
      PUT_LONG_MH = putLongMethod(lookup, unsafeClazz, unsafe);
      SET_MEMORY_MH = setMemoryMethod(lookup, unsafeClazz, unsafe);
      GET_INT_FROM_BYTE_ARRAY_MH = getIntFromByteArrayMethod(lookup, unsafeClazz, unsafe);
      GET_LONG_FROM_BYTE_ARRAY_MH = getLongFromByteArrayMethod(lookup, unsafeClazz, unsafe);
      ALLOCATE_MEMORY_MH = allocateMemoryMethod(lookup, unsafeClazz, unsafe);
      FREE_MEMORY_MH = freeMemoryMethod(lookup, unsafeClazz, unsafe);
      GET_LONG_FROM_BYTE_BUFFER_MH = getLongFromByteBufferMethod(lookup, unsafeClazz, unsafe);

      // get the offset of the data inside a byte array object
      BYTE_ARRAY_BASE_OFFSET =
          (long)
              lookup
                  .findVirtual(
                      unsafeClazz, "arrayBaseOffset", methodType(Integer.TYPE, Class.class))
                  .invoke(unsafe, byte[].class);

      // get the offset of the address field in a java.nio.Buffer object
      Field addressField = java.nio.Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      BYTE_BUFFER_ADDRESS_OFFSET =
          (long)
              lookup
                  .findVirtual(unsafeClazz, "objectFieldOffset", methodType(Long.TYPE, Field.class))
                  .invoke(unsafe, addressField);

      Constructor<?> directBufferConstructor;
      long address = -1;
      final ByteBuffer direct = ByteBuffer.allocateDirect(1);
      try {

        final Object maybeDirectBufferConstructor =
            AccessController.doPrivileged(
                new PrivilegedAction<Object>() {
                  @Override
                  public Object run() {
                    try {
                      final Constructor<?> constructor =
                          (majorVersion >= 21)
                              ? direct.getClass().getDeclaredConstructor(long.class, long.class)
                              : direct.getClass().getDeclaredConstructor(long.class, int.class);
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
          address = allocateMemory(1);
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
              "direct buffer constructor: unavailable", (Throwable) maybeDirectBufferConstructor);
          directBufferConstructor = null;
        }
      } finally {
        if (address != -1) {
          freeMemory(address);
        }
      }
      DIRECT_BUFFER_CONSTRUCTOR = directBufferConstructor;
    } catch (Throwable e) {
      // This exception will get swallowed, but it's necessary for the static analysis that ensures
      // the static fields above get initialized
      final RuntimeException failure =
          new RuntimeException(
              "Failed to initialize MemoryUtil. You must start Java with "
                  + "`--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` "
                  + "(See https://arrow.apache.org/docs/java/install.html)",
              e);
      failure.printStackTrace();
      throw failure;
    }
  }

  private static MethodHandle copyMemoryMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(
            unsafeClazz, "copyMemory", methodType(Void.TYPE, Long.TYPE, Long.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle copyFromToMemoryMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    final MethodHandle copyMemoryMH =
        lookup
            .findVirtual(
                unsafeClazz,
                "copyMemory",
                methodType(Void.TYPE, Object.class, Long.TYPE, Object.class, Long.TYPE, Long.TYPE))
            .bindTo(unsafe);
    return MethodHandles.explicitCastArguments(
        copyMemoryMH,
        methodType(Void.TYPE, byte[].class, Long.TYPE, byte[].class, Long.TYPE, Long.TYPE));
  }

  private static MethodHandle getByteMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "getByte", methodType(Byte.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle putByteMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "putByte", methodType(Void.TYPE, Long.TYPE, Byte.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle getShortMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "getShort", methodType(Short.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle putShortMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "putShort", methodType(Void.TYPE, Long.TYPE, Short.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle getIntMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "getInt", methodType(Integer.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle putIntMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "putInt", methodType(Void.TYPE, Long.TYPE, Integer.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle getLongMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "getLong", methodType(Long.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle putLongMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "putLong", methodType(Void.TYPE, Long.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle setMemoryMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(
            unsafeClazz, "setMemory", methodType(Void.TYPE, Long.TYPE, Long.TYPE, Byte.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle getIntFromByteArrayMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    final MethodHandle getIntMH =
        lookup
            .findVirtual(unsafeClazz, "getInt", methodType(Integer.TYPE, Object.class, Long.TYPE))
            .bindTo(unsafe);
    return MethodHandles.explicitCastArguments(
        getIntMH, methodType(Integer.TYPE, byte[].class, Long.TYPE));
  }

  private static MethodHandle getLongFromByteArrayMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    final MethodHandle getLongMH =
        lookup
            .findVirtual(unsafeClazz, "getLong", methodType(Long.TYPE, Object.class, Long.TYPE))
            .bindTo(unsafe);
    return MethodHandles.explicitCastArguments(
        getLongMH, methodType(Long.TYPE, byte[].class, Long.TYPE));
  }

  private static MethodHandle getLongFromByteBufferMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    final MethodHandle getLongMH =
        lookup
            .findVirtual(unsafeClazz, "getLong", methodType(Long.TYPE, Object.class, Long.TYPE))
            .bindTo(unsafe);
    return MethodHandles.explicitCastArguments(
        getLongMH, methodType(Long.TYPE, ByteBuffer.class, Long.TYPE));
  }

  private static MethodHandle allocateMemoryMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "allocateMemory", methodType(Long.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  private static MethodHandle freeMemoryMethod(
      MethodHandles.Lookup lookup, Class<?> unsafeClazz, Object unsafe)
      throws NoSuchMethodException, IllegalAccessException {
    return lookup
        .findVirtual(unsafeClazz, "freeMemory", methodType(Void.TYPE, Long.TYPE))
        .bindTo(unsafe);
  }

  /**
   * Given a {@link ByteBuffer}, gets the address the underlying memory space.
   *
   * @param buf the byte buffer.
   * @return address of the underlying memory.
   */
  public static long getByteBufferAddress(ByteBuffer buf) {
    try {
      return (long) GET_LONG_FROM_BYTE_BUFFER_MH.invokeExact(buf, BYTE_BUFFER_ADDRESS_OFFSET);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  private MemoryUtil() {}

  /** Create nio byte buffer. */
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

  @SuppressWarnings(
      "nullness:argument") // to handle null assignment on third party dependency: Unsafe
  private static void copyMemory(
      @Nullable byte[] srcBase,
      long srcOffset,
      @Nullable byte[] destBase,
      long destOffset,
      long bytes) {
    try {
      COPY_FROMTO_MEMORY_MH.invokeExact(srcBase, srcOffset, destBase, destOffset, bytes);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Copy a given number of bytes between two memory locations. */
  public static void copyMemory(long srcAddress, long destAddress, long bytes) {
    try {
      COPY_MEMORY_MH.invokeExact(srcAddress, destAddress, bytes);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  public static void copyToMemory(byte[] src, long srcIndex, long destAddress, long bytes) {
    copyMemory(src, BYTE_ARRAY_BASE_OFFSET + srcIndex, null, destAddress, bytes);
  }

  public static void copyFromMemory(long srcAddress, byte[] dest, long destIndex, long bytes) {
    copyMemory(null, srcAddress, dest, BYTE_ARRAY_BASE_OFFSET + destIndex, bytes);
  }

  /** Gets a byte at the provided address. */
  public static byte getByte(long address) {
    try {
      return (byte) GET_BYTE_MH.invokeExact(address);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Puts a byte at the provided address. */
  public static void putByte(long address, byte value) {
    try {
      PUT_BYTE_MH.invokeExact(address, value);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Gets a short at the provided address. */
  public static short getShort(long address) {
    try {
      return (short) GET_SHORT_MH.invokeExact(address);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Puts a short at the provided address. */
  public static void putShort(long address, short value) {
    try {
      PUT_SHORT_MH.invokeExact(address, value);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Gets a int at the provided address. */
  public static int getInt(long address) {
    try {
      return (int) GET_INT_MH.invokeExact(address);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Puts a int at the provided address. */
  public static void putInt(long address, int value) {
    try {
      PUT_INT_MH.invokeExact(address, value);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Gets a long at the provided address. */
  public static long getLong(long address) {
    try {
      return (long) GET_LONG_MH.invokeExact(address);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Puts a long at the provided address. */
  public static void putLong(long address, long value) {
    try {
      PUT_LONG_MH.invokeExact(address, value);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Sets the provided value to the memory address. */
  public static void setMemory(long address, long bytes, byte value) {
    try {
      SET_MEMORY_MH.invokeExact(address, bytes, value);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Reads a int from a byte array. */
  public static int getInt(byte[] bytes, int index) {
    try {
      return (int) GET_INT_FROM_BYTE_ARRAY_MH.invokeExact(bytes, BYTE_ARRAY_BASE_OFFSET + index);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Reads a long from a byte array. */
  public static long getLong(byte[] bytes, int index) {
    try {
      return (long) GET_LONG_FROM_BYTE_ARRAY_MH.invokeExact(bytes, BYTE_ARRAY_BASE_OFFSET + index);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Allocates memory. */
  public static long allocateMemory(long bytes) {
    try {
      return (long) ALLOCATE_MEMORY_MH.invokeExact(bytes);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }

  /** Frees memory. */
  public static void freeMemory(long address) {
    try {
      FREE_MEMORY_MH.invokeExact(address);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable t) {
      throw new UndeclaredThrowableException(t);
    }
  }
}
