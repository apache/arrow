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

package org.apache.arrow.dataset.jni;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.util.VisibleForTesting;

/**
 * Reserving Java direct memory bytes from java.nio.Bits. Used by Java Dataset API's C++ memory
 * pool implementation. This makes memory allocated by the pool to be controlled by JVM option
 * "-XX:MaxDirectMemorySize".
 */
public class DirectReservationListener implements ReservationListener {
  private final Method methodReserve;
  private final Method methodUnreserve;

  private DirectReservationListener() {
    try {
      final Class<?> classBits = Class.forName("java.nio.Bits");
      methodReserve = this.getDeclaredMethodBaseOnJDKVersion(classBits, "reserveMemory");
      methodReserve.setAccessible(true);
      methodUnreserve = this.getDeclaredMethodBaseOnJDKVersion(classBits, "unreserveMemory");
      methodUnreserve.setAccessible(true);
    } catch (Exception e) {
      final RuntimeException failure = new RuntimeException(
          "Failed to initialize DirectReservationListener. When starting Java you must include " +
              "`--add-opens=java.base/java.nio=org.apache.arrow.dataset,org.apache.arrow.memory.core,ALL-UNNAMED` " +
              "(See https://arrow.apache.org/docs/java/install.html)", e);
      failure.printStackTrace();
      throw failure;
    }
  }

  private static final DirectReservationListener INSTANCE = new DirectReservationListener();

  public static DirectReservationListener instance() {
    return INSTANCE;
  }

  /**
   * Reserve bytes by invoking java.nio.java.Bitjava.nio.Bitss#reserveMemory.
   */
  @Override
  public void reserve(long size) {
    try {
      if (size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("reserve size should not be larger than Integer.MAX_VALUE (0x7fffffff)");
      }
      methodReserve.invoke(null, (int) size, (int) size);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Unreserve bytes by invoking java.nio.java.Bitjava.nio.Bitss#unreserveMemory.
   */
  @Override
  public void unreserve(long size) {
    try {
      if (size > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("unreserve size should not be larger than Integer.MAX_VALUE (0x7fffffff)");
      }
      methodUnreserve.invoke(null, (int) size, (int) size);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get current reservation of jVM direct memory. Visible for testing.
   */
  @VisibleForTesting
  public long getCurrentDirectMemReservation() {
    try {
      final Class<?> classBits = Class.forName("java.nio.Bits");
      Field f;
      try {
        f = classBits.getDeclaredField("reservedMemory");
      } catch (NoSuchFieldException e) {
        try {
          f = classBits.getDeclaredField("RESERVED_MEMORY");
        } catch (NoSuchFieldException ex) {
          throw new AssertionError(ex);
        }
      }
      f.setAccessible(true);
      return ((AtomicLong) f.get(null)).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the given method via reflection, searching for different signatures based on the Java version.
   * @param classBits The java.nio.Bits class.
   * @param name The method being requested.
   * @return The method object.
   */
  private Method getDeclaredMethodBaseOnJDKVersion(Class<?> classBits, String name) {
    try {
      return classBits.getDeclaredMethod(name, long.class, int.class);
    } catch (NoSuchMethodException e) {
      try {
        return classBits.getDeclaredMethod(name, long.class, long.class);
      } catch (NoSuchMethodException ex) {
        throw new AssertionError(ex);
      }
    }
  }
}
