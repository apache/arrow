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
      throw new RuntimeException(e);
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
      final Field f = this.getDeclaredFieldBaseOnJDKVersion(classBits, "reservedMemory");
      f.setAccessible(true);
      return ((AtomicLong) f.get(null)).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * To evaluate method on a class base on JDK version.
   * @param classBits Object associated with the class with the given string name
   * @param name Method needed to evaluate
   * @return
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

  /**
   * To evaluate field on a class base on JDK version.
   * @param classBits Object associated with the class with the given string name
   * @param name Field needed to evaluate
   * @return
   */
  private Field getDeclaredFieldBaseOnJDKVersion(Class<?> classBits, String name) {
    try {
      return classBits.getDeclaredField(name);
    } catch (NoSuchFieldException e) {
      try {
        return classBits.getDeclaredField("RESERVED_MEMORY");
      } catch (NoSuchFieldException ex) {
        throw new AssertionError(ex);
      }
    }
  }
}
