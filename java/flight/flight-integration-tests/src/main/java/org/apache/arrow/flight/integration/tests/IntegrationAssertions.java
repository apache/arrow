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

package org.apache.arrow.flight.integration.tests;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Objects;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;

/**
 * Utility methods to implement integration tests without using JUnit assertions.
 */
final class IntegrationAssertions {

  /**
   * Assert that the given code throws the given exception or subclass thereof.
   *
   * @param clazz The exception type.
   * @param body The code to run.
   * @param <T> The exception type.
   * @return The thrown exception.
   */
  @SuppressWarnings("unchecked")
  static <T extends Throwable> T assertThrows(Class<T> clazz, AssertThrows body) {
    try {
      body.run();
    } catch (Throwable t) {
      if (clazz.isInstance(t)) {
        return (T) t;
      }
      throw new AssertionError("Expected exception of class " + clazz + " but got " + t.getClass(), t);
    }
    throw new AssertionError("Expected exception of class " + clazz + " but did not throw.");
  }

  /**
   * Assert that the two (non-array) objects are equal.
   */
  static void assertEquals(Object expected, Object actual) {
    if (!Objects.equals(expected, actual)) {
      throw new AssertionError("Expected:\n" + expected + "\nbut got:\n" + actual);
    }
  }

  /**
   * Assert that the two arrays are equal.
   */
  static void assertEquals(byte[] expected, byte[] actual) {
    if (!Arrays.equals(expected, actual)) {
      throw new AssertionError(
          String.format("Expected:\n%s\nbut got:\n%s", Arrays.toString(expected), Arrays.toString(actual)));
    }
  }

  /**
   * Assert that the value is false, using the given message as an error otherwise.
   */
  static void assertFalse(String message, boolean value) {
    if (value) {
      throw new AssertionError("Expected false: " + message);
    }
  }

  /**
   * Assert that the value is true, using the given message as an error otherwise.
   */
  static void assertTrue(String message, boolean value) {
    if (!value) {
      throw new AssertionError("Expected true: " + message);
    }
  }

  static void assertNotNull(Object actual) {
    if (actual == null) {
      throw new AssertionError("Expected: (not null)\n\nbut got: null\n");
    }
  }

  /**
   * Convert a throwable into a FlightRuntimeException with error details, for debugging.
   */
  static FlightRuntimeException toFlightRuntimeException(Throwable t) {
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter writer = new PrintWriter(stringWriter);
    t.printStackTrace(writer);
    return CallStatus.UNKNOWN
            .withCause(t)
            .withDescription("Unknown error: " + t + "\n. Stack trace:\n" + stringWriter.toString())
            .toRuntimeException();
  }

  /**
   * An interface used with {@link #assertThrows(Class, AssertThrows)}.
   */
  @FunctionalInterface
  interface AssertThrows {

    void run() throws Throwable;
  }
}
