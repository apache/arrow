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

package org.apache.arrow.driver.jdbc.utils;

/**
 * Utility class to avoid upgrading JUnit to version >= 4.13 and keep using code to assert a {@link Throwable}.
 * This should be removed as soon as we can use the proper assertThrows/checkThrows.
 */
public class ThrowableAssertionUtils {
  private ThrowableAssertionUtils() {
  }

  public static <T extends Throwable> void simpleAssertThrowableClass(
      final Class<? extends Throwable> expectedThrowable, final ThrowingRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable actualThrown) {
      if (expectedThrowable.isInstance(actualThrown)) {
        return;
      } else {
        final String mismatchMessage = String.format("unexpected exception type thrown;\nexpected: %s\nactual: %s",
            formatClass(expectedThrowable),
            formatClass(actualThrown.getClass()));

        throw new AssertionError(mismatchMessage, actualThrown);
      }
    }
    final String notThrownMessage = String.format("expected %s to be thrown, but nothing was thrown",
        formatClass(expectedThrowable));
    throw new AssertionError(notThrownMessage);
  }

  private static String formatClass(final Class<?> value) {
    // Fallback for anonymous inner classes
    final String className = value.getCanonicalName();
    return className == null ? value.getName() : className;
  }

  public interface ThrowingRunnable {
    void run() throws Throwable;
  }
}
