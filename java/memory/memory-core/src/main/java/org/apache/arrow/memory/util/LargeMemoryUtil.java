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

import org.apache.arrow.memory.BoundsChecking;

/** Contains utilities for dealing with a 64-bit address base. */
public final class LargeMemoryUtil {

  private LargeMemoryUtil() {}

  /**
   * Casts length to an int, but raises an exception the value is outside
   * the range of an int.
   */
  public static int checkedCastToInt(long length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      return Math.toIntExact(length);
    }
    return (int) length;
  }

  /**
   * Returns a min(Integer.MAX_VALUE, length).
   */
  public static int capAtMaxInt(long length) {
    return (int) Math.min(length, Integer.MAX_VALUE);
  }
}
