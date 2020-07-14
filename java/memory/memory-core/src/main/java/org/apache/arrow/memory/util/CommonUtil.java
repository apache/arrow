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

import java.util.Arrays;

/**
 * Utilities and static methods needed for arrow-memory.
 */
public final class CommonUtil {

  private CommonUtil() { }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val An integer value.
   * @return The closest power of two of that value.
   */
  public static int nextPowerOfTwo(int val) {
    if (val == 0 || val == 1) {
      return val + 1;
    }
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  /**
   * Rounds up the provided value to the nearest power of two.
   *
   * @param val A long value.
   * @return The closest power of two of that value.
   */
  public static long nextPowerOfTwo(long val) {
    if (val == 0 || val == 1) {
      return val + 1;
    }
    long highestBit = Long.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  /**
   * Specify an indentation amount when using a StringBuilder.
   *
   * @param sb StringBuilder to use
   * @param indent Indentation amount
   * @return the StringBuilder object with indentation applied
   */
  public static StringBuilder indent(StringBuilder sb, int indent) {
    final char[] indentation = new char[indent * 2];
    Arrays.fill(indentation, ' ');
    sb.append(indentation);
    return sb;
  }
}

