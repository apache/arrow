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

package org.apache.arrow.util;

/**
 * Utilities for rounding data size.
 */
public final class DataSizeRoundingUtil {

  /**
   * The mask for rounding an integer to a multiple of 8.
   * (i.e. clear the lowest 3 bits)
   */
  public static int ROUND_8_MASK_INT = 0xFFFFFFF8;

  /**
   * The mask for rounding a long integer to a multiple of 8.
   * (i.e. clear the lowest 3 bits)
   */
  public static long ROUND_8_MASK_LONG = 0xFFFFFFFFFFFFFFF8L;

  /**
   * The number of bits to shift for dividing by 8.
   */
  public static int DIVIDE_BY_8_SHIFT_BITS = 3;

  /**
   * Round up the number to the nearest multiple of 8.
   * @param input the number to round.
   * @return the rounded number.
   */
  public static int roundUpTo8Multiple(int input) {
    return (input + 7) & ROUND_8_MASK_INT;
  }

  /**
   * Round up the number to the nearest multiple of 8.
   * @param input the number to round.
   * @return the rounded number
   */
  public static long roundUpTo8Multiple(long input) {
    return (input + 7L) & ROUND_8_MASK_LONG;
  }

  /**
   * Round down the number to the nearest multiple of 8.
   * @param input the number to round.
   * @return the rounded number.
   */
  public static int roundDownTo8Multiple(int input) {
    return input & ROUND_8_MASK_INT;
  }

  /**
   * Round down the number to the nearest multiple of 8.
   * @param input the number to round.
   * @return the rounded number
   */
  public static long roundDownTo8Multiple(long input) {
    return input & ROUND_8_MASK_LONG;
  }

  /**
   * A fast way to compute Math.ceil(input / 8.0).
   * @param input the input number.
   * @return the computed number.
   */
  public static int divideBy8Ceil(int input) {
    return (input + 7) >>> DIVIDE_BY_8_SHIFT_BITS;
  }

  /**
   * A fast way to compute Math.ceil(input / 8.0).
   * @param input the input number.
   * @return the computed number.
   */
  public static long divideBy8Ceil(long input) {
    return (input + 7) >>> (long) DIVIDE_BY_8_SHIFT_BITS;
  }

  private DataSizeRoundingUtil() {

  }
}
