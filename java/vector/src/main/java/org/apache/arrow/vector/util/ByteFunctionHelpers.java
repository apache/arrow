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

package org.apache.arrow.vector.util;

import io.netty.buffer.ArrowBuf;

/**
 * @deprecated This class will be removed. Please use org.apache.arrow.memory.util.ByteFunctionHelpers instead.
 */
@Deprecated
public class ByteFunctionHelpers {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ByteFunctionHelpers.class);

  private ByteFunctionHelpers() {}

  /**
   * @deprecated Helper function to check for equality of bytes in two ArrowBufs.
   *
   * @param left   Left ArrowBuf for comparison
   * @param lStart start offset in the buffer
   * @param lEnd   end offset in the buffer
   * @param right  Right ArrowBuf for comparison
   * @param rStart start offset in the buffer
   * @param rEnd   end offset in the buffer
   * @return 1 if equals, 0 otherwise
   */
  @Deprecated
  public static final int equal(final ArrowBuf left, int lStart, int lEnd, final ArrowBuf right, int rStart, int rEnd) {
    return org.apache.arrow.memory.util.ByteFunctionHelpers.equal(left, lStart, lEnd, right, rStart, rEnd);
  }

  /**
   * @deprecated Helper function to compare a set of bytes in two ArrowBufs.
   *     Function will check data before completing in the case that
   *
   * @param left   Left ArrowBuf to compare
   * @param lStart start offset in the buffer
   * @param lEnd   end offset in the buffer
   * @param right  Right ArrowBuf to compare
   * @param rStart start offset in the buffer
   * @param rEnd   end offset in the buffer
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  @Deprecated
  public static final int compare(
      final ArrowBuf left,
      int lStart,
      int lEnd,
      final ArrowBuf right,
      int rStart,
      int rEnd) {
    return org.apache.arrow.memory.util.ByteFunctionHelpers.compare(left, lStart, lEnd, right, rStart, rEnd);
  }

  /**
   * @deprecated Helper function to compare a set of bytes in ArrowBuf to a ByteArray.
   *
   * @param left   Left ArrowBuf for comparison purposes
   * @param lStart start offset in the buffer
   * @param lEnd   end offset in the buffer
   * @param right  second input to be compared
   * @param rStart start offset in the byte array
   * @param rEnd   end offset in the byte array
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  @Deprecated
  public static final int compare(
      final ArrowBuf left,
      int lStart,
      int lEnd,
      final byte[] right,
      int rStart,
      final int rEnd) {
    return org.apache.arrow.memory.util.ByteFunctionHelpers.compare(left, lStart, lEnd, right, rStart, rEnd);
  }

  /**
   * @deprecated Compares the two specified {@code long} values, treating them as unsigned values between
   * {@code 0} and {@code 2^64 - 1} inclusive.
   *
   * @param a the first unsigned {@code long} to compare
   * @param b the second unsigned {@code long} to compare
   * @return a negative value if {@code a} is less than {@code b}; a positive value if {@code a} is
   *     greater than {@code b}; or zero if they are equal
   */
  @Deprecated
  public static int unsignedLongCompare(long a, long b) {
    return org.apache.arrow.memory.util.ByteFunctionHelpers.unsignedLongCompare(a, b);
  }

  @Deprecated
  public static int unsignedIntCompare(int a, int b) {
    return org.apache.arrow.memory.util.ByteFunctionHelpers.unsignedIntCompare(a, b);
  }
}
