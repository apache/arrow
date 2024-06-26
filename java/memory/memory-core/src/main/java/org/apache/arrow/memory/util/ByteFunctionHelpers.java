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

import java.nio.ByteOrder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;

/** Utility methods for memory comparison at a byte level. */
public class ByteFunctionHelpers {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ByteFunctionHelpers.class);

  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private ByteFunctionHelpers() {}

  /**
   * Helper function to check for equality of bytes in two ArrowBufs.
   *
   * @param left Left ArrowBuf for comparison
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right Right ArrowBuf for comparison
   * @param rStart start offset in the buffer
   * @param rEnd end offset in the buffer
   * @return 1 if equals, 0 otherwise
   */
  public static int equal(
      final ArrowBuf left, long lStart, long lEnd, final ArrowBuf right, long rStart, long rEnd) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
      right.checkBytes(rStart, rEnd);
    }
    return memEqual(left.memoryAddress(), lStart, lEnd, right.memoryAddress(), rStart, rEnd);
  }

  private static int memEqual(
      final long laddr, long lStart, long lEnd, final long raddr, long rStart, final long rEnd) {

    long n = lEnd - lStart;
    if (n == rEnd - rStart) {
      long lPos = laddr + lStart;
      long rPos = raddr + rStart;

      while (n > 63) {
        for (int x = 0; x < 8; x++) {
          long leftLong = MemoryUtil.UNSAFE.getLong(lPos);
          long rightLong = MemoryUtil.UNSAFE.getLong(rPos);
          if (leftLong != rightLong) {
            return 0;
          }
          lPos += 8;
          rPos += 8;
        }
        n -= 64;
      }

      while (n > 7) {
        long leftLong = MemoryUtil.UNSAFE.getLong(lPos);
        long rightLong = MemoryUtil.UNSAFE.getLong(rPos);
        if (leftLong != rightLong) {
          return 0;
        }
        lPos += 8;
        rPos += 8;
        n -= 8;
      }

      if (n > 3) {
        int leftInt = MemoryUtil.UNSAFE.getInt(lPos);
        int rightInt = MemoryUtil.UNSAFE.getInt(rPos);
        if (leftInt != rightInt) {
          return 0;
        }
        lPos += 4;
        rPos += 4;
        n -= 4;
      }

      while (n-- != 0) {
        byte leftByte = MemoryUtil.UNSAFE.getByte(lPos);
        byte rightByte = MemoryUtil.UNSAFE.getByte(rPos);
        if (leftByte != rightByte) {
          return 0;
        }
        lPos++;
        rPos++;
      }
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * Helper function to compare a set of bytes in two ArrowBufs.
   *
   * <p>Function will check data before completing in the case that
   *
   * @param left Left ArrowBuf to compare
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right Right ArrowBuf to compare
   * @param rStart start offset in the buffer
   * @param rEnd end offset in the buffer
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  public static int compare(
      final ArrowBuf left, long lStart, long lEnd, final ArrowBuf right, long rStart, long rEnd) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
      right.checkBytes(rStart, rEnd);
    }
    return memcmp(left.memoryAddress(), lStart, lEnd, right.memoryAddress(), rStart, rEnd);
  }

  private static int memcmp(
      final long laddr, long lStart, long lEnd, final long raddr, long rStart, final long rEnd) {
    long lLen = lEnd - lStart;
    long rLen = rEnd - rStart;
    long n = Math.min(rLen, lLen);
    long lPos = laddr + lStart;
    long rPos = raddr + rStart;

    while (n > 63) {
      for (int x = 0; x < 8; x++) {
        long leftLong = MemoryUtil.UNSAFE.getLong(lPos);
        long rightLong = MemoryUtil.UNSAFE.getLong(rPos);
        if (leftLong != rightLong) {
          if (LITTLE_ENDIAN) {
            return unsignedLongCompare(Long.reverseBytes(leftLong), Long.reverseBytes(rightLong));
          } else {
            return unsignedLongCompare(leftLong, rightLong);
          }
        }
        lPos += 8;
        rPos += 8;
      }
      n -= 64;
    }

    while (n > 7) {
      long leftLong = MemoryUtil.UNSAFE.getLong(lPos);
      long rightLong = MemoryUtil.UNSAFE.getLong(rPos);
      if (leftLong != rightLong) {
        if (LITTLE_ENDIAN) {
          return unsignedLongCompare(Long.reverseBytes(leftLong), Long.reverseBytes(rightLong));
        } else {
          return unsignedLongCompare(leftLong, rightLong);
        }
      }
      lPos += 8;
      rPos += 8;
      n -= 8;
    }

    if (n > 3) {
      int leftInt = MemoryUtil.UNSAFE.getInt(lPos);
      int rightInt = MemoryUtil.UNSAFE.getInt(rPos);
      if (leftInt != rightInt) {
        if (LITTLE_ENDIAN) {
          return unsignedIntCompare(Integer.reverseBytes(leftInt), Integer.reverseBytes(rightInt));
        } else {
          return unsignedIntCompare(leftInt, rightInt);
        }
      }
      lPos += 4;
      rPos += 4;
      n -= 4;
    }

    while (n-- != 0) {
      byte leftByte = MemoryUtil.UNSAFE.getByte(lPos);
      byte rightByte = MemoryUtil.UNSAFE.getByte(rPos);
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
      lPos++;
      rPos++;
    }

    if (lLen == rLen) {
      return 0;
    }

    return lLen > rLen ? 1 : -1;
  }

  /**
   * Helper function to compare a set of bytes in ArrowBuf to a ByteArray.
   *
   * @param left Left ArrowBuf for comparison purposes
   * @param lStart start offset in the buffer
   * @param lEnd end offset in the buffer
   * @param right second input to be compared
   * @param rStart start offset in the byte array
   * @param rEnd end offset in the byte array
   * @return 1 if left input is greater, -1 if left input is smaller, 0 otherwise
   */
  public static int compare(
      final ArrowBuf left, int lStart, int lEnd, final byte[] right, int rStart, final int rEnd) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      left.checkBytes(lStart, lEnd);
    }
    return memcmp(left.memoryAddress(), lStart, lEnd, right, rStart, rEnd);
  }

  /**
   * Compares the two specified {@code long} values, treating them as unsigned values between {@code
   * 0} and {@code 2^64 - 1} inclusive.
   *
   * @param a the first unsigned {@code long} to compare
   * @param b the second unsigned {@code long} to compare
   * @return a negative value if {@code a} is less than {@code b}; a positive value if {@code a} is
   *     greater than {@code b}; or zero if they are equal
   */
  public static int unsignedLongCompare(long a, long b) {
    return Long.compare(a ^ Long.MIN_VALUE, b ^ Long.MIN_VALUE);
  }

  public static int unsignedIntCompare(int a, int b) {
    return Integer.compare(a ^ Integer.MIN_VALUE, b ^ Integer.MIN_VALUE);
  }

  private static int memcmp(
      final long laddr, int lStart, int lEnd, final byte[] right, int rStart, final int rEnd) {
    int lLen = lEnd - lStart;
    int rLen = rEnd - rStart;
    int n = Math.min(rLen, lLen);
    long lPos = laddr + lStart;
    int rPos = rStart;

    while (n > 7) {
      long leftLong = MemoryUtil.UNSAFE.getLong(lPos);
      long rightLong = MemoryUtil.UNSAFE.getLong(right, MemoryUtil.BYTE_ARRAY_BASE_OFFSET + rPos);
      if (leftLong != rightLong) {
        if (LITTLE_ENDIAN) {
          return unsignedLongCompare(Long.reverseBytes(leftLong), Long.reverseBytes(rightLong));
        } else {
          return unsignedLongCompare(leftLong, rightLong);
        }
      }
      lPos += 8;
      rPos += 8;
      n -= 8;
    }

    if (n > 3) {
      int leftInt = MemoryUtil.UNSAFE.getInt(lPos);
      int rightInt = MemoryUtil.UNSAFE.getInt(right, MemoryUtil.BYTE_ARRAY_BASE_OFFSET + rPos);
      if (leftInt != rightInt) {
        if (LITTLE_ENDIAN) {
          return unsignedIntCompare(Integer.reverseBytes(leftInt), Integer.reverseBytes(rightInt));
        } else {
          return unsignedIntCompare(leftInt, rightInt);
        }
      }
      lPos += 4;
      rPos += 4;
      n -= 4;
    }

    while (n-- != 0) {
      byte leftByte = MemoryUtil.UNSAFE.getByte(lPos);
      byte rightByte = right[rPos];
      if (leftByte != rightByte) {
        return ((leftByte & 0xFF) - (rightByte & 0xFF)) > 0 ? 1 : -1;
      }
      lPos++;
      rPos++;
    }

    if (lLen == rLen) {
      return 0;
    }

    return lLen > rLen ? 1 : -1;
  }

  /** Compute hashCode with the given {@link ArrowBuf} and start/end index. */
  public static int hash(final ArrowBuf buf, long start, long end) {

    return hash(SimpleHasher.INSTANCE, buf, start, end);
  }

  /**
   * Compute hashCode with the given {@link ArrowBufHasher}, {@link ArrowBuf} and start/end index.
   */
  public static final int hash(ArrowBufHasher hasher, final ArrowBuf buf, long start, long end) {

    if (hasher == null) {
      hasher = SimpleHasher.INSTANCE;
    }

    return hasher.hashCode(buf, start, end - start);
  }

  /** Generate a new hashCode with the given current hashCode and new hashCode. */
  public static int combineHash(int currentHash, int newHash) {
    return currentHash * 31 + newHash;
  }
}
