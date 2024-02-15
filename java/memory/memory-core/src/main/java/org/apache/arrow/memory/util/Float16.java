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


import org.apache.arrow.util.VisibleForTesting;

/**
 * Lifted from Apache Parquet MR project:
 * https://github.com/apache/parquet-mr/blob/e87b80308869b77f914fcfd04364686e11158950/parquet-column/src/main/java/org/apache/parquet/schema/Float16.java
 * <ul>
 * Changes made:
 * <li>Modify the data type input from Parquet-MR Binary (toFloat(Binary b)) to Arrow Java short (toFloat(short b))</li>
 * <li>Expose NAN and POSITIVE_INFINITY variables</li>
 * </ul>
 *
 *
 * The class is a utility class to manipulate half-precision 16-bit
 * <a href="https://en.wikipedia.org/wiki/Half-precision_floating-point_format">IEEE 754</a>
 * floating point data types (also called fp16 or binary16). A half-precision float can be
 * created from or converted to single-precision floats, and is stored in a short data type.
 * The IEEE 754 standard specifies an float16 as having the following format:
 * <ul>
 * <li>Sign bit: 1 bit</li>
 * <li>Exponent width: 5 bits</li>
 * <li>Significand: 10 bits</li>
 * </ul>
 *
 * <p>The format is laid out as follows:</p>
 * <pre>
 * 1   11111   1111111111
 * ^   --^--   -----^----
 * sign  |          |_______ significand
 *       |
 *      -- exponent
 * </pre>
 * Half-precision floating points can be useful to save memory and/or
 * bandwidth at the expense of range and precision when compared to single-precision
 * floating points (float32).
 * Ref: https://android.googlesource.com/platform/libcore/+/master/luni/src/main/java/libcore/util/FP16.java
 */
public class Float16 {
  // Positive infinity of type half-precision float.
  public static final short POSITIVE_INFINITY = (short) 0x7c00;
  // A Not-a-Number representation of a half-precision float.
  public static final short NaN = (short) 0x7e00;
  // The bitmask to and a number with to obtain the sign bit.
  private static final int SIGN_MASK = 0x8000;
  // The offset to shift by to obtain the exponent bits.
  private static final int EXPONENT_SHIFT = 10;
  // The bitmask to and a number shifted by EXPONENT_SHIFT right, to obtain exponent bits.
  private static final int SHIFTED_EXPONENT_MASK = 0x1f;
  // The bitmask to and a number with to obtain significand bits.
  private static final int SIGNIFICAND_MASK = 0x3ff;
  // The offset of the exponent from the actual value.
  private static final int EXPONENT_BIAS = 15;
  // The offset to shift by to obtain the sign bit.
  private static final int SIGN_SHIFT = 15;
  // The bitmask to AND with to obtain exponent and significand bits.
  private static final int EXPONENT_SIGNIFICAND_MASK = 0x7fff;

  private static final int FP32_SIGN_SHIFT = 31;
  private static final int FP32_EXPONENT_SHIFT = 23;
  private static final int FP32_SHIFTED_EXPONENT_MASK = 0xff;
  private static final int FP32_SIGNIFICAND_MASK = 0x7fffff;
  private static final int FP32_EXPONENT_BIAS = 127;
  private static final int FP32_QNAN_MASK = 0x400000;
  private static final int FP32_DENORMAL_MAGIC = 126 << 23;
  private static final float FP32_DENORMAL_FLOAT = Float.intBitsToFloat(FP32_DENORMAL_MAGIC);

  /**
   * Returns true if the specified half-precision float value represents
   * a Not-a-Number, false otherwise.
   *
   * @param h A half-precision float value
   * @return True if the value is a NaN, false otherwise
   *
   */
  @VisibleForTesting
  public static boolean isNaN(short h) {
    return (h & EXPONENT_SIGNIFICAND_MASK) > POSITIVE_INFINITY;
  }

  /**
   * <p>Compares the two specified half-precision float values. The following
   * conditions apply during the comparison:</p>
   *
   * <ul>
   * <li>NaN is considered by this method to be equal to itself and greater
   * than all other half-precision float values (including {@code #POSITIVE_INFINITY})</li>
   * <li>POSITIVE_ZERO is considered by this method to be greater than NEGATIVE_ZERO.</li>
   * </ul>
   *
   * @param x The first half-precision float value to compare.
   * @param y The second half-precision float value to compare
   *
   * @return  The value {@code 0} if {@code x} is numerically equal to {@code y}, a
   *          value less than {@code 0} if {@code x} is numerically less than {@code y},
   *          and a value greater than {@code 0} if {@code x} is numerically greater
   *          than {@code y}
   *
   */
  @VisibleForTesting
  public static int compare(short x, short y) {
    boolean xIsNaN = isNaN(x);
    boolean yIsNaN = isNaN(y);

    if (!xIsNaN && !yIsNaN) {
      int first = ((x & SIGN_MASK) != 0 ? 0x8000 - (x & 0xffff) : x & 0xffff);
      int second = ((y & SIGN_MASK) != 0 ? 0x8000 - (y & 0xffff) : y & 0xffff);
      // Returns true if the first half-precision float value is less
      // (smaller toward negative infinity) than the second half-precision float value.
      if (first < second) {
        return -1;
      }

      // Returns true if the first half-precision float value is greater
      // (larger toward positive infinity) than the second half-precision float value.
      if (first > second) {
        return 1;
      }
    }

    // Collapse NaNs, akin to halfToIntBits(), but we want to keep
    // (signed) short value types to preserve the ordering of -0.0
    // and +0.0
    short xBits = xIsNaN ? NaN : x;
    short yBits = yIsNaN ? NaN : y;
    return (xBits == yBits ? 0 : (xBits < yBits ? -1 : 1));
  }

  /**
   * Converts the specified half-precision float value into a
   * single-precision float value. The following special cases are handled:
   * If the input is NaN, the returned value is Float NaN.
   * If the input is POSITIVE_INFINITY or NEGATIVE_INFINITY, the returned value is respectively
   *   Float POSITIVE_INFINITY or Float NEGATIVE_INFINITY.
   * If the input is 0 (positive or negative), the returned value is +/-0.0f.
   * Otherwise, the returned value is a normalized single-precision float value.
   *
   * @param b The half-precision float value to convert to single-precision
   * @return A normalized single-precision float value
   */
  @VisibleForTesting
  public static float toFloat(short b) {
    int bits = b & 0xffff;
    int s = bits & SIGN_MASK;
    int e = (bits >>> EXPONENT_SHIFT) & SHIFTED_EXPONENT_MASK;
    int m = (bits) & SIGNIFICAND_MASK;
    int outE = 0;
    int outM = 0;
    if (e == 0) { // Denormal or 0
      if (m != 0) {
        // Convert denorm fp16 into normalized fp32
        float o = Float.intBitsToFloat(FP32_DENORMAL_MAGIC + m);
        o -= FP32_DENORMAL_FLOAT;
        return s == 0 ? o : -o;
      }
    } else {
      outM = m << 13;
      if (e == 0x1f) { // Infinite or NaN
        outE = 0xff;
        if (outM != 0) { // SNaNs are quieted
          outM |= FP32_QNAN_MASK;
        }
      } else {
        outE = e - EXPONENT_BIAS + FP32_EXPONENT_BIAS;
      }
    }
    int out = (s << 16) | (outE << FP32_EXPONENT_SHIFT) | outM;
    return Float.intBitsToFloat(out);
  }

  /**
   * Converts the specified single-precision float value into a
   * half-precision float value. The following special cases are handled:
   *
   * If the input is NaN, the returned value is NaN.
   * If the input is Float POSITIVE_INFINITY or Float NEGATIVE_INFINITY,
   *   the returned value is respectively POSITIVE_INFINITY or NEGATIVE_INFINITY.
   * If the input is 0 (positive or negative), the returned value is
   *   POSITIVE_ZERO or NEGATIVE_ZERO.
   * If the input is a less than MIN_VALUE, the returned value
   *   is flushed to POSITIVE_ZERO or NEGATIVE_ZERO.
   * If the input is a less than MIN_NORMAL, the returned value
   *   is a denorm half-precision float.
   * Otherwise, the returned value is rounded to the nearest
   *   representable half-precision float value.
   *
   * @param f The single-precision float value to convert to half-precision
   * @return A half-precision float value
   */
  public static short toFloat16(float f) {
    int bits = Float.floatToRawIntBits(f);
    int s = (bits >>> FP32_SIGN_SHIFT);
    int e = (bits >>> FP32_EXPONENT_SHIFT) & FP32_SHIFTED_EXPONENT_MASK;
    int m = (bits) & FP32_SIGNIFICAND_MASK;
    int outE = 0;
    int outM = 0;
    if (e == 0xff) { // Infinite or NaN
      outE = 0x1f;
      outM = m != 0 ? 0x200 : 0;
    } else {
      e = e - FP32_EXPONENT_BIAS + EXPONENT_BIAS;
      if (e >= 0x1f) { // Overflow
        outE = 0x1f;
      } else if (e <= 0) { // Underflow
        if (e < -10) {
          // The absolute fp32 value is less than MIN_VALUE, flush to +/-0
        } else {
          // The fp32 value is a normalized float less than MIN_NORMAL,
          // we convert to a denorm fp16
          m = m | 0x800000;
          int shift = 14 - e;
          outM = m >> shift;
          int lowm = m & ((1 << shift) - 1);
          int hway = 1 << (shift - 1);
          // if above halfway or exactly halfway and outM is odd
          if (lowm + (outM & 1) > hway) {
            // Round to nearest even
            // Can overflow into exponent bit, which surprisingly is OK.
            // This increment relies on the +outM in the return statement below
            outM++;
          }
        }
      } else {
        outE = e;
        outM = m >> 13;
        // if above halfway or exactly halfway and outM is odd
        if ((m & 0x1fff) + (outM & 0x1) > 0x1000) {
          // Round to nearest even
          // Can overflow into exponent bit, which surprisingly is OK.
          // This increment relies on the +outM in the return statement below
          outM++;
        }
      }
    }
    // The outM is added here as the +1 increments for outM above can
    // cause an overflow in the exponent bit which is OK.
    return (short) ((s << SIGN_SHIFT) | (outE << EXPONENT_SHIFT) + outM);
  }

  /**
   * Returns a string representation of the specified half-precision
   * float value. Calling this method is equivalent to calling
   * <code>Float.toString(toFloat(h))</code>. See {@link Float#toString(float)}
   * for more information on the format of the string representation.
   *
   * @param h A half-precision float value in binary little-endian format
   * @return A string representation of the specified value
   */
  @VisibleForTesting
  public static String toFloatString(short h) {
    return Float.toString(Float16.toFloat(h));
  }
}
