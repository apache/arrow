/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import io.netty.buffer.ArrowBuf;

public class DecimalUtility {

  public final static int MAX_DIGITS = 9;
  public final static int DIGITS_BASE = 1000000000;
  public final static int DIGITS_MAX = 999999999;
  public final static int INTEGER_SIZE = (Integer.SIZE / 8);

  public final static String[] decimalToString = {"",
      "0",
      "00",
      "000",
      "0000",
      "00000",
      "000000",
      "0000000",
      "00000000",
      "000000000"};

  public final static long[] scale_long_constants = {
      1,
      10,
      100,
      1000,
      10000,
      100000,
      1000000,
      10000000,
      100000000,
      1000000000,
      10000000000l,
      100000000000l,
      1000000000000l,
      10000000000000l,
      100000000000000l,
      1000000000000000l,
      10000000000000000l,
      100000000000000000l,
      1000000000000000000l};

  public static final int DECIMAL_BYTE_LENGTH = 16;

  /**
   * Simple function that returns the static precomputed
   * power of ten, instead of using Math.pow
   */
  public static long getPowerOfTen(int power) {
    assert power >= 0 && power < scale_long_constants.length;
    return scale_long_constants[(power)];
  }

  /**
   * Math.pow returns a double and while multiplying with large digits
   * in the decimal data type we encounter noise. So instead of multiplying
   * with Math.pow we use the static constants to perform the multiplication
   */
  public static long adjustScaleMultiply(long input, int factor) {
    int index = Math.abs(factor);
    assert index >= 0 && index < scale_long_constants.length;
    if (factor >= 0) {
      return input * scale_long_constants[index];
    } else {
      return input / scale_long_constants[index];
    }
  }

  public static long adjustScaleDivide(long input, int factor) {
    int index = Math.abs(factor);
    assert index >= 0 && index < scale_long_constants.length;
    if (factor >= 0) {
      return input / scale_long_constants[index];
    } else {
      return input * scale_long_constants[index];
    }
  }

  /**
   * Returns a string representation of the given integer
   * If the length of the given integer is less than the
   * passed length, this function will prepend zeroes to the string
   */
  public static StringBuilder toStringWithZeroes(int number, int desiredLength) {
    String value = ((Integer) number).toString();
    int length = value.length();

    StringBuilder str = new StringBuilder();
    str.append(decimalToString[desiredLength - length]);
    str.append(value);

    return str;
  }

  public static StringBuilder toStringWithZeroes(long number, int desiredLength) {
    String value = ((Long) number).toString();
    int length = value.length();

    StringBuilder str = new StringBuilder();

    // Desired length can be > MAX_DIGITS
    int zeroesLength = desiredLength - length;
    while (zeroesLength > MAX_DIGITS) {
      str.append(decimalToString[MAX_DIGITS]);
      zeroesLength -= MAX_DIGITS;
    }
    str.append(decimalToString[zeroesLength]);
    str.append(value);

    return str;
  }

  /**
   * Read an ArrowType.Decimal at the given value index in the ArrowBuf and convert to a BigDecimal
   * with the given scale.
   */
  public static BigDecimal getBigDecimalFromArrowBuf(ArrowBuf bytebuf, int index, int scale) {
    byte[] value = new byte[DECIMAL_BYTE_LENGTH];
    byte temp;
    final int startIndex = index * DECIMAL_BYTE_LENGTH;

    // Decimal stored as little endian, need to swap bytes to make BigDecimal
    bytebuf.getBytes(startIndex, value, 0, DECIMAL_BYTE_LENGTH);
    int stop = DECIMAL_BYTE_LENGTH / 2;
    for (int i = 0, j; i < stop; i++) {
      temp = value[i];
      j = (DECIMAL_BYTE_LENGTH - 1) - i;
      value[i] = value[j];
      value[j] = temp;
    }
    BigInteger unscaledValue = new BigInteger(value);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Read an ArrowType.Decimal from the ByteBuffer and convert to a BigDecimal with the given
   * scale.
   */
  public static BigDecimal getBigDecimalFromByteBuffer(ByteBuffer bytebuf, int scale) {
    byte[] value = new byte[DECIMAL_BYTE_LENGTH];
    bytebuf.get(value);
    BigInteger unscaledValue = new BigInteger(value);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Read an ArrowType.Decimal from the ArrowBuf at the given value index and return it as a byte
   * array.
   */
  public static byte[] getByteArrayFromArrowBuf(ArrowBuf bytebuf, int index) {
    final byte[] value = new byte[DECIMAL_BYTE_LENGTH];
    final int startIndex = index * DECIMAL_BYTE_LENGTH;
    bytebuf.getBytes(startIndex, value, 0, DECIMAL_BYTE_LENGTH);
    return value;
  }

  /**
   * Check that the BigDecimal scale equals the vectorScale and that the BigDecimal precision is
   * less than or equal to the vectorPrecision. If not, then an UnsupportedOperationException is
   * thrown, otherwise returns true.
   */
  public static boolean checkPrecisionAndScale(BigDecimal value, int vectorPrecision, int vectorScale) {
    if (value.scale() != vectorScale) {
      throw new UnsupportedOperationException("BigDecimal scale must equal that in the Arrow vector: " +
          value.scale() + " != " + vectorScale);
    }
    if (value.precision() > vectorPrecision) {
      throw new UnsupportedOperationException("BigDecimal precision can not be greater than that in the Arrow vector: " +
          value.precision() + " > " + vectorPrecision);
    }
    return true;
  }

  /**
   * Write the given BigDecimal to the ArrowBuf at the given value index. Will throw an
   * UnsupportedOperationException if the decimal size is greater than the Decimal vector byte
   * width.
   */
  public static void writeBigDecimalToArrowBuf(BigDecimal value, ArrowBuf bytebuf, int index) {
    final byte[] bytes = value.unscaledValue().toByteArray();
    final int padValue = value.signum() == -1 ? 0xFF : 0;
    writeByteArrayToArrowBuf(bytes, bytebuf, index, padValue);
  }

  /**
   * Write the given byte array to the ArrowBuf at the given value index. Will throw an
   * UnsupportedOperationException if the decimal size is greater than the Decimal vector byte
   * width.
   */
  public static void writeByteArrayToArrowBuf(byte[] bytes, ArrowBuf bytebuf, int index) {
    writeByteArrayToArrowBuf(bytes, bytebuf, index, 0);
  }

  private static void writeByteArrayToArrowBuf(byte[] bytes, ArrowBuf bytebuf, int index, int padValue) {
    final int startIndex = index * DECIMAL_BYTE_LENGTH;
    if (bytes.length > DECIMAL_BYTE_LENGTH) {
      throw new UnsupportedOperationException("Decimal size greater than 16 bytes");
    }

    // Decimal stored as little endian, need to swap data bytes before writing to ArrowBuf
    byte[] bytesLE = new byte[bytes.length];
    int stop = bytes.length / 2;
    for (int i = 0, j; i < stop; i++) {
      j = (bytes.length - 1) - i;
      bytesLE[i] = bytes[j];
      bytesLE[j] = bytes[i];
    }
    if (bytes.length % 2 != 0) {
      int i = (bytes.length / 2);
      bytesLE[i] = bytes[i];
    }

    // Write LE data
    bytebuf.setBytes(startIndex, bytesLE, 0, bytes.length);

    // Write padding after data
    for (int i = bytes.length; i < DECIMAL_BYTE_LENGTH; i++) {
      bytebuf.setByte(startIndex + i, padValue);
    }
  }
}
