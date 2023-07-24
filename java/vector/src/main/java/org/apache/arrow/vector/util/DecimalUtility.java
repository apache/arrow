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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;

/**
 * Utility methods for configurable precision Decimal values (e.g. {@link BigDecimal}).
 */
public class DecimalUtility {
  private DecimalUtility() {}

  public static final byte [] zeroes = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                                   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  public static final byte [] minus_one = new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                                                      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  /**
   * Read an ArrowType.Decimal at the given value index in the ArrowBuf and convert to a BigDecimal
   * with the given scale.
   */
  public static BigDecimal getBigDecimalFromArrowBuf(ArrowBuf bytebuf, int index, int scale, int byteWidth) {
    byte[] value = new byte[byteWidth];
    byte temp;
    final long startIndex = (long) index * byteWidth;

    bytebuf.getBytes(startIndex, value, 0, byteWidth);
    if (LITTLE_ENDIAN) {
      // Decimal stored as native endian, need to swap bytes to make BigDecimal if native endian is LE
      int stop = byteWidth / 2;
      for (int i = 0, j; i < stop; i++) {
        temp = value[i];
        j = (byteWidth - 1) - i;
        value[i] = value[j];
        value[j] = temp;
      }
    }
    BigInteger unscaledValue = new BigInteger(value);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Read an ArrowType.Decimal from the ByteBuffer and convert to a BigDecimal with the given
   * scale.
   */
  public static BigDecimal getBigDecimalFromByteBuffer(ByteBuffer bytebuf, int scale, int byteWidth) {
    byte[] value = new byte[byteWidth];
    bytebuf.get(value);
    BigInteger unscaledValue = new BigInteger(value);
    return new BigDecimal(unscaledValue, scale);
  }

  /**
   * Read an ArrowType.Decimal from the ArrowBuf at the given value index and return it as a byte
   * array.
   */
  public static byte[] getByteArrayFromArrowBuf(ArrowBuf bytebuf, int index, int byteWidth) {
    final byte[] value = new byte[byteWidth];
    final long startIndex = (long) index * byteWidth;
    bytebuf.getBytes(startIndex, value, 0, byteWidth);
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
      throw new UnsupportedOperationException("BigDecimal precision can not be greater than that in the Arrow " +
        "vector: " + value.precision() + " > " + vectorPrecision);
    }
    return true;
  }

  /**
   * Check that the decimal scale equals the vectorScale and that the decimal precision is
   * less than or equal to the vectorPrecision. If not, then an UnsupportedOperationException is
   * thrown, otherwise returns true.
   */
  public static boolean checkPrecisionAndScale(int decimalPrecision, int decimalScale, int vectorPrecision,
                                               int vectorScale) {
    if (decimalScale != vectorScale) {
      throw new UnsupportedOperationException("BigDecimal scale must equal that in the Arrow vector: " +
          decimalScale + " != " + vectorScale);
    }
    if (decimalPrecision > vectorPrecision) {
      throw new UnsupportedOperationException("BigDecimal precision can not be greater than that in the Arrow " +
          "vector: " + decimalPrecision + " > " + vectorPrecision);
    }
    return true;
  }

  /**
   * Write the given BigDecimal to the ArrowBuf at the given value index. Will throw an
   * UnsupportedOperationException if the decimal size is greater than the Decimal vector byte
   * width.
   */
  public static void writeBigDecimalToArrowBuf(BigDecimal value, ArrowBuf bytebuf, int index, int byteWidth) {
    final byte[] bytes = value.unscaledValue().toByteArray();
    writeByteArrayToArrowBufHelper(bytes, bytebuf, index, byteWidth);
  }

  /**
   * Write the given long to the ArrowBuf at the given value index.
   * This routine extends the original sign bit to a new upper area in 128-bit or 256-bit.
   */
  public static void writeLongToArrowBuf(long value, ArrowBuf bytebuf, int index, int byteWidth) {
    if (byteWidth != 16 && byteWidth != 32) {
      throw new UnsupportedOperationException("DecimalUtility.writeLongToArrowBuf() currently supports " +
          "128-bit or 256-bit width data");
    }
    final long addressOfValue = bytebuf.memoryAddress() + (long) index * byteWidth;
    final long padValue = Long.signum(value) == -1 ? -1L : 0L;
    if (LITTLE_ENDIAN) {
      MemoryUtil.UNSAFE.putLong(addressOfValue, value);
      for (int i = 1; i <= (byteWidth - 8) / 8; i++) {
        MemoryUtil.UNSAFE.putLong(addressOfValue + Long.BYTES * i, padValue);
      }
    } else {
      for (int i = 0; i < (byteWidth - 8) / 8; i++) {
        MemoryUtil.UNSAFE.putLong(addressOfValue + Long.BYTES * i, padValue);
      }
      MemoryUtil.UNSAFE.putLong(addressOfValue + Long.BYTES * (byteWidth - 8) / 8, value);
    }
  }

  /**
   * Write the given byte array to the ArrowBuf at the given value index. Will throw an
   * UnsupportedOperationException if the decimal size is greater than the Decimal vector byte
   * width.
   */
  public static void writeByteArrayToArrowBuf(byte[] bytes, ArrowBuf bytebuf, int index, int byteWidth) {
    writeByteArrayToArrowBufHelper(bytes, bytebuf, index, byteWidth);
  }

  private static void writeByteArrayToArrowBufHelper(byte[] bytes, ArrowBuf bytebuf, int index, int byteWidth) {
    final long startIndex = (long) index * byteWidth;
    if (bytes.length > byteWidth) {
      throw new UnsupportedOperationException("Decimal size greater than " + byteWidth + " bytes: " + bytes.length);
    }

    byte [] padBytes = bytes[0] < 0 ? minus_one : zeroes;
    if (LITTLE_ENDIAN) {
      // Decimal stored as native-endian, need to swap data bytes before writing to ArrowBuf if LE
      byte[] bytesLE = new byte[bytes.length];
      for (int i = 0; i < bytes.length; i++) {
        bytesLE[i] = bytes[bytes.length - 1 - i];
      }

      // Write LE data
      bytebuf.setBytes(startIndex, bytesLE, 0, bytes.length);
      bytebuf.setBytes(startIndex + bytes.length, padBytes, 0, byteWidth - bytes.length);
    } else {
      // Write BE data
      bytebuf.setBytes(startIndex + byteWidth - bytes.length, bytes, 0, bytes.length);
      bytebuf.setBytes(startIndex, padBytes, 0, byteWidth - bytes.length);
    }
  }
}
