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

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Utility methods for configurable precision Decimal values (e.g. {@link BigDecimal}).
 */
public class DecimalUtility {
  private DecimalUtility() {}

  public static final int DECIMAL_BYTE_LENGTH = 16;
  public static final byte [] zeroes = new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
  public static final byte [] minus_one = new byte[] {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};

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
      throw new UnsupportedOperationException("BigDecimal precision can not be greater than that in the Arrow " +
        "vector: " + value.precision() + " > " + vectorPrecision);
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
    writeByteArrayToArrowBufHelper(bytes, bytebuf, index);
  }

  /**
   * Write the given long to the ArrowBuf at the given value index.
   */
  public static void writeLongToArrowBuf(long value, ArrowBuf bytebuf, int index) {
    final long addressOfValue = bytebuf.memoryAddress() + index * DECIMAL_BYTE_LENGTH;
    PlatformDependent.putLong(addressOfValue, value);
    final long padValue = Long.signum(value) == -1 ? -1L : 0L;
    PlatformDependent.putLong(addressOfValue + Long.BYTES, padValue);
  }

  /**
   * Write the given byte array to the ArrowBuf at the given value index. Will throw an
   * UnsupportedOperationException if the decimal size is greater than the Decimal vector byte
   * width.
   */
  public static void writeByteArrayToArrowBuf(byte[] bytes, ArrowBuf bytebuf, int index) {
    writeByteArrayToArrowBufHelper(bytes, bytebuf, index);
  }

  private static void writeByteArrayToArrowBufHelper(byte[] bytes, ArrowBuf bytebuf, int index) {
    final int startIndex = index * DECIMAL_BYTE_LENGTH;
    if (bytes.length > DECIMAL_BYTE_LENGTH) {
      throw new UnsupportedOperationException("Decimal size greater than 16 bytes");
    }

    // Decimal stored as little endian, need to swap data bytes before writing to ArrowBuf
    byte[] bytesLE = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      bytesLE[i] = bytes[bytes.length - 1 - i];
    }

    // Write LE data
    byte [] padByes = bytes[0] < 0 ? minus_one : zeroes;
    bytebuf.setBytes(startIndex, bytesLE, 0, bytes.length);
    bytebuf.setBytes(startIndex + bytes.length, padByes, 0, DECIMAL_BYTE_LENGTH - bytes.length);
  }
}
