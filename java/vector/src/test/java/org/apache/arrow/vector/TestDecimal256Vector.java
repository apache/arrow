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

package org.apache.arrow.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.TransferPair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestDecimal256Vector {

  private static long[] intValues;

  static {
    intValues = new long[60];
    for (int i = 0; i < intValues.length / 2; i++) {
      intValues[i] = 1L << (i + 1);
      intValues[2 * i] = -1L * (1 << (i + 1));
    }
  }

  private int scale = 3;

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testValuesWriteRead() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
        new ArrowType.Decimal(10, scale, 256), allocator);) {

      try (Decimal256Vector oldConstructor = new Decimal256Vector("decimal", allocator, 10, scale);) {
        assertEquals(decimalVector.getField().getType(), oldConstructor.getField().getType());
      }

      decimalVector.allocateNew();
      BigDecimal[] values = new BigDecimal[intValues.length];
      for (int i = 0; i < intValues.length; i++) {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(intValues[i]), scale);
        values[i] = decimal;
        decimalVector.setSafe(i, decimal);
      }

      decimalVector.setValueCount(intValues.length);

      for (int i = 0; i < intValues.length; i++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals(values[i], value, "unexpected data at index: " + i);
      }
    }
  }

  @Test
  public void testDecimal256DifferentScaleAndPrecision() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
        new ArrowType.Decimal(4, 2, 256), allocator)) {
      decimalVector.allocateNew();

      // test Decimal256 with different scale
      {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(0), 3);
        UnsupportedOperationException ue =
            assertThrows(UnsupportedOperationException.class, () -> decimalVector.setSafe(0, decimal));
        assertEquals("BigDecimal scale must equal that in the Arrow vector: 3 != 2", ue.getMessage());
      }

      // test BigDecimal with larger precision than initialized
      {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(12345), 2);
        UnsupportedOperationException ue =
            assertThrows(UnsupportedOperationException.class, () -> decimalVector.setSafe(0, decimal));
        assertEquals("BigDecimal precision cannot be greater than that in the Arrow vector: 5 > 4", ue.getMessage());
      }
    }
  }

  @Test
  public void testWriteBigEndian() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
        new ArrowType.Decimal(38, 18, 256), allocator);) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000000000000");
      BigDecimal decimal4 = new BigDecimal("0.111111111000000000");
      BigDecimal decimal5 = new BigDecimal("987654321.123456789000000000");
      BigDecimal decimal6 = new BigDecimal("222222222222.222222222000000000");
      BigDecimal decimal7 = new BigDecimal("7777777777777.666666667000000000");
      BigDecimal decimal8 = new BigDecimal("1212121212.343434343000000000");

      byte[] decimalValue1 = decimal1.unscaledValue().toByteArray();
      byte[] decimalValue2 = decimal2.unscaledValue().toByteArray();
      byte[] decimalValue3 = decimal3.unscaledValue().toByteArray();
      byte[] decimalValue4 = decimal4.unscaledValue().toByteArray();
      byte[] decimalValue5 = decimal5.unscaledValue().toByteArray();
      byte[] decimalValue6 = decimal6.unscaledValue().toByteArray();
      byte[] decimalValue7 = decimal7.unscaledValue().toByteArray();
      byte[] decimalValue8 = decimal8.unscaledValue().toByteArray();

      decimalVector.setBigEndian(0, decimalValue1);
      decimalVector.setBigEndian(1, decimalValue2);
      decimalVector.setBigEndian(2, decimalValue3);
      decimalVector.setBigEndian(3, decimalValue4);
      decimalVector.setBigEndian(4, decimalValue5);
      decimalVector.setBigEndian(5, decimalValue6);
      decimalVector.setBigEndian(6, decimalValue7);
      decimalVector.setBigEndian(7, decimalValue8);

      decimalVector.setValueCount(8);
      assertEquals(8, decimalVector.getValueCount());
      assertEquals(decimal1, decimalVector.getObject(0));
      assertEquals(decimal2, decimalVector.getObject(1));
      assertEquals(decimal3, decimalVector.getObject(2));
      assertEquals(decimal4, decimalVector.getObject(3));
      assertEquals(decimal5, decimalVector.getObject(4));
      assertEquals(decimal6, decimalVector.getObject(5));
      assertEquals(decimal7, decimalVector.getObject(6));
      assertEquals(decimal8, decimalVector.getObject(7));
    }
  }

  @Test
  public void testLongReadWrite() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
            new ArrowType.Decimal(38, 0, 256), allocator)) {
      decimalVector.allocateNew();

      long[] longValues = {0L, -2L, Long.MAX_VALUE, Long.MIN_VALUE, 187L};

      for (int i = 0; i < longValues.length; ++i) {
        decimalVector.set(i, longValues[i]);
      }

      decimalVector.setValueCount(longValues.length);

      for (int i = 0; i < longValues.length; ++i) {
        assertEquals(new BigDecimal(longValues[i]), decimalVector.getObject(i));
      }
    }
  }


  @Test
  public void testBigDecimalReadWrite() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
        new ArrowType.Decimal(38, 9, 256), allocator);) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000");
      BigDecimal decimal4 = new BigDecimal("-0.111111111");
      BigDecimal decimal5 = new BigDecimal("-987654321.123456789");
      BigDecimal decimal6 = new BigDecimal("-222222222222.222222222");
      BigDecimal decimal7 = new BigDecimal("7777777777777.666666667");
      BigDecimal decimal8 = new BigDecimal("1212121212.343434343");

      decimalVector.set(0, decimal1);
      decimalVector.set(1, decimal2);
      decimalVector.set(2, decimal3);
      decimalVector.set(3, decimal4);
      decimalVector.set(4, decimal5);
      decimalVector.set(5, decimal6);
      decimalVector.set(6, decimal7);
      decimalVector.set(7, decimal8);

      decimalVector.setValueCount(8);
      assertEquals(8, decimalVector.getValueCount());
      assertEquals(decimal1, decimalVector.getObject(0));
      assertEquals(decimal2, decimalVector.getObject(1));
      assertEquals(decimal3, decimalVector.getObject(2));
      assertEquals(decimal4, decimalVector.getObject(3));
      assertEquals(decimal5, decimalVector.getObject(4));
      assertEquals(decimal6, decimalVector.getObject(5));
      assertEquals(decimal7, decimalVector.getObject(6));
      assertEquals(decimal8, decimalVector.getObject(7));
    }
  }

  /**
   * Test {@link Decimal256Vector#setBigEndian(int, byte[])} which takes BE layout input and stores in native-endian
   * (NE) layout.
   * Cases to cover: input byte array in different lengths in range [1-16] and negative values.
   */
  @Test
  public void decimalBE2NE() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
        new ArrowType.Decimal(23, 2, 256), allocator)) {
      decimalVector.allocateNew();

      BigInteger[] testBigInts = new BigInteger[] {
          new BigInteger("0"),
          new BigInteger("-1"),
          new BigInteger("23"),
          new BigInteger("234234"),
          new BigInteger("-234234234"),
          new BigInteger("234234234234"),
          new BigInteger("-56345345345345"),
          new BigInteger("2982346298346289346293467923465345634500"), // converts to 16+ byte array
          new BigInteger("-389457298347598237459832459823434653600"), // converts to 16+ byte array
          new BigInteger("-345345"),
          new BigInteger("754533")
      };

      int insertionIdx = 0;
      insertionIdx++; // insert a null
      for (BigInteger val : testBigInts) {
        decimalVector.setBigEndian(insertionIdx++, val.toByteArray());
      }
      insertionIdx++; // insert a null
      // insert a zero length buffer
      decimalVector.setBigEndian(insertionIdx++, new byte[0]);

      // Try inserting a buffer larger than 33 bytes and expect a failure
      final int insertionIdxCapture = insertionIdx;
      IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
          () -> decimalVector.setBigEndian(insertionIdxCapture, new byte[33]));
      assertTrue(ex.getMessage().equals("Invalid decimal value length. Valid length in [1 - 32], got 33"));
      decimalVector.setValueCount(insertionIdx);

      // retrieve values and check if they are correct
      int outputIdx = 0;
      assertTrue(decimalVector.isNull(outputIdx++));
      for (BigInteger expected : testBigInts) {
        final BigDecimal actual = decimalVector.getObject(outputIdx++);
        assertEquals(expected, actual.unscaledValue());
      }
      assertTrue(decimalVector.isNull(outputIdx++));
      assertEquals(BigInteger.valueOf(0), decimalVector.getObject(outputIdx).unscaledValue());
    }
  }

  @Test
  public void setUsingArrowBufOfLEInts() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
            new ArrowType.Decimal(5, 2, 256), allocator);
         ArrowBuf buf = allocator.buffer(8);) {
      decimalVector.allocateNew();

      // add a positive value equivalent to 705.32
      int val = 70532;
      buf.setInt(0, val);
      decimalVector.setSafe(0, 0, buf, 4);

      // add a -ve value equivalent to -705.32
      val = -70532;
      buf.setInt(4, val);
      decimalVector.setSafe(1, 4, buf, 4);

      decimalVector.setValueCount(2);

      BigDecimal [] expectedValues = new BigDecimal[] {BigDecimal.valueOf(705.32), BigDecimal
              .valueOf(-705.32)};
      for (int i = 0; i < 2; i ++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals(expectedValues[i], value);
      }
    }

  }

  @Test
  public void setUsingArrowLongLEBytes() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
            new ArrowType.Decimal(18, 0, 256), allocator);
         ArrowBuf buf = allocator.buffer(16);) {
      decimalVector.allocateNew();

      long val = Long.MAX_VALUE;
      buf.setLong(0, val);
      decimalVector.setSafe(0, 0, buf, 8);

      val = Long.MIN_VALUE;
      buf.setLong(8, val);
      decimalVector.setSafe(1, 8, buf, 8);

      decimalVector.setValueCount(2);

      BigDecimal [] expectedValues = new BigDecimal[] {BigDecimal.valueOf(Long.MAX_VALUE), BigDecimal
              .valueOf(Long.MIN_VALUE)};
      for (int i = 0; i < 2; i ++) {
        BigDecimal value = decimalVector.getObject(i);
        assertEquals(expectedValues[i], value);
      }
    }
  }

  @Test
  public void setUsingArrowBufOfBEBytes() {
    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
            new ArrowType.Decimal(5, 2, 256), allocator);
         ArrowBuf buf = allocator.buffer(9);) {
      BigDecimal [] expectedValues = new BigDecimal[] {BigDecimal.valueOf(705.32), BigDecimal
              .valueOf(-705.32), BigDecimal.valueOf(705.32)};
      verifyWritingArrowBufWithBigEndianBytes(decimalVector, buf, expectedValues, 3);
    }

    try (Decimal256Vector decimalVector = TestUtils.newVector(Decimal256Vector.class, "decimal",
            new ArrowType.Decimal(43, 2, 256), allocator);
         ArrowBuf buf = allocator.buffer(45);) {
      BigDecimal[] expectedValues = new BigDecimal[] {new BigDecimal("29823462983462893462934679234653450000000.63"),
                                                      new BigDecimal("-2982346298346289346293467923465345.63"),
                                                      new BigDecimal("2982346298346289346293467923465345.63")};
      verifyWritingArrowBufWithBigEndianBytes(decimalVector, buf, expectedValues, 15);
    }
  }

  @Test
  public void testGetTransferPairWithField() {
    final Decimal256Vector fromVector = new Decimal256Vector("decimal", allocator, 10, scale);
    final TransferPair transferPair = fromVector.getTransferPair(fromVector.getField(), allocator);
    final Decimal256Vector toVector = (Decimal256Vector) transferPair.getTo();
    // Field inside a new vector created by reusing a field should be the same in memory as the original field.
    assertSame(fromVector.getField(), toVector.getField());
  }

  private void verifyWritingArrowBufWithBigEndianBytes(Decimal256Vector decimalVector,
                                                       ArrowBuf buf, BigDecimal[] expectedValues,
                                                       int length) {
    decimalVector.allocateNew();
    for (int i = 0; i < expectedValues.length; i++) {
      byte[] bigEndianBytes = expectedValues[i].unscaledValue().toByteArray();
      buf.setBytes(length * i , bigEndianBytes, 0 , bigEndianBytes.length);
      decimalVector.setBigEndianSafe(i, length * i, buf, bigEndianBytes.length);
    }

    decimalVector.setValueCount(3);

    for (int i = 0; i < expectedValues.length; i ++) {
      BigDecimal value = decimalVector.getObject(i);
      assertEquals(expectedValues[i], value);
    }
  }
}
