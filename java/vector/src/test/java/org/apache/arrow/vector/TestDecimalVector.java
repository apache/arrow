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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDecimalVector {

  private static long[] intValues;

  static {
    intValues = new long[60];
    for (int i = 0; i < intValues.length / 2; i++) {
      intValues[i] = 1 << i + 1;
      intValues[2 * i] = -1 * (1 << i + 1);
    }
  }

  private int scale = 3;

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testValuesWriteRead() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
      new ArrowType.Decimal(10, scale), allocator);) {

      try (DecimalVector oldConstructor = new DecimalVector("decimal", allocator, 10, scale);) {
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
        assertEquals("unexpected data at index: " + i, values[i], value);
      }
    }
  }

  @Test
  public void testBigDecimalDifferentScaleAndPrecision() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
      new ArrowType.Decimal(4, 2), allocator);) {
      decimalVector.allocateNew();

      // test BigDecimal with different scale
      boolean hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(0), 3);
        decimalVector.setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }

      // test BigDecimal with larger precision than initialized
      hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(12345), 2);
        decimalVector.setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }
    }
  }

  @Test
  public void testWriteBigEndian() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
      new ArrowType.Decimal(38, 9), allocator);) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000");
      BigDecimal decimal4 = new BigDecimal("0.111111111");
      BigDecimal decimal5 = new BigDecimal("987654321.123456789");
      BigDecimal decimal6 = new BigDecimal("222222222222.222222222");
      BigDecimal decimal7 = new BigDecimal("7777777777777.666666667");
      BigDecimal decimal8 = new BigDecimal("1212121212.343434343");

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
  public void testBigDecimalReadWrite() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
      new ArrowType.Decimal(38, 9), allocator);) {
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
   * Test {@link DecimalVector#setBigEndian(int, byte[])} which takes BE layout input and stores in LE layout.
   * Cases to cover: input byte array in different lengths in range [1-16] and negative values.
   */
  @Test
  public void decimalBE2LE() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
      new ArrowType.Decimal(21, 2), allocator)) {
      decimalVector.allocateNew();

      BigInteger[] testBigInts = new BigInteger[] {
          new BigInteger("0"),
          new BigInteger("-1"),
          new BigInteger("23"),
          new BigInteger("234234"),
          new BigInteger("-234234234"),
          new BigInteger("234234234234"),
          new BigInteger("-56345345345345"),
          new BigInteger("29823462983462893462934679234653456345"), // converts to 16 byte array
          new BigInteger("-3894572983475982374598324598234346536"), // converts to 16 byte array
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

      // Try inserting a buffer larger than 16bytes and expect a failure
      try {
        decimalVector.setBigEndian(insertionIdx, new byte[17]);
        fail("above statement should have failed");
      } catch (IllegalArgumentException ex) {
        assertTrue(ex.getMessage().equals("Invalid decimal value length. Valid length in [1 - 16], got 17"));
      }
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
}
