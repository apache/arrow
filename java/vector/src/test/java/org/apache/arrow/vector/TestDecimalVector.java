/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
    try (NullableDecimalVector decimalVector = TestUtils.newVector(NullableDecimalVector.class, "decimal", new ArrowType.Decimal(10, scale), allocator);) {

      try (NullableDecimalVector oldConstructor = new NullableDecimalVector("decimal", allocator, 10, scale);) {
        assertEquals(decimalVector.getField().getType(), oldConstructor.getField().getType());
      }

      decimalVector.allocateNew();
      BigDecimal[] values = new BigDecimal[intValues.length];
      for (int i = 0; i < intValues.length; i++) {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(intValues[i]), scale);
        values[i] = decimal;
        decimalVector.getMutator().setSafe(i, decimal);
      }

      decimalVector.getMutator().setValueCount(intValues.length);

      for (int i = 0; i < intValues.length; i++) {
        BigDecimal value = decimalVector.getAccessor().getObject(i);
        assertEquals(values[i], value);
      }
    }
  }

  @Test
  public void testBigDecimalDifferentScaleAndPrecision() {
    try (NullableDecimalVector decimalVector = TestUtils.newVector(NullableDecimalVector.class, "decimal", new ArrowType.Decimal(4, 2), allocator);) {
      decimalVector.allocateNew();

      // test BigDecimal with different scale
      boolean hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(0), 3);
        decimalVector.getMutator().setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }

      // test BigDecimal with larger precision than initialized
      hasError = false;
      try {
        BigDecimal decimal = new BigDecimal(BigInteger.valueOf(12345), 2);
        decimalVector.getMutator().setSafe(0, decimal);
      } catch (UnsupportedOperationException ue) {
        hasError = true;
      } finally {
        assertTrue(hasError);
      }
    }
  }
}
