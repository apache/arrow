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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.util.DecimalUtility;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertEquals;

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

  @Test
  public void test() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    NullableDecimalVector decimalVector = new NullableDecimalVector("decimal", allocator, null, 10, scale);
    decimalVector.allocateNew();
    BigDecimal[] values = new BigDecimal[intValues.length];
    for (int i = 0; i < intValues.length; i++) {
      BigDecimal decimal = new BigDecimal(BigInteger.valueOf(intValues[i]), scale);
      values[i] = decimal;
      decimalVector.getMutator().setIndexDefined(i);
      DecimalUtility.writeBigDecimalToArrowBuf(decimal, decimalVector.getBuffer(), i);
    }

    decimalVector.getMutator().setValueCount(intValues.length);

    for (int i = 0; i < intValues.length; i++) {
      BigDecimal value = decimalVector.getAccessor().getObject(i);
      assertEquals(values[i], value);
    }
  }
}
