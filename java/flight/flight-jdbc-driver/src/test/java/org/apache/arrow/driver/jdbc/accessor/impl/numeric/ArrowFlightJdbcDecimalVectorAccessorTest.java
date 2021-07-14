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

package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.DecimalVector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcDecimalVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private DecimalVector vector;
  private DecimalVector vectorWithNull;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcDecimalVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcDecimalVectorAccessor((DecimalVector) vector, getCurrentRow);

  @Before
  public void setup() {
    this.vector = rootAllocatorTestRule.createDecimalVector();
    this.vectorWithNull = rootAllocatorTestRule.createDecimalVectorForNullTests();
  }

  @After
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  @Test
  public void testShouldGetBigDecimalFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetDoubleMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final double secondResult = accessor.getDouble();

          collector.checkThat(secondResult, equalTo(result.doubleValue()));
        });
  }

  @Test
  public void testShouldGetFloatMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final float secondResult = accessor.getFloat();

          collector.checkThat(secondResult, equalTo(result.floatValue()));
        });
  }

  @Test
  public void testShouldGetLongMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final long secondResult = accessor.getLong();

          collector.checkThat(secondResult, equalTo(result.longValue()));
        });
  }

  @Test
  public void testShouldGetIntMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final int secondResult = accessor.getInt();

          collector.checkThat(secondResult, equalTo(result.intValue()));
        });
  }

  @Test
  public void testShouldGetShortMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final short secondResult = accessor.getShort();

          collector.checkThat(secondResult, equalTo(result.shortValue()));
        });
  }

  @Test
  public void testShouldGetByteMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final byte secondResult = accessor.getByte();

          collector.checkThat(secondResult, equalTo(result.byteValue()));
        });
  }

  @Test
  public void testShouldGetStringMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final String secondResult = accessor.getString();

          collector.checkThat(secondResult, equalTo(String.valueOf(result)));
        });
  }

  @Test
  public void testShouldGetBooleanMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final boolean secondResult = accessor.getBoolean();

          collector.checkThat(secondResult, equalTo(!result.equals(BigDecimal.ZERO)));
        });
  }

  @Test
  public void testShouldGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getBigDecimal();
          final Object secondResult = accessor.getObject();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToIntegerViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final int result = accessor.getObject(Integer.class);
          final int secondResult = accessor.getInt();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToShortViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final short result = accessor.getObject(Short.class);
          final short secondResult = accessor.getShort();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToByteViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final byte result = accessor.getObject(Byte.class);
          final byte secondResult = accessor.getByte();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToLongViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getObject(Long.class);
          final long secondResult = accessor.getLong();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToFloatViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float result = accessor.getObject(Float.class);
          final float secondResult = accessor.getFloat();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToDoubleViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double result = accessor.getObject(Double.class);
          final double secondResult = accessor.getDouble();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToBigDecimalViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getObject(BigDecimal.class);
          final BigDecimal secondResult = accessor.getBigDecimal();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToBigDecimalWithScaleViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final BigDecimal result = accessor.getObject(BigDecimal.class);
          final BigDecimal secondResult = accessor.getBigDecimal(2);

          collector.checkThat(secondResult, equalTo(result.setScale(2, RoundingMode.UNNECESSARY)));
        });
  }

  @Test
  public void testShouldConvertToBooleanViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Boolean result = accessor.getObject(Boolean.class);
          final Boolean secondResult = accessor.getBoolean();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldConvertToStringViaGetObjectMethodFromDecimalVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final String result = accessor.getObject(String.class);
          final String secondResult = accessor.getString();

          collector.checkThat(secondResult, equalTo(result));
        });
  }

  @Test
  public void testShouldGetBigDecimalMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBigDecimal(), CoreMatchers.nullValue());
        });
  }

  @Test
  public void testShouldGetObjectMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getObject(), CoreMatchers.nullValue());
        });
  }

  @Test
  public void testShouldGetBytesMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.nullValue());
        });
  }

  @Test
  public void testShouldGetStringMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getString(), CoreMatchers.nullValue());
        });
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(BigDecimal.class));
        });
  }


  @Test
  public void testShouldGetByteMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getByte(), is((byte) 0));
        });
  }

  @Test
  public void testShouldGetShortMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getShort(), is((short) 0));
        });
  }

  @Test
  public void testShouldGetIntMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getInt(), is(0));
        });
  }

  @Test
  public void testShouldGetLongMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getLong(), is((long) 0));
        });
  }

  @Test
  public void testShouldGetFloatMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getFloat(), is((float) 0));
        });
  }

  @Test
  public void testShouldGetDoubleMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getDouble(), is((double) 0));
        });
  }

  @Test
  public void testShouldGetBooleanMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBoolean(), is(false));
        });
  }

  @Test
  public void testShouldGetBigDecimalWithScaleMethodFromDecimalVectorWithNull() throws Exception {
    iterateOnAccessor(vectorWithNull, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBigDecimal(2), CoreMatchers.nullValue());
        });
  }
}
