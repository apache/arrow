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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

import java.math.BigDecimal;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.FloatingPointVector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;

public class ArrowFlightJdbcFloat8VectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Rule
  public ExpectedException exceptionCollector = ExpectedException.none();


  private FloatingPointVector vector;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloat8VectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcFloat8VectorAccessor((Float8Vector) vector, getCurrentRow);

  @Before
  public void setup() {
    this.vector = rootAllocatorTestRule.createFloat8Vector();
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void testShouldGetDoubleMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double doubleValue = accessor.getDouble();

          collector.checkThat(doubleValue, is(vector.getValueAsDouble(currentRow)));
        });
  }


  @Test
  public void testShouldGetObjectMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double doubleValue = accessor.getDouble();
          Object object = accessor.getObject();

          collector.checkThat(object, instanceOf(Double.class));
          collector.checkThat(object, is(doubleValue));
        });
  }


  @Test
  public void testShouldGetStringMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getString(), is(Double.toString(accessor.getDouble())));
        });
  }


  @Test
  public void getBoolean() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBoolean(), is(accessor.getDouble() != 0.0));
        });
  }


  @Test
  public void testShouldGetByteMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getByte(), is((byte) accessor.getDouble()));
        });
  }


  @Test
  public void testShouldGetShortMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getShort(), is((short) accessor.getDouble()));
        });
  }


  @Test
  public void testShouldGetIntMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getInt(), is((int) accessor.getDouble()));
        });
  }


  @Test
  public void testShouldGetLongMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getLong(), is((long) accessor.getDouble()));
        });
  }

  @Test
  public void testShouldGetBytesMethodFloatingPointVector() throws Exception {
    Float8Vector float8Vector = new Float8Vector("ID", rootAllocatorTestRule.getRootAllocator());
    float8Vector.setSafe(0, 0x1.8965f02c82f69p-1);
    float8Vector.setValueCount(1);

    byte[] value = new byte[] {0x3f, (byte) 0xe8, (byte) 0x96, 0x5f, 0x2, (byte) 0xc8, 0x2f, 0x69};

    iterateOnAccessor(float8Vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBytes(), CoreMatchers.is(value));
        });

    float8Vector.close();
  }


  @Test
  public void testShouldGetFloatMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getFloat(), is((float) accessor.getDouble()));
        });
  }


  @Test
  public void testShouldGetBigDecimalMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          double value = accessor.getDouble();
          if (Double.isInfinite(value)) {
            // BigDecimal does not support Infinities
            return;
          }
          collector.checkThat(accessor.getBigDecimal(), is(BigDecimal.valueOf(value)));
        });
  }

  @Test
  public void testShouldConvertToByteMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double firstValue = accessor.getDouble();
          final byte secondValue = accessor.getByte();

          collector.checkThat(secondValue, is((byte) firstValue));
        });
  }

  @Test
  public void testShouldConvertToShortMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double firstValue = accessor.getDouble();
          final short secondValue = accessor.getShort();

          collector.checkThat(secondValue, is((short) firstValue));
        });
  }

  @Test
  public void testShouldConvertToIntegerMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double firstValue = accessor.getDouble();
          final int secondValue = accessor.getInt();

          collector.checkThat(secondValue, is((int) firstValue));
        });
  }

  @Test
  public void testShouldConvertToLongMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double firstValue = accessor.getDouble();
          final long secondValue = accessor.getLong();

          collector.checkThat(secondValue, is((long) firstValue));
        });
  }

  @Test
  public void testShouldConvertToFloatMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double firstValue = accessor.getDouble();
          final float secondValue = accessor.getFloat();

          collector.checkThat(secondValue, is((float) firstValue));
        });
  }

  @Test
  public void testShouldConvertToIntegerViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final int result = accessor.getObject(Integer.class);
          final int secondResult = accessor.getInt();

          collector.checkThat(result, instanceOf(int.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToShortViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final short result = accessor.getObject(Short.class);
          final short secondResult = accessor.getShort();

          collector.checkThat(result, instanceOf(short.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToByteViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final byte result = accessor.getObject(Byte.class);
          final byte secondResult = accessor.getByte();

          collector.checkThat(result, instanceOf(byte.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToLongViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final long result = accessor.getObject(Long.class);
          final long secondResult = accessor.getLong();

          collector.checkThat(result, instanceOf(long.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToFloatViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float result = accessor.getObject(Float.class);
          final float secondResult = accessor.getFloat();

          if (Float.isInfinite(result)) {
            // BigDecimal does not support Infinities
            return;
          }

          collector.checkThat(result, instanceOf(float.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToDoubleViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final double result = accessor.getObject(Double.class);
          final double secondResult = accessor.getDouble();

          if (Double.isInfinite(result)) {
            // BigDecimal does not support Infinities
            return;
          }

          collector.checkThat(result, instanceOf(double.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToBigDecimalViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          if (Double.isInfinite(accessor.getFloat())) {
            // BigDecimal does not support Infinities
            return;
          }

          final BigDecimal result = accessor.getObject(BigDecimal.class);
          final BigDecimal secondResult = accessor.getBigDecimal();

          collector.checkThat(result, instanceOf(BigDecimal.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToBooleanViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Boolean result = accessor.getObject(Boolean.class);
          final Boolean secondResult = accessor.getBoolean();

          collector.checkThat(result, instanceOf(Boolean.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldConvertToStringViaGetObjectMethodFromBaseIntVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final String result = accessor.getObject(String.class);
          final String secondResult = accessor.getString();

          collector.checkThat(result, instanceOf(String.class));
          collector.checkThat(secondResult, equalTo(result));

          collector.checkThat(result, CoreMatchers.notNullValue());
        });
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(Double.class));
        });
  }
}
