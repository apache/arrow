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
import org.apache.arrow.vector.Float4Vector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcFloat4VectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private Float4Vector vector;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloat4VectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcFloat4VectorAccessor((Float4Vector) vector, getCurrentRow);

  @Before
  public void setup() {
    this.vector = rootAllocatorTestRule.createFloat4Vector();
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void testShouldGetFloatMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          float floatValue = accessor.getFloat();

          collector.checkThat(floatValue, is(vector.get(currentRow)));
        });
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          float floatValue = accessor.getFloat();
          Object object = accessor.getObject();

          collector.checkThat(object, instanceOf(Float.class));
          collector.checkThat(object, is(floatValue));
        });
  }

  @Test
  public void testShouldGetStringMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getString(), is(Float.toString(accessor.getFloat())));
        });
  }

  @Test
  public void testShouldGetBooleanMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getBoolean(), is(accessor.getFloat() != 0.0));
        });
  }

  @Test
  public void testShouldGetByteMethodFromFloatingPointVector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getByte(), is((byte) accessor.getFloat()));
        });
  }

  @Test
  public void testShouldGetShortMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getShort(), is((short) accessor.getFloat()));
        });
  }

  @Test
  public void testShouldGetIntMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getInt(), is((int) accessor.getFloat()));
        });
  }

  @Test
  public void testShouldGetLongMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getLong(), is((long) accessor.getFloat()));
        });
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getFloat(), is(accessor.getFloat()));
        });
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          float value = accessor.getFloat();
          if (Double.isInfinite(value)) {
            // BigDecimal does not support Infinities
            return;
          }
          collector.checkThat(accessor.getBigDecimal(), is(BigDecimal.valueOf(value)));
        });
  }

  @Test
  public void testShouldConvertToByteMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float firstValue = accessor.getFloat();
          final byte secondValue = accessor.getByte();

          collector.checkThat(secondValue, is((byte) firstValue));
        });
  }

  @Test
  public void testShouldConvertToShortMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float firstValue = accessor.getFloat();
          final short secondValue = accessor.getShort();

          collector.checkThat(secondValue, is((short) firstValue));
        });
  }

  @Test
  public void testShouldConvertToIntegerMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float firstValue = accessor.getFloat();
          final int secondValue = accessor.getInt();

          collector.checkThat(secondValue, is((int) firstValue));
        });
  }

  @Test
  public void testShouldConvertToLongMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float firstValue = accessor.getFloat();
          final long secondValue = accessor.getLong();

          collector.checkThat(secondValue, is((long) firstValue));
        });
  }

  @Test
  public void testShouldConvertToFloatMethodFromFloat4Vector() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final float firstValue = accessor.getFloat();
          final double secondValue = accessor.getDouble();

          collector.checkThat(firstValue, is((float) secondValue));
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
}
