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
import org.junit.rules.ExpectedException;

public class ArrowFlightJdbcFloat4VectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Rule
  public ExpectedException exceptionCollector = ExpectedException.none();

  private Float4Vector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloat4VectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcFloat4VectorAccessor((Float4Vector) vector, getCurrentRow);

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcFloat4VectorAccessor> accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

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
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getFloat,
        (accessor, currentRow) -> is(vector.get(currentRow)));
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getObject,
        (accessor) -> is(accessor.getFloat()));
  }

  @Test
  public void testShouldGetBytesMethodFromFloat4Vector() throws Exception {
    try (Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setSafe(0, (float) 0x1.6f4f97c2d4d15p-3);
      float4Vector.setValueCount(1);

      byte[] value = new byte[] {0x3e, 0x37, (byte) 0xa7, (byte) 0xcc};

      accessorIterator.assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getBytes,
          CoreMatchers.is(value));
    }
  }

  @Test
  public void testShouldGetStringMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getString,
        accessor -> is(Float.toString(accessor.getFloat())));
  }

  @Test
  public void testShouldGetStringMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getString,
          CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetBytesMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator
          .assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getBytes, CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetFloatMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getFloat, is(0.0f));
    }
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getBigDecimal,
          CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetObjectMethodFromFloat4VectorWithNull() throws Exception {
    try (final Float4Vector float4Vector = new Float4Vector("ID", rootAllocatorTestRule.getRootAllocator())) {
      float4Vector.setNull(0);
      float4Vector.setValueCount(1);

      accessorIterator.assertAccessorGetter(float4Vector, ArrowFlightJdbcFloat4VectorAccessor::getObject,
          CoreMatchers.nullValue());
    }
  }

  @Test
  public void testShouldGetBooleanMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getBoolean,
        accessor -> is(accessor.getFloat() != 0.0f));
  }

  @Test
  public void testShouldGetByteMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getByte,
        accessor -> is((byte) accessor.getFloat()));
  }

  @Test
  public void testShouldGetShortMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getShort,
        accessor -> is((short) accessor.getFloat()));
  }

  @Test
  public void testShouldGetIntMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getInt,
        accessor -> is((int) accessor.getFloat()));
  }

  @Test
  public void testShouldGetLongMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getLong,
        accessor -> is((long) accessor.getFloat()));
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getDouble,
        accessor -> is((double) accessor.getFloat()));
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat4Vector() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      float value = accessor.getFloat();
      if (Double.isInfinite(value)) {
        exceptionCollector.expect(UnsupportedOperationException.class);
      }
      collector.checkThat(accessor.getBigDecimal(), is(BigDecimal.valueOf(value)));
    });
  }

  @Test
  public void testShouldConvertToIntegerViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Integer.class),
        accessor -> is(accessor.getInt()));
  }

  @Test
  public void testShouldConvertToShortViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Short.class),
        accessor -> is(accessor.getShort()));
  }

  @Test
  public void testShouldConvertToByteViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Byte.class),
        accessor -> is(accessor.getByte()));
  }

  @Test
  public void testShouldConvertToLongViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Long.class),
        accessor -> is(accessor.getLong()));
  }

  @Test
  public void testShouldConvertToFloatViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Float.class),
        accessor -> is(accessor.getFloat()));
  }

  @Test
  public void testShouldConvertToDoubleViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Double.class),
        accessor -> is(accessor.getDouble()));
  }

  @Test
  public void testShouldConvertToBigDecimalViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
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
  public void testShouldConvertToBooleanViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(Boolean.class),
        accessor -> is(accessor.getBoolean()));
  }

  @Test
  public void testShouldConvertToStringViaGetObjectMethodFromFloat4Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getObject(String.class),
        accessor -> is(accessor.getString()));
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat4VectorAccessor::getObjectClass,
        accessor -> equalTo(Float.class));
  }
}
