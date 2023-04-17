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
import static org.hamcrest.CoreMatchers.is;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.Float8Vector;
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


  private Float8Vector vector;
  private Float8Vector vectorWithNull;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcFloat8VectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> new ArrowFlightJdbcFloat8VectorAccessor((Float8Vector) vector,
              getCurrentRow, (boolean wasNull) -> {
          });

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcFloat8VectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Before
  public void setup() {
    this.vector = rootAllocatorTestRule.createFloat8Vector();
    this.vectorWithNull = rootAllocatorTestRule.createFloat8VectorForNullTests();
  }

  @After
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  @Test
  public void testShouldGetDoubleMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getDouble,
        (accessor, currentRow) -> is(vector.getValueAsDouble(currentRow)));
  }

  @Test
  public void testShouldGetObjectMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getObject,
        (accessor) -> is(accessor.getDouble()));
  }

  @Test
  public void testShouldGetStringMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getString,
        (accessor) -> is(Double.toString(accessor.getDouble())));
  }

  @Test
  public void testShouldGetBooleanMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getBoolean,
        (accessor) -> is(accessor.getDouble() != 0.0));
  }

  @Test
  public void testShouldGetByteMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getByte,
        (accessor) -> is((byte) accessor.getDouble()));
  }

  @Test
  public void testShouldGetShortMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getShort,
        (accessor) -> is((short) accessor.getDouble()));
  }

  @Test
  public void testShouldGetIntMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getInt,
        (accessor) -> is((int) accessor.getDouble()));
  }

  @Test
  public void testShouldGetLongMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getLong,
        (accessor) -> is((long) accessor.getDouble()));
  }

  @Test
  public void testShouldGetFloatMethodFromFloat8Vector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getFloat,
        (accessor) -> is((float) accessor.getDouble()));
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat8Vector() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      double value = accessor.getDouble();
      if (Double.isInfinite(value) || Double.isNaN(value)) {
        exceptionCollector.expect(SQLException.class);
      }
      collector.checkThat(accessor.getBigDecimal(), is(BigDecimal.valueOf(value)));
    });
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator
        .assertAccessorGetter(vector, ArrowFlightJdbcFloat8VectorAccessor::getObjectClass,
            equalTo(Double.class));
  }

  @Test
  public void testShouldGetStringMethodFromFloat8VectorWithNull() throws Exception {
    accessorIterator
        .assertAccessorGetter(vectorWithNull, ArrowFlightJdbcFloat8VectorAccessor::getString,
            CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetFloatMethodFromFloat8VectorWithNull() throws Exception {
    accessorIterator
        .assertAccessorGetter(vectorWithNull, ArrowFlightJdbcFloat8VectorAccessor::getFloat,
            is(0.0f));
  }

  @Test
  public void testShouldGetBigDecimalMethodFromFloat8VectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcFloat8VectorAccessor::getBigDecimal,
        CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetBigDecimalWithScaleMethodFromFloat4Vector() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      double value = accessor.getDouble();
      if (Double.isInfinite(value) || Double.isNaN(value)) {
        exceptionCollector.expect(SQLException.class);
      }
      collector.checkThat(accessor.getBigDecimal(9),
          is(BigDecimal.valueOf(value).setScale(9, RoundingMode.HALF_UP)));
    });
  }

  @Test
  public void testShouldGetObjectMethodFromFloat8VectorWithNull() throws Exception {
    accessorIterator
        .assertAccessorGetter(vectorWithNull, ArrowFlightJdbcFloat8VectorAccessor::getObject,
            CoreMatchers.nullValue());
  }
}
