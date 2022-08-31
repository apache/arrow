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
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowFlightJdbcDecimalVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private final Supplier<ValueVector> vectorSupplier;
  private ValueVector vector;
  private ValueVector vectorWithNull;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcDecimalVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof DecimalVector) {
          return new ArrowFlightJdbcDecimalVectorAccessor((DecimalVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof Decimal256Vector) {
          return new ArrowFlightJdbcDecimalVectorAccessor((Decimal256Vector) vector, getCurrentRow,
              noOpWasNullConsumer);
        }
        return null;
      };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcDecimalVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createDecimalVector(),
            "DecimalVector"},
        {(Supplier<ValueVector>) () -> rootAllocatorTestRule.createDecimal256Vector(),
            "Decimal256Vector"},
    });
  }

  public ArrowFlightJdbcDecimalVectorAccessorTest(Supplier<ValueVector> vectorSupplier,
                                                  String vectorType) {
    this.vectorSupplier = vectorSupplier;
  }

  @Before
  public void setup() {
    this.vector = vectorSupplier.get();

    this.vectorWithNull = vectorSupplier.get();
    this.vectorWithNull.clear();
    this.vectorWithNull.setValueCount(5);
  }

  @After
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  @Test
  public void testShouldGetBigDecimalFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        ArrowFlightJdbcDecimalVectorAccessor::getBigDecimal,
        (accessor, currentRow) -> CoreMatchers.notNullValue());
  }

  @Test
  public void testShouldGetDoubleMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getDouble,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().doubleValue()));
  }

  @Test
  public void testShouldGetFloatMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getFloat,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().floatValue()));
  }

  @Test
  public void testShouldGetLongMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getLong,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().longValue()));
  }

  @Test
  public void testShouldGetIntMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getInt,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().intValue()));
  }

  @Test
  public void testShouldGetShortMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getShort,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().shortValue()));
  }

  @Test
  public void testShouldGetByteMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getByte,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().byteValue()));
  }

  @Test
  public void testShouldGetStringMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getString,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().toString()));
  }

  @Test
  public void testShouldGetBooleanMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getBoolean,
        (accessor, currentRow) -> equalTo(!accessor.getBigDecimal().equals(BigDecimal.ZERO)));
  }

  @Test
  public void testShouldGetObjectMethodFromDecimalVector() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcDecimalVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal()));
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector,
        ArrowFlightJdbcDecimalVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(BigDecimal.class));
  }

  @Test
  public void testShouldGetBigDecimalMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getBigDecimal,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetObjectMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getObject,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetStringMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getString,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @Test
  public void testShouldGetByteMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getByte,
        (accessor, currentRow) -> is((byte) 0));
  }

  @Test
  public void testShouldGetShortMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getShort,
        (accessor, currentRow) -> is((short) 0));
  }

  @Test
  public void testShouldGetIntMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getInt,
        (accessor, currentRow) -> is(0));
  }

  @Test
  public void testShouldGetLongMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getLong,
        (accessor, currentRow) -> is((long) 0));
  }

  @Test
  public void testShouldGetFloatMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getFloat,
        (accessor, currentRow) -> is(0.0f));
  }

  @Test
  public void testShouldGetDoubleMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getDouble,
        (accessor, currentRow) -> is(0.0D));
  }

  @Test
  public void testShouldGetBooleanMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getBoolean,
        (accessor, currentRow) -> is(false));
  }

  @Test
  public void testShouldGetBigDecimalWithScaleMethodFromDecimalVectorWithNull() throws Exception {
    accessorIterator.assertAccessorGetter(vectorWithNull, accessor -> accessor.getBigDecimal(2),
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }
}
