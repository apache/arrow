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
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcDecimalVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private ValueVector vector;
  private ValueVector vectorWithNull;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcDecimalVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer =
                (boolean wasNull) -> {};
            if (vector instanceof DecimalVector) {
              return new ArrowFlightJdbcDecimalVectorAccessor(
                  (DecimalVector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof Decimal256Vector) {
              return new ArrowFlightJdbcDecimalVectorAccessor(
                  (Decimal256Vector) vector, getCurrentRow, noOpWasNullConsumer);
            }
            return null;
          };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcDecimalVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createDecimalVector(),
            "DecimalVector"),
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createDecimal256Vector(),
            "Decimal256Vector"));
  }

  public void setup(Supplier<ValueVector> vectorSupplier) {
    this.vector = vectorSupplier.get();

    this.vectorWithNull = vectorSupplier.get();
    this.vectorWithNull.clear();
    this.vectorWithNull.setValueCount(5);
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBigDecimalFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getBigDecimal,
        (accessor, currentRow) -> CoreMatchers.notNullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDoubleMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getDouble,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().doubleValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetFloatMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getFloat,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().floatValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetLongMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getLong,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().longValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetIntMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getInt,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().intValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetShortMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getShort,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().shortValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetByteMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getByte,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().byteValue()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getString,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal().toString()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBooleanMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getBoolean,
        (accessor, currentRow) -> equalTo(!accessor.getBigDecimal().equals(BigDecimal.ZERO)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectMethodFromDecimalVector(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(accessor.getBigDecimal()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClass(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcDecimalVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(BigDecimal.class));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBigDecimalMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getBigDecimal,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getObject,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getString,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetByteMethodFromDecimalVectorWithNull(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getByte,
        (accessor, currentRow) -> is((byte) 0));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetShortMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getShort,
        (accessor, currentRow) -> is((short) 0));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetIntMethodFromDecimalVectorWithNull(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getInt,
        (accessor, currentRow) -> is(0));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetLongMethodFromDecimalVectorWithNull(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getLong,
        (accessor, currentRow) -> is((long) 0));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetFloatMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getFloat,
        (accessor, currentRow) -> is(0.0f));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDoubleMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getDouble,
        (accessor, currentRow) -> is(0.0D));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBooleanMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        ArrowFlightJdbcDecimalVectorAccessor::getBoolean,
        (accessor, currentRow) -> is(false));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBigDecimalWithScaleMethodFromDecimalVectorWithNull(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vectorWithNull,
        accessor -> accessor.getBigDecimal(2),
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }
}
