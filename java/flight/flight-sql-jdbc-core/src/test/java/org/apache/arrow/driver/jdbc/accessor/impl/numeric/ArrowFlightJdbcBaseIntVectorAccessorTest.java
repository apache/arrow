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

import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcBaseIntVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private BaseIntVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBaseIntVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer =
                (boolean wasNull) -> {};
            if (vector instanceof UInt1Vector) {
              return new ArrowFlightJdbcBaseIntVectorAccessor(
                  (UInt1Vector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof UInt2Vector) {
              return new ArrowFlightJdbcBaseIntVectorAccessor(
                  (UInt2Vector) vector, getCurrentRow, noOpWasNullConsumer);
            } else {
              if (vector instanceof UInt4Vector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (UInt4Vector) vector, getCurrentRow, noOpWasNullConsumer);
              } else if (vector instanceof UInt8Vector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (UInt8Vector) vector, getCurrentRow, noOpWasNullConsumer);
              } else if (vector instanceof TinyIntVector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (TinyIntVector) vector, getCurrentRow, noOpWasNullConsumer);
              } else if (vector instanceof SmallIntVector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (SmallIntVector) vector, getCurrentRow, noOpWasNullConsumer);
              } else if (vector instanceof IntVector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (IntVector) vector, getCurrentRow, noOpWasNullConsumer);
              } else if (vector instanceof BigIntVector) {
                return new ArrowFlightJdbcBaseIntVectorAccessor(
                    (BigIntVector) vector, getCurrentRow, noOpWasNullConsumer);
              }
            }
            throw new UnsupportedOperationException();
          };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcBaseIntVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createIntVector(),
            "IntVector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createSmallIntVector(),
            "SmallIntVector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createTinyIntVector(),
            "TinyIntVector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createBigIntVector(),
            "BigIntVector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createUInt1Vector(),
            "UInt1Vector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createUInt2Vector(),
            "UInt2Vector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createUInt4Vector(),
            "UInt4Vector"),
        Arguments.of(
            (Supplier<BaseIntVector>) () -> rootAllocatorTestExtension.createUInt8Vector(),
            "UInt8Vector"));
  }

  public void setup(Supplier<BaseIntVector> vectorSupplier) {
    this.vector = vectorSupplier.get();
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToByteMethodFromBaseIntVector(Supplier<BaseIntVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getByte,
        (accessor, currentRow) -> equalTo((byte) accessor.getLong()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToShortMethodFromBaseIntVector(
      Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getShort,
        (accessor, currentRow) -> equalTo((short) accessor.getLong()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToIntegerMethodFromBaseIntVector(
      Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getInt,
        (accessor, currentRow) -> equalTo((int) accessor.getLong()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToFloatMethodFromBaseIntVector(
      Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getFloat,
        (accessor, currentRow) -> equalTo((float) accessor.getLong()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToDoubleMethodFromBaseIntVector(
      Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getDouble,
        (accessor, currentRow) -> equalTo((double) accessor.getLong()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldConvertToBooleanMethodFromBaseIntVector(
      Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBaseIntVectorAccessor::getBoolean,
        (accessor, currentRow) -> equalTo(accessor.getLong() != 0L));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClass(Supplier<BaseIntVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector, ArrowFlightJdbcBaseIntVectorAccessor::getObjectClass, equalTo(Long.class));
  }
}
