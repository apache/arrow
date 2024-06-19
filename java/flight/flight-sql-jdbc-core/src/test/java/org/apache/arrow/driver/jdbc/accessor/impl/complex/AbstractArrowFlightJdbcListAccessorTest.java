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
package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AbstractArrowFlightJdbcListAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<AbstractArrowFlightJdbcListVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer =
                (boolean wasNull) -> {};
            if (vector instanceof ListVector) {
              return new ArrowFlightJdbcListVectorAccessor(
                  (ListVector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof LargeListVector) {
              return new ArrowFlightJdbcLargeListVectorAccessor(
                  (LargeListVector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof FixedSizeListVector) {
              return new ArrowFlightJdbcFixedSizeListVectorAccessor(
                  (FixedSizeListVector) vector, getCurrentRow, noOpWasNullConsumer);
            }
            return null;
          };

  final AccessorTestUtils.AccessorIterator<AbstractArrowFlightJdbcListVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createListVector(),
            "ListVector"),
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createLargeListVector(),
            "LargeListVector"),
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createFixedSizeListVector(),
            "FixedSizeListVector"));
  }

  public void setup(Supplier<ValueVector> vectorSupplier) {
    this.vector = vectorSupplier.get();
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClassReturnCorrectClass(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(List.class));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnValidList(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObject,
        (accessor, currentRow) ->
            equalTo(Arrays.asList(0, currentRow, currentRow * 2, currentRow * 3, currentRow * 4)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnNull(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    vector.clear();
    vector.allocateNewSafe();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(
        vector,
        AbstractArrowFlightJdbcListVectorAccessor::getObject,
        (accessor, currentRow) -> CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetArrayReturnValidArray(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          Array array = accessor.getArray();
          assert array != null;

          Object[] arrayObject = (Object[]) array.getArray();

          assertThat(
              arrayObject,
              equalTo(
                  new Object[] {0, currentRow, currentRow * 2, currentRow * 3, currentRow * 4}));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetArrayReturnNull(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    vector.clear();
    vector.allocateNewSafe();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(
        vector, AbstractArrowFlightJdbcListVectorAccessor::getArray, CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetArrayReturnValidArrayPassingOffsets(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          Array array = accessor.getArray();
          assert array != null;

          Object[] arrayObject = (Object[]) array.getArray(1, 3);

          assertThat(
              arrayObject, equalTo(new Object[] {currentRow, currentRow * 2, currentRow * 3}));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetArrayGetResultSetReturnValidResultSet(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          Array array = accessor.getArray();
          assert array != null;

          try (ResultSet rs = array.getResultSet()) {
            int count = 0;
            while (rs.next()) {
              final int value = rs.getInt(1);
              assertThat(value, equalTo(currentRow * count));
              count++;
            }
            assertThat(count, equalTo(5));
          }
        });
  }
}
