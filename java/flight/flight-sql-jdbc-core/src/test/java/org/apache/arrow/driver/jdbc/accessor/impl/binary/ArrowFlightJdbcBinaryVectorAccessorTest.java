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
package org.apache.arrow.driver.jdbc.accessor.impl.binary;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.InputStream;
import java.io.Reader;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcBinaryVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBinaryVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer =
                (boolean wasNull) -> {};
            if (vector instanceof VarBinaryVector) {
              return new ArrowFlightJdbcBinaryVectorAccessor(
                  ((VarBinaryVector) vector), getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof LargeVarBinaryVector) {
              return new ArrowFlightJdbcBinaryVectorAccessor(
                  ((LargeVarBinaryVector) vector), getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof FixedSizeBinaryVector) {
              return new ArrowFlightJdbcBinaryVectorAccessor(
                  ((FixedSizeBinaryVector) vector), getCurrentRow, noOpWasNullConsumer);
            }
            return null;
          };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcBinaryVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createVarBinaryVector(),
            "VarBinaryVector"),
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createLargeVarBinaryVector(),
            "LargeVarBinaryVector"),
        Arguments.of(
            (Supplier<ValueVector>) () -> rootAllocatorTestExtension.createFixedSizeBinaryVector(),
            "FixedSizeBinaryVector"));
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
  public void testShouldGetStringReturnExpectedString(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBinaryVectorAccessor::getString,
        (accessor) -> is(new String(accessor.getBytes(), UTF_8)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringReturnNull(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    accessorIterator.assertAccessorGetter(
        vector, ArrowFlightJdbcBinaryVectorAccessor::getString, CoreMatchers.nullValue());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBytesReturnExpectedByteArray(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBinaryVectorAccessor::getBytes,
        (accessor, currentRow) -> {
          if (vector instanceof VarBinaryVector) {
            return is(((VarBinaryVector) vector).get(currentRow));
          } else if (vector instanceof LargeVarBinaryVector) {
            return is(((LargeVarBinaryVector) vector).get(currentRow));
          } else if (vector instanceof FixedSizeBinaryVector) {
            return is(((FixedSizeBinaryVector) vector).get(currentRow));
          }
          return null;
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBytesReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getBytes(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnAsGetBytes(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcBinaryVectorAccessor::getObject,
        (accessor) -> is(accessor.getBytes()));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getObject(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetUnicodeStreamReturnCorrectInputStream(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          InputStream inputStream = accessor.getUnicodeStream();
          String actualString = IOUtils.toString(inputStream, UTF_8);
          assertThat(accessor.wasNull(), is(false));
          assertThat(actualString, is(accessor.getString()));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetUnicodeStreamReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getUnicodeStream(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetAsciiStreamReturnCorrectInputStream(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          InputStream inputStream = accessor.getAsciiStream();
          String actualString = IOUtils.toString(inputStream, US_ASCII);
          assertThat(accessor.wasNull(), is(false));
          assertThat(actualString, is(accessor.getString()));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetAsciiStreamReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getAsciiStream(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBinaryStreamReturnCurrentInputStream(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          InputStream inputStream = accessor.getBinaryStream();
          String actualString = IOUtils.toString(inputStream, UTF_8);
          assertThat(accessor.wasNull(), is(false));
          assertThat(actualString, is(accessor.getString()));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetBinaryStreamReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getBinaryStream(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetCharacterStreamReturnCorrectReader(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          Reader characterStream = accessor.getCharacterStream();
          String actualString = IOUtils.toString(characterStream);
          assertThat(accessor.wasNull(), is(false));
          assertThat(actualString, is(accessor.getString()));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetCharacterStreamReturnNull(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.reset();
    vector.setValueCount(5);

    ArrowFlightJdbcBinaryVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getCharacterStream(), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }
}
