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
package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcDateVectorAccessor.getTimeUnitForVector;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.impl.text.ArrowFlightJdbcVarCharVectorAccessor;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcDateVectorAccessorTest {

  public static final String AMERICA_VANCOUVER = "America/Vancouver";

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private BaseFixedWidthVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcDateVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            if (vector instanceof DateDayVector) {
              return new ArrowFlightJdbcDateVectorAccessor(
                  (DateDayVector) vector, getCurrentRow, (boolean wasNull) -> {});
            } else if (vector instanceof DateMilliVector) {
              return new ArrowFlightJdbcDateVectorAccessor(
                  (DateMilliVector) vector, getCurrentRow, (boolean wasNull) -> {});
            }
            return null;
          };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcDateVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<DateDayVector>) () -> rootAllocatorTestExtension.createDateDayVector(),
            "DateDayVector"),
        Arguments.of(
            (Supplier<DateMilliVector>) () -> rootAllocatorTestExtension.createDateMilliVector(),
            "DateMilliVector"));
  }

  public void setup(Supplier<BaseFixedWidthVector> vectorSupplier) {
    this.vector = vectorSupplier.get();
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnValidTimestampWithoutCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getTimestamp(null),
        (accessor, currentRow) -> is(getTimestampForVector(currentRow)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectWithDateClassReturnValidDateWithoutCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getObject(Date.class),
        (accessor, currentRow) -> is(new Date(getTimestampForVector(currentRow).getTime())));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnValidTimestampWithCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_VANCOUVER);
    Calendar calendar = Calendar.getInstance(timeZone);

    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final Timestamp resultWithoutCalendar = accessor.getTimestamp(null);
          final Timestamp result = accessor.getTimestamp(calendar);

          long offset = timeZone.getOffset(resultWithoutCalendar.getTime());

          assertThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          assertThat(accessor.wasNull(), is(false));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnNull(Supplier<BaseFixedWidthVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.setNull(0);
    ArrowFlightJdbcDateVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getTimestamp(null), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnValidDateWithoutCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getDate(null),
        (accessor, currentRow) -> is(new Date(getTimestampForVector(currentRow).getTime())));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnValidDateWithCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_VANCOUVER);
    Calendar calendar = Calendar.getInstance(timeZone);

    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final Date resultWithoutCalendar = accessor.getDate(null);
          final Date result = accessor.getDate(calendar);

          long offset = timeZone.getOffset(resultWithoutCalendar.getTime());

          assertThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          assertThat(accessor.wasNull(), is(false));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnNull(Supplier<BaseFixedWidthVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.setNull(0);
    ArrowFlightJdbcDateVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getDate(null), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  private Timestamp getTimestampForVector(int currentRow) {
    Object object = vector.getObject(currentRow);

    Timestamp expectedTimestamp = null;
    if (object instanceof LocalDateTime) {
      expectedTimestamp = Timestamp.valueOf((LocalDateTime) object);
    } else if (object instanceof Number) {
      long value = ((Number) object).longValue();
      TimeUnit timeUnit = getTimeUnitForVector(vector);
      long millis = timeUnit.toMillis(value);
      expectedTimestamp = new Timestamp(millis);
    }
    return expectedTimestamp;
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClass(Supplier<BaseFixedWidthVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector, ArrowFlightJdbcDateVectorAccessor::getObjectClass, equalTo(Date.class));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithoutCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    assertGetStringIsConsistentWithVarCharAccessor(null);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithCalendar(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(AMERICA_VANCOUVER));
    assertGetStringIsConsistentWithVarCharAccessor(calendar);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateGetStringTimeZoneConsistency(
      Supplier<BaseFixedWidthVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final TimeZone defaultTz = TimeZone.getDefault();
          try {
            final String string =
                accessor.getString(); // Should always be UTC as no calendar is provided

            // Validate with UTC
            Date date = accessor.getDate(null);
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            assertThat(date.toString(), is(string));

            // Validate with different TZ
            TimeZone.setDefault(TimeZone.getTimeZone(AMERICA_VANCOUVER));
            assertThat(date.toString(), not(string));

            assertThat(accessor.wasNull(), is(false));
          } finally {
            // Set default Tz back
            TimeZone.setDefault(defaultTz);
          }
        });
  }

  private void assertGetStringIsConsistentWithVarCharAccessor(Calendar calendar) throws Exception {
    try (VarCharVector varCharVector =
        new VarCharVector("", rootAllocatorTestExtension.getRootAllocator())) {
      varCharVector.allocateNew(1);
      ArrowFlightJdbcVarCharVectorAccessor varCharVectorAccessor =
          new ArrowFlightJdbcVarCharVectorAccessor(varCharVector, () -> 0, (boolean wasNull) -> {});

      accessorIterator.iterate(
          vector,
          (accessor, currentRow) -> {
            final String string = accessor.getString();
            varCharVector.set(0, new Text(string));
            varCharVector.setValueCount(1);

            Date dateFromVarChar = varCharVectorAccessor.getDate(calendar);
            Date date = accessor.getDate(calendar);

            assertThat(date, is(dateFromVarChar));
            assertThat(accessor.wasNull(), is(false));
          });
    }
  }
}
