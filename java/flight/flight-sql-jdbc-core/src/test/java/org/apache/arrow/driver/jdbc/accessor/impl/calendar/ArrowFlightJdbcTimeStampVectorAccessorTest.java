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

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorAccessor.getTimeUnitForVector;
import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeStampVectorAccessor.getTimeZoneForVector;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Date;
import java.sql.Time;
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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcTimeStampVectorAccessorTest {

  public static final String AMERICA_VANCOUVER = "America/Vancouver";
  public static final String ASIA_BANGKOK = "Asia/Bangkok";
  public static final String AMERICA_SAO_PAULO = "America/Sao_Paulo";

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

  private TimeStampVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcTimeStampVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) ->
              new ArrowFlightJdbcTimeStampVectorAccessor(
                  (TimeStampVector) vector, getCurrentRow, (boolean wasNull) -> {});

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcTimeStampVectorAccessor>
      accessorIterator = new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampNanoVector(),
            "TimeStampNanoVector",
            null),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampNanoTZVector("UTC"),
            "TimeStampNanoTZVector",
            "UTC"),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampNanoTZVector(AMERICA_VANCOUVER),
            "TimeStampNanoTZVector",
            AMERICA_VANCOUVER),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampNanoTZVector(ASIA_BANGKOK),
            "TimeStampNanoTZVector",
            ASIA_BANGKOK),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMicroVector(),
            "TimeStampMicroVector",
            null),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMicroTZVector("UTC"),
            "TimeStampMicroTZVector",
            "UTC"),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMicroTZVector(AMERICA_VANCOUVER),
            "TimeStampMicroTZVector",
            AMERICA_VANCOUVER),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMicroTZVector(ASIA_BANGKOK),
            "TimeStampMicroTZVector",
            ASIA_BANGKOK),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMilliVector(),
            "TimeStampMilliVector",
            null),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMilliTZVector("UTC"),
            "TimeStampMilliTZVector",
            "UTC"),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMilliTZVector(AMERICA_VANCOUVER),
            "TimeStampMilliTZVector",
            AMERICA_VANCOUVER),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampMilliTZVector(ASIA_BANGKOK),
            "TimeStampMilliTZVector",
            ASIA_BANGKOK),
        Arguments.of(
            (Supplier<TimeStampVector>) () -> rootAllocatorTestExtension.createTimeStampSecVector(),
            "TimeStampSecVector",
            null),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampSecTZVector("UTC"),
            "TimeStampSecTZVector",
            "UTC"),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampSecTZVector(AMERICA_VANCOUVER),
            "TimeStampSecTZVector",
            AMERICA_VANCOUVER),
        Arguments.of(
            (Supplier<TimeStampVector>)
                () -> rootAllocatorTestExtension.createTimeStampSecTZVector(ASIA_BANGKOK),
            "TimeStampSecTZVector",
            ASIA_BANGKOK));
  }

  public void setup(Supplier<TimeStampVector> vectorSupplier) {
    this.vector = vectorSupplier.get();
  }

  @AfterEach
  public void tearDown() {
    this.vector.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnValidTimestampWithoutCalendar(
      Supplier<TimeStampVector> vectorSupplier, String vectorType, String timeZone)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getTimestamp(null),
        (accessor, currentRow) -> is(getTimestampForVector(currentRow, timeZone)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnValidTimestampWithCalendar(
      Supplier<TimeStampVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final Timestamp resultWithoutCalendar = accessor.getTimestamp(null);
          final Timestamp result = accessor.getTimestamp(calendar);

          long offset =
              (long) timeZone.getOffset(resultWithoutCalendar.getTime())
                  - timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          assertThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          assertThat(accessor.wasNull(), is(false));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimestampReturnNull(Supplier<TimeStampVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getTimestamp(null), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnValidDateWithoutCalendar(
      Supplier<TimeStampVector> vectorSupplier, String vectorType, String timeZone)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getDate(null),
        (accessor, currentRow) ->
            is(new Date(getTimestampForVector(currentRow, timeZone).getTime())));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnValidDateWithCalendar(Supplier<TimeStampVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final Date resultWithoutCalendar = accessor.getDate(null);
          final Date result = accessor.getDate(calendar);

          long offset =
              (long) timeZone.getOffset(resultWithoutCalendar.getTime())
                  - timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          assertThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          assertThat(accessor.wasNull(), is(false));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetDateReturnNull(Supplier<TimeStampVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getDate(null), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimeReturnValidTimeWithoutCalendar(
      Supplier<TimeStampVector> vectorSupplier, String vectorType, String timeZone)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getTime(null),
        (accessor, currentRow) ->
            is(new Time(getTimestampForVector(currentRow, timeZone).getTime())));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimeReturnValidTimeWithCalendar(Supplier<TimeStampVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    accessorIterator.iterate(
        vector,
        (accessor, currentRow) -> {
          final Time resultWithoutCalendar = accessor.getTime(null);
          final Time result = accessor.getTime(calendar);

          long offset =
              (long) timeZone.getOffset(resultWithoutCalendar.getTime())
                  - timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          assertThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          assertThat(accessor.wasNull(), is(false));
        });
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetTimeReturnNull(Supplier<TimeStampVector> vectorSupplier) {
    setup(vectorSupplier);
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    assertThat(accessor.getTime(null), CoreMatchers.equalTo(null));
    assertThat(accessor.wasNull(), is(true));
  }

  private Timestamp getTimestampForVector(int currentRow, String timeZone) {
    Object object = vector.getObject(currentRow);

    Timestamp expectedTimestamp = null;
    if (object instanceof LocalDateTime) {
      expectedTimestamp = Timestamp.valueOf((LocalDateTime) object);
    } else if (object instanceof Long) {
      TimeUnit timeUnit = getTimeUnitForVector(vector);
      long millis = timeUnit.toMillis((Long) object);
      long offset = TimeZone.getTimeZone(timeZone).getOffset(millis);
      expectedTimestamp = new Timestamp(millis + offset);
    }
    return expectedTimestamp;
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClass(Supplier<TimeStampVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector, ArrowFlightJdbcTimeStampVectorAccessor::getObjectClass, equalTo(Timestamp.class));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithoutCalendar(
      Supplier<TimeStampVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    assertGetStringIsConsistentWithVarCharAccessor(null);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithCalendar(
      Supplier<TimeStampVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    // Ignore for TimeStamp vectors with TZ, as VarChar accessor won't consider their TZ
    Assumptions.assumeTrue(
        vector instanceof TimeStampNanoVector
            || vector instanceof TimeStampMicroVector
            || vector instanceof TimeStampMilliVector
            || vector instanceof TimeStampSecVector);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(AMERICA_VANCOUVER));
    assertGetStringIsConsistentWithVarCharAccessor(calendar);
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

            Timestamp timestampFromVarChar = varCharVectorAccessor.getTimestamp(calendar);
            Timestamp timestamp = accessor.getTimestamp(calendar);

            assertThat(timestamp, is(timestampFromVarChar));
            assertThat(accessor.wasNull(), is(false));
          });
    }
  }
}
