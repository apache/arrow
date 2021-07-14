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
import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.TimeStampVector;
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
public class ArrowFlightJdbcTimeStampVectorAccessorTest {

  public static final String AMERICA_VANCOUVER = "America/Vancouver";
  public static final String ASIA_BANGKOK = "Asia/Bangkok";
  public static final String AMERICA_SAO_PAULO = "America/Sao_Paulo";

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();
  private final String timeZone;

  private TimeStampVector vector;
  private final Supplier<TimeStampVector> vectorSupplier;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcTimeStampVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcTimeStampVectorAccessor((TimeStampVector) vector, getCurrentRow);

  @Parameterized.Parameters(name = "{1} - TimeZone: {2}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampNanoVector(),
            "TimeStampNanoVector",
            null},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampNanoTZVector("UTC"),
            "TimeStampNanoTZVector",
            "UTC"},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampNanoTZVector(AMERICA_VANCOUVER),
            "TimeStampNanoTZVector",
            AMERICA_VANCOUVER},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampNanoTZVector(ASIA_BANGKOK),
            "TimeStampNanoTZVector",
            ASIA_BANGKOK},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMicroVector(),
            "TimeStampMicroVector",
            null},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMicroTZVector("UTC"),
            "TimeStampMicroTZVector",
            "UTC"},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMicroTZVector(AMERICA_VANCOUVER),
            "TimeStampMicroTZVector",
            AMERICA_VANCOUVER},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMicroTZVector(ASIA_BANGKOK),
            "TimeStampMicroTZVector",
            ASIA_BANGKOK},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMilliVector(),
            "TimeStampMilliVector",
            null},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMilliTZVector("UTC"),
            "TimeStampMilliTZVector",
            "UTC"},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMilliTZVector(AMERICA_VANCOUVER),
            "TimeStampMilliTZVector",
            AMERICA_VANCOUVER},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampMilliTZVector(ASIA_BANGKOK),
            "TimeStampMilliTZVector",
            ASIA_BANGKOK},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampSecVector(),
            "TimeStampSecVector",
            null},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampSecTZVector("UTC"),
            "TimeStampSecTZVector",
            "UTC"},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampSecTZVector(AMERICA_VANCOUVER),
            "TimeStampSecTZVector",
            AMERICA_VANCOUVER},
        {(Supplier<TimeStampVector>) () -> rootAllocatorTestRule.createTimeStampSecTZVector(ASIA_BANGKOK),
            "TimeStampSecTZVector",
            ASIA_BANGKOK}
    });
  }

  public ArrowFlightJdbcTimeStampVectorAccessorTest(Supplier<TimeStampVector> vectorSupplier, String vectorType,
                                                    String timeZone) {
    this.vectorSupplier = vectorSupplier;
    this.timeZone = timeZone;
  }

  @Before
  public void setup() {
    this.vector = vectorSupplier.get();
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void getTimestampWithoutCalendar() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Timestamp expectedTimestamp = getTimestampForVector(currentRow);
          final Timestamp result = accessor.getTimestamp(null);

          collector.checkThat(result, is(expectedTimestamp));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getTimestampWithCalendar() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Timestamp resultWithoutCalendar = accessor.getTimestamp(null);
          final Timestamp result = accessor.getTimestamp(calendar);

          long offset = timeZone.getOffset(resultWithoutCalendar.getTime()) -
              timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          collector.checkThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getTimestampForNull() throws Exception {
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getTimestamp(null), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getDateWithoutCalendar() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Timestamp expectedTimestamp = getTimestampForVector(currentRow);
          final Date result = accessor.getDate(null);

          collector.checkThat(result, is(new Date(expectedTimestamp.getTime())));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getDateWithCalendar() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Date resultWithoutCalendar = accessor.getDate(null);
          final Date result = accessor.getDate(calendar);

          long offset = timeZone.getOffset(resultWithoutCalendar.getTime()) -
              timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          collector.checkThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getDateForNull() throws Exception {
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getDate(null), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void getTimeWithoutCalendar() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Timestamp expectedTimestamp = getTimestampForVector(currentRow);
          final Time result = accessor.getTime(null);

          collector.checkThat(result, is(new Time(expectedTimestamp.getTime())));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getTimeWithCalendar() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_SAO_PAULO);
    Calendar calendar = Calendar.getInstance(timeZone);

    TimeZone timeZoneForVector = getTimeZoneForVector(vector);

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          final Time resultWithoutCalendar = accessor.getTime(null);
          final Time result = accessor.getTime(calendar);

          long offset = timeZone.getOffset(resultWithoutCalendar.getTime()) -
              timeZoneForVector.getOffset(resultWithoutCalendar.getTime());

          collector.checkThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getTimeForNull() throws Exception {
    vector.setNull(0);
    ArrowFlightJdbcTimeStampVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getTime(null), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  private Timestamp getTimestampForVector(int currentRow) {
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

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(Timestamp.class));
        });
  }
}
