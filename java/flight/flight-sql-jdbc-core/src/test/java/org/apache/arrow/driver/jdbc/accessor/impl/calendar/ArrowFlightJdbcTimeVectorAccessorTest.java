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

import static org.apache.arrow.driver.jdbc.accessor.impl.calendar.ArrowFlightJdbcTimeVectorAccessor.getTimeUnitForVector;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.accessor.impl.text.ArrowFlightJdbcVarCharVectorAccessor;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
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
public class ArrowFlightJdbcTimeVectorAccessorTest {

  public static final String AMERICA_VANCOUVER = "America/Vancouver";

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private BaseFixedWidthVector vector;
  private final Supplier<BaseFixedWidthVector> vectorSupplier;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcTimeVectorAccessor>
      accessorSupplier = (vector, getCurrentRow) -> {
        ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer = (boolean wasNull) -> {
        };
        if (vector instanceof TimeNanoVector) {
          return new ArrowFlightJdbcTimeVectorAccessor((TimeNanoVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof TimeMicroVector) {
          return new ArrowFlightJdbcTimeVectorAccessor((TimeMicroVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof TimeMilliVector) {
          return new ArrowFlightJdbcTimeVectorAccessor((TimeMilliVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        } else if (vector instanceof TimeSecVector) {
          return new ArrowFlightJdbcTimeVectorAccessor((TimeSecVector) vector, getCurrentRow,
              noOpWasNullConsumer);
        }
        return null;
      };

  private final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcTimeVectorAccessor>
      accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {(Supplier<TimeNanoVector>) () -> rootAllocatorTestRule.createTimeNanoVector(),
            "TimeNanoVector"},
        {(Supplier<TimeMicroVector>) () -> rootAllocatorTestRule.createTimeMicroVector(),
            "TimeMicroVector"},
        {(Supplier<TimeMilliVector>) () -> rootAllocatorTestRule.createTimeMilliVector(),
            "TimeMilliVector"},
        {(Supplier<TimeSecVector>) () -> rootAllocatorTestRule.createTimeSecVector(),
            "TimeSecVector"}
    });
  }

  public ArrowFlightJdbcTimeVectorAccessorTest(Supplier<BaseFixedWidthVector> vectorSupplier,
                                               String vectorType) {
    this.vectorSupplier = vectorSupplier;
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
  public void testShouldGetTimestampReturnValidTimestampWithoutCalendar() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getTimestamp(null),
        (accessor, currentRow) -> is(getTimestampForVector(currentRow)));
  }

  @Test
  public void testShouldGetTimestampReturnValidTimestampWithCalendar() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_VANCOUVER);
    Calendar calendar = Calendar.getInstance(timeZone);

    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      final Timestamp resultWithoutCalendar = accessor.getTimestamp(null);
      final Timestamp result = accessor.getTimestamp(calendar);

      long offset = timeZone.getOffset(resultWithoutCalendar.getTime());

      collector.checkThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
      collector.checkThat(accessor.wasNull(), is(false));
    });
  }

  @Test
  public void testShouldGetTimestampReturnNull() {
    vector.setNull(0);
    ArrowFlightJdbcTimeVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getTimestamp(null), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
  }

  @Test
  public void testShouldGetTimeReturnValidTimeWithoutCalendar() throws Exception {
    accessorIterator.assertAccessorGetter(vector, accessor -> accessor.getTime(null),
        (accessor, currentRow) -> {
          Timestamp expectedTimestamp = getTimestampForVector(currentRow);
          return is(new Time(expectedTimestamp.getTime()));
        });
  }

  @Test
  public void testShouldGetTimeReturnValidTimeWithCalendar() throws Exception {
    TimeZone timeZone = TimeZone.getTimeZone(AMERICA_VANCOUVER);
    Calendar calendar = Calendar.getInstance(timeZone);

    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      final Time resultWithoutCalendar = accessor.getTime(null);
      final Time result = accessor.getTime(calendar);

      long offset = timeZone.getOffset(resultWithoutCalendar.getTime());

      collector.checkThat(resultWithoutCalendar.getTime() - result.getTime(), is(offset));
      collector.checkThat(accessor.wasNull(), is(false));
    });
  }

  @Test
  public void testShouldGetTimeReturnNull() {
    vector.setNull(0);
    ArrowFlightJdbcTimeVectorAccessor accessor = accessorSupplier.supply(vector, () -> 0);
    collector.checkThat(accessor.getTime(null), CoreMatchers.equalTo(null));
    collector.checkThat(accessor.wasNull(), is(true));
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

  @Test
  public void testShouldGetObjectClass() throws Exception {
    accessorIterator.assertAccessorGetter(vector, ArrowFlightJdbcTimeVectorAccessor::getObjectClass,
        equalTo(Time.class));
  }

  @Test
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithoutCalendar() throws Exception {
    assertGetStringIsConsistentWithVarCharAccessor(null);
  }

  @Test
  public void testShouldGetStringBeConsistentWithVarCharAccessorWithCalendar() throws Exception {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(AMERICA_VANCOUVER));
    assertGetStringIsConsistentWithVarCharAccessor(calendar);
  }

  @Test
  public void testValidateGetStringTimeZoneConsistency() throws Exception {
    accessorIterator.iterate(vector, (accessor, currentRow) -> {
      final TimeZone defaultTz = TimeZone.getDefault();
      try {
        final String string = accessor.getString(); // Should always be UTC as no calendar is provided

        // Validate with UTC
        Time time = accessor.getTime(null);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        collector.checkThat(time.toString(), is(string));

        // Validate with different TZ
        TimeZone.setDefault(TimeZone.getTimeZone(AMERICA_VANCOUVER));
        collector.checkThat(time.toString(), not(string));

        collector.checkThat(accessor.wasNull(), is(false));
      } finally {
        // Set default Tz back
        TimeZone.setDefault(defaultTz);
      }
    });
  }

  private void assertGetStringIsConsistentWithVarCharAccessor(Calendar calendar) throws Exception {
    try (VarCharVector varCharVector = new VarCharVector("",
        rootAllocatorTestRule.getRootAllocator())) {
      varCharVector.allocateNew(1);
      ArrowFlightJdbcVarCharVectorAccessor varCharVectorAccessor =
          new ArrowFlightJdbcVarCharVectorAccessor(varCharVector, () -> 0, (boolean wasNull) -> {
          });

      accessorIterator.iterate(vector, (accessor, currentRow) -> {
        final String string = accessor.getString();
        varCharVector.set(0, new Text(string));
        varCharVector.setValueCount(1);

        Time timeFromVarChar = varCharVectorAccessor.getTime(calendar);
        Time time = accessor.getTime(calendar);

        collector.checkThat(time, is(timeFromVarChar));
        collector.checkThat(accessor.wasNull(), is(false));
      });
    }
  }
}
