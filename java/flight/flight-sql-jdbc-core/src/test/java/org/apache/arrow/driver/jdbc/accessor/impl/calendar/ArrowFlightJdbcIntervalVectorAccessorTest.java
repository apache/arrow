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

import static org.apache.arrow.driver.jdbc.utils.IntervalStringUtils.formatIntervalDay;
import static org.apache.arrow.driver.jdbc.utils.IntervalStringUtils.formatIntervalYear;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.ValueVector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowFlightJdbcIntervalVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule public final ErrorCollector collector = new ErrorCollector();

  private final Supplier<ValueVector> vectorSupplier;
  private ValueVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcIntervalVectorAccessor>
      accessorSupplier =
          (vector, getCurrentRow) -> {
            ArrowFlightJdbcAccessorFactory.WasNullConsumer noOpWasNullConsumer =
                (boolean wasNull) -> {};
            if (vector instanceof IntervalDayVector) {
              return new ArrowFlightJdbcIntervalVectorAccessor(
                  (IntervalDayVector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof IntervalYearVector) {
              return new ArrowFlightJdbcIntervalVectorAccessor(
                  (IntervalYearVector) vector, getCurrentRow, noOpWasNullConsumer);
            } else if (vector instanceof IntervalMonthDayNanoVector) {
              return new ArrowFlightJdbcIntervalVectorAccessor(
                  (IntervalMonthDayNanoVector) vector, getCurrentRow, noOpWasNullConsumer);
            }
            return null;
          };

  final AccessorTestUtils.AccessorIterator<ArrowFlightJdbcIntervalVectorAccessor> accessorIterator =
      new AccessorTestUtils.AccessorIterator<>(collector, accessorSupplier);

  @Parameterized.Parameters(name = "{1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            (Supplier<ValueVector>)
                () -> {
                  IntervalDayVector vector =
                      new IntervalDayVector("", rootAllocatorTestRule.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1, (i + 1) * 1000);
                  }
                  return vector;
                },
            "IntervalDayVector"
          },
          {
            (Supplier<ValueVector>)
                () -> {
                  IntervalYearVector vector =
                      new IntervalYearVector("", rootAllocatorTestRule.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1);
                  }
                  return vector;
                },
            "IntervalYearVector"
          },
          {
            (Supplier<ValueVector>)
                () -> {
                  IntervalMonthDayNanoVector vector =
                      new IntervalMonthDayNanoVector("", rootAllocatorTestRule.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1, (i + 1) * 10, (i + 1) * 100);
                  }
                  return vector;
                },
            "IntervalMonthDayNanoVector"
          },
        });
  }

  public ArrowFlightJdbcIntervalVectorAccessorTest(
      Supplier<ValueVector> vectorSupplier, String vectorType) {
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
  public void testShouldGetObjectReturnValidObject() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getObject,
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @Test
  public void testShouldGetObjectPassingObjectClassAsParameterReturnValidObject() throws Exception {
    Class<?> objectClass = getExpectedObjectClassForVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getObject(objectClass),
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @Test
  public void testShouldGetObjectReturnNull() throws Exception {
    setAllNullOnVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getObject,
        (accessor, currentRow) -> equalTo(null));
  }

  private String getStringOnVector(ValueVector vector, int index) {
    Object object = getExpectedObject(vector, index);
    if (object == null) {
      return null;
    } else if (vector instanceof IntervalDayVector) {
      return formatIntervalDay(Duration.parse(object.toString()));
    } else if (vector instanceof IntervalYearVector) {
      return formatIntervalYear(Period.parse(object.toString()));
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      String iso8601IntervalString = ((PeriodDuration) object).toISO8601IntervalString();
      String[] periodAndDuration = iso8601IntervalString.split("T");
      if (periodAndDuration.length == 1) {
        // If there is no 'T', then either Period or Duration is zero, and the other one will
        // successfully parse it
        String periodOrDuration = periodAndDuration[0];
        try {
          return new PeriodDuration(Period.parse(periodOrDuration), Duration.ZERO)
              .toISO8601IntervalString();
        } catch (DateTimeParseException e) {
          return new PeriodDuration(Period.ZERO, Duration.parse(periodOrDuration))
              .toISO8601IntervalString();
        }
      } else {
        // If there is a 'T', both Period and Duration are non-zero, and we just need to prepend the
        // 'PT' to the
        // duration for both to parse successfully
        Period parse = Period.parse(periodAndDuration[0]);
        Duration duration = Duration.parse("PT" + periodAndDuration[1]);
        return new PeriodDuration(parse, duration).toISO8601IntervalString();
      }
    }
    return null;
  }

  @Test
  public void testShouldGetIntervalYear() {
    Assert.assertEquals("-002-00", formatIntervalYear(Period.parse("P-2Y")));
    Assert.assertEquals("-001-01", formatIntervalYear(Period.parse("P-1Y-1M")));
    Assert.assertEquals("-001-02", formatIntervalYear(Period.parse("P-1Y-2M")));
    Assert.assertEquals("-002-03", formatIntervalYear(Period.parse("P-2Y-3M")));
    Assert.assertEquals("-002-04", formatIntervalYear(Period.parse("P-2Y-4M")));
    Assert.assertEquals("-011-01", formatIntervalYear(Period.parse("P-11Y-1M")));
    Assert.assertEquals("+002-00", formatIntervalYear(Period.parse("P+2Y")));
    Assert.assertEquals("+001-01", formatIntervalYear(Period.parse("P+1Y1M")));
    Assert.assertEquals("+001-02", formatIntervalYear(Period.parse("P+1Y2M")));
    Assert.assertEquals("+002-03", formatIntervalYear(Period.parse("P+2Y3M")));
    Assert.assertEquals("+002-04", formatIntervalYear(Period.parse("P+2Y4M")));
    Assert.assertEquals("+011-01", formatIntervalYear(Period.parse("P+11Y1M")));
  }

  @Test
  public void testShouldGetIntervalDay() {
    Assert.assertEquals("-001 00:00:00.000", formatIntervalDay(Duration.parse("PT-24H")));
    Assert.assertEquals("+001 00:00:00.000", formatIntervalDay(Duration.parse("PT+24H")));
    Assert.assertEquals("-000 01:00:00.000", formatIntervalDay(Duration.parse("PT-1H")));
    // "JDK-8054978: java.time.Duration.parse() fails for negative duration with 0 seconds and
    // nanos" not fixed on JDK8
    // Assert.assertEquals("-000 01:00:00.001",
    // formatIntervalDay(Duration.parse("PT-1H-0M-00.001S")));
    Assert.assertEquals(
        "-000 01:00:00.001", formatIntervalDay(Duration.ofHours(-1).minusMillis(1)));
    Assert.assertEquals("-000 01:01:01.000", formatIntervalDay(Duration.parse("PT-1H-1M-1S")));
    Assert.assertEquals("-000 02:02:02.002", formatIntervalDay(Duration.parse("PT-2H-2M-02.002S")));
    Assert.assertEquals(
        "-000 23:59:59.999", formatIntervalDay(Duration.parse("PT-23H-59M-59.999S")));
    // "JDK-8054978: java.time.Duration.parse() fails for negative duration with 0 seconds and
    // nanos" not fixed on JDK8
    // Assert.assertEquals("-000 11:59:00.100",
    // formatIntervalDay(Duration.parse("PT-11H-59M-00.100S")));
    Assert.assertEquals(
        "-000 11:59:00.100",
        formatIntervalDay(Duration.ofHours(-11).minusMinutes(59).minusMillis(100)));
    Assert.assertEquals("-000 05:02:03.000", formatIntervalDay(Duration.parse("PT-5H-2M-3S")));
    Assert.assertEquals(
        "-000 22:22:22.222", formatIntervalDay(Duration.parse("PT-22H-22M-22.222S")));
    Assert.assertEquals("+000 01:00:00.000", formatIntervalDay(Duration.parse("PT+1H")));
    Assert.assertEquals("+000 01:00:00.001", formatIntervalDay(Duration.parse("PT+1H0M00.001S")));
    Assert.assertEquals("+000 01:01:01.000", formatIntervalDay(Duration.parse("PT+1H1M1S")));
    Assert.assertEquals("+000 02:02:02.002", formatIntervalDay(Duration.parse("PT+2H2M02.002S")));
    Assert.assertEquals("+000 23:59:59.999", formatIntervalDay(Duration.parse("PT+23H59M59.999S")));
    Assert.assertEquals("+000 11:59:00.100", formatIntervalDay(Duration.parse("PT+11H59M00.100S")));
    Assert.assertEquals("+000 05:02:03.000", formatIntervalDay(Duration.parse("PT+5H2M3S")));
    Assert.assertEquals("+000 22:22:22.222", formatIntervalDay(Duration.parse("PT+22H22M22.222S")));
  }

  @Test
  public void testIntervalDayWithJodaPeriodObject() {
    Assert.assertEquals("+1567 00:00:00.000", formatIntervalDay(Duration.ofDays(1567)));
    Assert.assertEquals("-1567 00:00:00.000", formatIntervalDay(Duration.ofDays(-1567)));
  }

  @Test
  public void testShouldGetStringReturnCorrectString() throws Exception {
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> is(getStringOnVector(vector, currentRow)));
  }

  @Test
  public void testShouldGetStringReturnNull() throws Exception {
    setAllNullOnVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> equalTo(null));
  }

  @Test
  public void testShouldGetObjectClassReturnCorrectClass() throws Exception {
    Class<?> expectedObjectClass = getExpectedObjectClassForVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getObjectClass,
        (accessor, currentRow) -> equalTo(expectedObjectClass));
  }

  private Class<?> getExpectedObjectClassForVector(ValueVector vector) {
    if (vector instanceof IntervalDayVector) {
      return Duration.class;
    } else if (vector instanceof IntervalYearVector) {
      return Period.class;
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      return PeriodDuration.class;
    }
    return null;
  }

  private void setAllNullOnVector(ValueVector vector) {
    int valueCount = vector.getValueCount();
    if (vector instanceof IntervalDayVector) {
      for (int i = 0; i < valueCount; i++) {
        ((IntervalDayVector) vector).setNull(i);
      }
    } else if (vector instanceof IntervalYearVector) {
      for (int i = 0; i < valueCount; i++) {
        ((IntervalYearVector) vector).setNull(i);
      }
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      for (int i = 0; i < valueCount; i++) {
        ((IntervalMonthDayNanoVector) vector).setNull(i);
      }
    }
  }

  private Object getExpectedObject(ValueVector vector, int currentRow) {
    if (vector instanceof IntervalDayVector) {
      return Duration.ofDays(currentRow + 1).plusMillis((currentRow + 1) * 1000L);
    } else if (vector instanceof IntervalYearVector) {
      return Period.ofMonths(currentRow + 1);
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      Period period = Period.ofMonths(currentRow + 1).plusDays((currentRow + 1) * 10L);
      Duration duration = Duration.ofNanos((currentRow + 1) * 100L);
      return new PeriodDuration(period, duration);
    }
    return null;
  }
}
