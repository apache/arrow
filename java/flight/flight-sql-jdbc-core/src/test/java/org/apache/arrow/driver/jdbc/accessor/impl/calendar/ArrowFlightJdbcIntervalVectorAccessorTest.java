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
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.utils.RootAllocatorTestExtension;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.ValueVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ArrowFlightJdbcIntervalVectorAccessorTest {

  @RegisterExtension
  public static RootAllocatorTestExtension rootAllocatorTestExtension =
      new RootAllocatorTestExtension();

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
      new AccessorTestUtils.AccessorIterator<>(accessorSupplier);

  public static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(
            (Supplier<ValueVector>)
                () -> {
                  IntervalDayVector vector =
                      new IntervalDayVector("", rootAllocatorTestExtension.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1, (i + 1) * 1000);
                  }
                  return vector;
                },
            "IntervalDayVector"),
        Arguments.of(
            (Supplier<ValueVector>)
                () -> {
                  IntervalYearVector vector =
                      new IntervalYearVector("", rootAllocatorTestExtension.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1);
                  }
                  return vector;
                },
            "IntervalYearVector"),
        Arguments.of(
            (Supplier<ValueVector>)
                () -> {
                  IntervalMonthDayNanoVector vector =
                      new IntervalMonthDayNanoVector(
                          "", rootAllocatorTestExtension.getRootAllocator());

                  int valueCount = 10;
                  vector.setValueCount(valueCount);
                  for (int i = 0; i < valueCount; i++) {
                    vector.set(i, i + 1, (i + 1) * 10, (i + 1) * 100);
                  }
                  return vector;
                },
            "IntervalMonthDayNanoVector"));
  }

  public void setup(Supplier<ValueVector> vectorSupplier) {
    this.vector = vectorSupplier.get();
  }

  @AfterEach
  public void tearDown() {
    if (this.vector != null) {
      this.vector.close();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnValidObject(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getObject,
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectPassingObjectClassAsParameterReturnValidObject(
      Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    Class<?> objectClass = getExpectedObjectClassForVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        accessor -> accessor.getObject(objectClass),
        (accessor, currentRow) -> is(getExpectedObject(vector, currentRow)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectReturnNull(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
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

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetIntervalYear(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    assertEquals("-002-00", formatIntervalYear(Period.parse("P-2Y")));
    assertEquals("-001-01", formatIntervalYear(Period.parse("P-1Y-1M")));
    assertEquals("-001-02", formatIntervalYear(Period.parse("P-1Y-2M")));
    assertEquals("-002-03", formatIntervalYear(Period.parse("P-2Y-3M")));
    assertEquals("-002-04", formatIntervalYear(Period.parse("P-2Y-4M")));
    assertEquals("-011-01", formatIntervalYear(Period.parse("P-11Y-1M")));
    assertEquals("+002-00", formatIntervalYear(Period.parse("P+2Y")));
    assertEquals("+001-01", formatIntervalYear(Period.parse("P+1Y1M")));
    assertEquals("+001-02", formatIntervalYear(Period.parse("P+1Y2M")));
    assertEquals("+002-03", formatIntervalYear(Period.parse("P+2Y3M")));
    assertEquals("+002-04", formatIntervalYear(Period.parse("P+2Y4M")));
    assertEquals("+011-01", formatIntervalYear(Period.parse("P+11Y1M")));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetIntervalDay(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    assertEquals("-001 00:00:00.000", formatIntervalDay(Duration.parse("PT-24H")));
    assertEquals("+001 00:00:00.000", formatIntervalDay(Duration.parse("PT+24H")));
    assertEquals("-000 01:00:00.000", formatIntervalDay(Duration.parse("PT-1H")));
    // "JDK-8054978: java.time.Duration.parse() fails for negative duration with 0 seconds and
    // nanos" not fixed on JDK8
    // assertEquals("-000 01:00:00.001", formatIntervalDay(Duration.parse("PT-1H-0M-00.001S")));
    assertEquals("-000 01:00:00.001", formatIntervalDay(Duration.ofHours(-1).minusMillis(1)));
    assertEquals("-000 01:01:01.000", formatIntervalDay(Duration.parse("PT-1H-1M-1S")));
    assertEquals("-000 02:02:02.002", formatIntervalDay(Duration.parse("PT-2H-2M-02.002S")));
    assertEquals("-000 23:59:59.999", formatIntervalDay(Duration.parse("PT-23H-59M-59.999S")));
    // "JDK-8054978: java.time.Duration.parse() fails for negative duration with 0 seconds and
    // nanos" not fixed on JDK8
    // assertEquals("-000 11:59:00.100", formatIntervalDay(Duration.parse("PT-11H-59M-00.100S")));
    assertEquals(
        "-000 11:59:00.100",
        formatIntervalDay(Duration.ofHours(-11).minusMinutes(59).minusMillis(100)));
    assertEquals("-000 05:02:03.000", formatIntervalDay(Duration.parse("PT-5H-2M-3S")));
    assertEquals("-000 22:22:22.222", formatIntervalDay(Duration.parse("PT-22H-22M-22.222S")));
    assertEquals("+000 01:00:00.000", formatIntervalDay(Duration.parse("PT+1H")));
    assertEquals("+000 01:00:00.001", formatIntervalDay(Duration.parse("PT+1H0M00.001S")));
    assertEquals("+000 01:01:01.000", formatIntervalDay(Duration.parse("PT+1H1M1S")));
    assertEquals("+000 02:02:02.002", formatIntervalDay(Duration.parse("PT+2H2M02.002S")));
    assertEquals("+000 23:59:59.999", formatIntervalDay(Duration.parse("PT+23H59M59.999S")));
    assertEquals("+000 11:59:00.100", formatIntervalDay(Duration.parse("PT+11H59M00.100S")));
    assertEquals("+000 05:02:03.000", formatIntervalDay(Duration.parse("PT+5H2M3S")));
    assertEquals("+000 22:22:22.222", formatIntervalDay(Duration.parse("PT+22H22M22.222S")));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testIntervalDayWithJodaPeriodObject(Supplier<ValueVector> vectorSupplier) {
    setup(vectorSupplier);
    assertEquals("+1567 00:00:00.000", formatIntervalDay(Duration.ofDays(1567)));
    assertEquals("-1567 00:00:00.000", formatIntervalDay(Duration.ofDays(-1567)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringReturnCorrectString(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> is(getStringOnVector(vector, currentRow)));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetStringReturnNull(Supplier<ValueVector> vectorSupplier) throws Exception {
    setup(vectorSupplier);
    setAllNullOnVector(vector);
    accessorIterator.assertAccessorGetter(
        vector,
        ArrowFlightJdbcIntervalVectorAccessor::getString,
        (accessor, currentRow) -> equalTo(null));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testShouldGetObjectClassReturnCorrectClass(Supplier<ValueVector> vectorSupplier)
      throws Exception {
    setup(vectorSupplier);
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
