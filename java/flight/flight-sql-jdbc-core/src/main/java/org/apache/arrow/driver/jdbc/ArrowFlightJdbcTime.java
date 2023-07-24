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

package org.apache.arrow.driver.jdbc;

import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import java.sql.Time;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.util.VisibleForTesting;

import com.google.common.collect.ImmutableList;

/**
 * Wrapper class for Time objects to include the milliseconds part in ISO 8601 format in this#toString.
 */
public class ArrowFlightJdbcTime extends Time {
  private static final List<String> LEADING_ZEROES = ImmutableList.of("", "0", "00");

  // Desired length of the millisecond portion should be 3
  private static final int DESIRED_MILLIS_LENGTH = 3;

  // Millis of the date time object.
  private final int millisReprValue;

  /**
   * Constructs this object based on epoch millis.
   *
   * @param milliseconds milliseconds representing Time.
   */
  public ArrowFlightJdbcTime(final long milliseconds) {
    super(milliseconds);
    millisReprValue = getMillisReprValue(milliseconds);
  }

  @VisibleForTesting
  ArrowFlightJdbcTime(final LocalTime time) {
    // Although the constructor is deprecated, this is the exact same code as Time#valueOf(LocalTime)
    super(time.getHour(), time.getMinute(), time.getSecond());
    millisReprValue = time.get(ChronoField.MILLI_OF_SECOND);
  }

  private int getMillisReprValue(long milliseconds) {
    // Extract the millisecond part from epoch nano day
    if (milliseconds >= MILLIS_PER_DAY) {
      // Convert to Epoch Day
      milliseconds %= MILLIS_PER_DAY;
    } else if (milliseconds < 0) {
      // LocalTime#ofNanoDay only accepts positive values
      milliseconds -= ((milliseconds / MILLIS_PER_DAY) - 1) * MILLIS_PER_DAY;
    }
    return LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(milliseconds))
        .get(ChronoField.MILLI_OF_SECOND);
  }

  @Override
  public String toString() {
    final StringBuilder time = new StringBuilder().append(super.toString());

    if (millisReprValue > 0) {
      final String millisString = Integer.toString(millisReprValue);

      // dot to separate the fractional seconds
      time.append(".");

      final int millisLength = millisString.length();
      if (millisLength < DESIRED_MILLIS_LENGTH) {
        // add necessary leading zeroes
        time.append(LEADING_ZEROES.get(DESIRED_MILLIS_LENGTH - millisLength));
      }
      time.append(millisString);
    }

    return time.toString();
  }

  // Spotbugs requires these methods to be overridden
  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), this.millisReprValue);
  }
}
