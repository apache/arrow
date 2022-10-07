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

package org.apache.arrow.driver.jdbc.utils;

import static org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Datetime utility functions.
 */
public class DateTimeUtils {
  private DateTimeUtils() {
    // Prevent instantiation.
  }

  /**
   * Subtracts given Calendar's TimeZone offset from epoch milliseconds.
   */
  public static long applyCalendarOffset(long milliseconds, Calendar calendar) {
    if (calendar == null) {
      calendar = Calendar.getInstance();
    }

    final TimeZone tz = calendar.getTimeZone();
    final TimeZone defaultTz = TimeZone.getDefault();

    if (tz != defaultTz) {
      milliseconds -= tz.getOffset(milliseconds) - defaultTz.getOffset(milliseconds);
    }

    return milliseconds;
  }


  /**
   * Converts Epoch millis to a {@link Timestamp} object.
   *
   * @param millisWithCalendar the Timestamp in Epoch millis
   * @return a {@link Timestamp} object representing the given Epoch millis
   */
  public static Timestamp getTimestampValue(long millisWithCalendar) {
    long milliseconds = millisWithCalendar;
    if (milliseconds < 0) {
      // LocalTime#ofNanoDay only accepts positive values
      milliseconds -= ((milliseconds / MILLIS_PER_DAY) - 1) * MILLIS_PER_DAY;
    }

    return Timestamp.valueOf(
        LocalDateTime.of(
            LocalDate.ofEpochDay(millisWithCalendar / MILLIS_PER_DAY),
            LocalTime.ofNanoOfDay(TimeUnit.MILLISECONDS.toNanos(milliseconds % MILLIS_PER_DAY)))
    );
  }
}
