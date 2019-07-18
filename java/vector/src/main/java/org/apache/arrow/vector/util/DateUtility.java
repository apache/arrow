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

package org.apache.arrow.vector.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

/** Utility class for Date, DateTime, TimeStamp, Interval data types. */
public class DateUtility {
  private DateUtility() {}

  private static final String UTC = "UTC";

  public static final DateTimeFormatter formatDate = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  public static final DateTimeFormatter formatTimeStampMilli = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
  public static final DateTimeFormatter formatTimeStampTZ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
  public static final DateTimeFormatter formatTime = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

  public static DateTimeFormatter dateTimeTZFormat = null;
  public static DateTimeFormatter timeFormat = null;

  public static final int yearsToMonths = 12;
  public static final int hoursToMillis = 60 * 60 * 1000;
  public static final int minutesToMillis = 60 * 1000;
  public static final int secondsToMillis = 1000;
  public static final int monthToStandardDays = 30;
  public static final long monthsToMillis = 2592000000L; // 30 * 24 * 60 * 60 * 1000
  public static final int daysToStandardMillis = 24 * 60 * 60 * 1000;

  /** Returns the date time formatter used to parse date strings. */
  public static DateTimeFormatter getDateTimeFormatter() {

    if (dateTimeTZFormat == null) {
      DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
      DateTimeFormatter optionalTime = DateTimeFormatter.ofPattern(" HH:mm:ss");
      DateTimeFormatter optionalSec = DateTimeFormatter.ofPattern(".SSS");
      DateTimeFormatter optionalZone = DateTimeFormatter.ofPattern(" ZZZ");

      dateTimeTZFormat = new DateTimeFormatterBuilder().append(dateFormatter).appendOptional(optionalTime)
        .appendOptional(optionalSec).appendOptional(optionalZone).toFormatter();
    }

    return dateTimeTZFormat;
  }

  /** Returns time formatter used to parse time strings. */
  public static DateTimeFormatter getTimeFormatter() {
    if (timeFormat == null) {
      DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
      DateTimeFormatter optionalSec = DateTimeFormatter.ofPattern(".SSS");
      timeFormat = new DateTimeFormatterBuilder().append(timeFormatter).appendOptional(optionalSec).toFormatter();
    }
    return timeFormat;
  }

  /**
   * Convert milliseconds from epoch to a LocalDateTime with timeZone offset.
   *
   * @param epochMillis milliseconds from epoch
   * @param timeZone current timeZone
   * @return LocalDateTime object with timeZone offset
   */
  public static LocalDateTime getLocalDateTimeFromEpochMilli(long epochMillis, String timeZone) {
    final LocalDateTime localDateTime = LocalDateTime.ofInstant(
         Instant.ofEpochMilli(epochMillis), TimeZone.getTimeZone(timeZone).toZoneId());
    return localDateTime;
  }

  /**
   * Convert milliseconds from epoch to a LocalDateTime with UTC offset.
   */
  public static LocalDateTime getLocalDateTimeFromEpochMilli(long epochMillis) {
    return getLocalDateTimeFromEpochMilli(epochMillis, UTC);
  }

  /**
   * Convert microseconds from epoch to a LocalDateTime with timeZone offset.
   *
   * @param epochMicros microseconds from epoch
   * @param timeZone current timeZone
   * @return LocalDateTime object with timeZone offset
   */
  public static LocalDateTime getLocalDateTimeFromEpochMicro(long epochMicros, String timeZone) {
    final long millis = java.util.concurrent.TimeUnit.MICROSECONDS.toMillis(epochMicros);
    final long addl_micros = epochMicros - (millis * 1000);
    return DateUtility.getLocalDateTimeFromEpochMilli(millis, timeZone).plus(addl_micros, ChronoUnit.MICROS);
  }

  /**
   * Convert microseconds from epoch to a LocalDateTime with UTC offset.
   */
  public static LocalDateTime getLocalDateTimeFromEpochMicro(long epochMicros) {
    return getLocalDateTimeFromEpochMicro(epochMicros, UTC);
  }

  /**
   * Convert nanoseconds from epoch to a LocalDateTime with timeZone offset.
   *
   * @param epochNanos nanoseconds from epoch
   * @param timeZone current timeZone
   * @return LocalDateTime object with timeZone offset
   */
  public static LocalDateTime getLocalDateTimeFromEpochNano(long epochNanos, String timeZone) {
    final long millis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(epochNanos);
    final long addl_nanos = epochNanos - (millis * 1000 * 1000);
    return DateUtility.getLocalDateTimeFromEpochMilli(millis, timeZone).plusNanos(addl_nanos);
  }

  /**
   * Convert nanoseconds from epoch to a LocalDateTime with UTC offset.
   */
  public static LocalDateTime getLocalDateTimeFromEpochNano(long epochNanos) {
    return getLocalDateTimeFromEpochNano(epochNanos, UTC);
  }
}
