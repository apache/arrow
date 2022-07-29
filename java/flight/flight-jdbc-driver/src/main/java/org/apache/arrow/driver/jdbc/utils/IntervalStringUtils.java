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

import org.apache.arrow.vector.util.DateUtility;
import org.joda.time.Period;

/**
 * Utility class to format periods similar to Oracle's representation
 * of "INTERVAL * to *" data type.
 */
public final class IntervalStringUtils {

  /**
   * Constructor Method of class.
   */
  private IntervalStringUtils( ) {}

  /**
   * Formats a period similar to Oracle INTERVAL YEAR TO MONTH data type<br>.
   * For example, the string "+21-02" defines an interval of 21 years and 2 months.
   */
  public static String formatIntervalYear(final Period p) {
    long months = p.getYears() * (long) DateUtility.yearsToMonths + p.getMonths();
    boolean neg = false;
    if (months < 0) {
      months = -months;
      neg = true;
    }
    final int years = (int) (months / DateUtility.yearsToMonths);
    months = months % DateUtility.yearsToMonths;

    return String.format("%c%03d-%02d", neg ? '-' : '+', years, months);
  }

  /**
   * Formats a period similar to Oracle INTERVAL DAY TO SECOND data type.<br>.
   * For example, the string "-001 18:25:16.766" defines an interval of
   * - 1 day 18 hours 25 minutes 16 seconds and 766 milliseconds.
   */
  public static String formatIntervalDay(final Period p) {
    long millis = p.getDays() * (long) DateUtility.daysToStandardMillis + millisFromPeriod(p);

    boolean neg = false;
    if (millis < 0) {
      millis = -millis;
      neg = true;
    }

    final int days = (int) (millis / DateUtility.daysToStandardMillis);
    millis = millis % DateUtility.daysToStandardMillis;

    final int hours = (int) (millis / DateUtility.hoursToMillis);
    millis = millis % DateUtility.hoursToMillis;

    final int minutes = (int) (millis / DateUtility.minutesToMillis);
    millis = millis % DateUtility.minutesToMillis;

    final int seconds = (int) (millis / DateUtility.secondsToMillis);
    millis = millis % DateUtility.secondsToMillis;

    return String.format("%c%03d %02d:%02d:%02d.%03d", neg ? '-' : '+', days, hours, minutes, seconds, millis);
  }

  public static int millisFromPeriod(Period period) {
    return period.getHours() * DateUtility.hoursToMillis + period.getMinutes() * DateUtility.minutesToMillis +
        period.getSeconds() * DateUtility.secondsToMillis + period.getMillis();
  }
}
