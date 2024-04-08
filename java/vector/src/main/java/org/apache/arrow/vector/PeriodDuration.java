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

package org.apache.arrow.vector;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.time.temporal.ChronoUnit.YEARS;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.util.Preconditions;

/**
 * Combination of Period and Duration for representing this interval type
 * as a POJO.
 */
public class PeriodDuration implements TemporalAmount {

  private static final List<TemporalUnit> SUPPORTED_UNITS =
          Collections.unmodifiableList(Arrays.<TemporalUnit>asList(YEARS, MONTHS, DAYS, SECONDS, NANOS));
  private final Period period;
  private final Duration duration;

  public PeriodDuration(Period period, Duration duration) {
    this.period = Preconditions.checkNotNull(period);
    this.duration = Preconditions.checkNotNull(duration);
  }

  public Period getPeriod() {
    return period;
  }

  public Duration getDuration() {
    return duration;
  }

  @Override
  public long get(TemporalUnit unit) {
    if (unit instanceof ChronoUnit) {
      switch ((ChronoUnit) unit) {
        case YEARS:
          return period.getYears();
        case MONTHS:
          return period.getMonths();
        case DAYS:
          return period.getDays();
        case SECONDS:
          return duration.getSeconds();
        case NANOS:
          return duration.getNano();
        default:
          break;
      }
    }
    throw new UnsupportedTemporalTypeException("Unsupported TemporalUnit: " + unit);
  }

  @Override
  public List<TemporalUnit> getUnits() {
    return SUPPORTED_UNITS;
  }

  @Override
  public Temporal addTo(Temporal temporal) {
    return temporal.plus(period).plus(duration);
  }

  @Override
  public Temporal subtractFrom(Temporal temporal) {
    return temporal.minus(period).minus(duration);
  }

  /**
   * Format this PeriodDuration as an ISO-8601 interval.
   *
   * @return An ISO-8601 formatted string representing the interval.
   */
  public String toISO8601IntervalString() {
    if (duration.isZero()) {
      return period.toString();
    }
    String durationString = duration.toString();
    if (period.isZero()) {
      return durationString;
    }

    // Remove 'P' from duration string and concatenate to produce an ISO-8601 representation
    return period + durationString.substring(1);
  }

  @Override
  public String toString() {
    return period.toString() + " " + duration.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PeriodDuration)) {
      return false;
    }
    PeriodDuration other = (PeriodDuration) o;
    return this.period.equals(other.period) && this.duration.equals(other.duration);
  }

  @Override
  public int hashCode() {
    return this.period.hashCode() * 31 + this.duration.hashCode();
  }
}
