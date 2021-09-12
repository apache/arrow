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

import java.time.Duration;
import java.time.Period;

import org.apache.arrow.util.Preconditions;

/**
 * Combination of Period and Duration for representing this interval type
 * as a POJO.
 */
public class PeriodDuration {
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
