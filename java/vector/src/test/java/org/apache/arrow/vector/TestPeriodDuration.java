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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.Duration;
import java.time.Period;

import org.junit.Test;

public class TestPeriodDuration {

  @Test
  public void testBasics() {
    PeriodDuration pd1 = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(123));
    PeriodDuration pdEq1 = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(123));
    PeriodDuration pd2 = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(12));
    PeriodDuration pd3 = new PeriodDuration(Period.of(-1, -2, -3), Duration.ofNanos(-123));

    assertEquals(pd1, pdEq1);
    assertEquals(pd1.hashCode(), pdEq1.hashCode());

    assertNotEquals(pd1, pd2);
    assertNotEquals(pd1.hashCode(), pd2.hashCode());
    assertNotEquals(pd1, pd3);
    assertNotEquals(pd1.hashCode(), pd3.hashCode());
  }

  @Test
  public void testToISO8601IntervalString() {
    assertEquals("P0D",
            new PeriodDuration(Period.ZERO, Duration.ZERO).toISO8601IntervalString());
    assertEquals("P1Y2M3D",
            new PeriodDuration(Period.of(1, 2, 3), Duration.ZERO).toISO8601IntervalString());
    assertEquals("PT0.000000123S",
            new PeriodDuration(Period.ZERO, Duration.ofNanos(123)).toISO8601IntervalString());
    assertEquals("PT1.000000123S",
            new PeriodDuration(Period.ZERO, Duration.ofSeconds(1).withNanos(123)).toISO8601IntervalString());
    assertEquals("PT1H1.000000123S",
            new PeriodDuration(Period.ZERO, Duration.ofSeconds(3601).withNanos(123)).toISO8601IntervalString());
    assertEquals("PT24H1M1.000000123S",
            new PeriodDuration(Period.ZERO, Duration.ofSeconds(86461).withNanos(123)).toISO8601IntervalString());
    assertEquals("P1Y2M3DT24H1M1.000000123S",
            new PeriodDuration(Period.of(1, 2, 3), Duration.ofSeconds(86461).withNanos(123)).toISO8601IntervalString());

    assertEquals("P-1Y-2M-3D",
            new PeriodDuration(Period.of(-1, -2, -3), Duration.ZERO).toISO8601IntervalString());
    assertEquals("PT-0.000000123S",
            new PeriodDuration(Period.ZERO, Duration.ofNanos(-123)).toISO8601IntervalString());
    assertEquals("PT-24H-1M-0.999999877S",
            new PeriodDuration(Period.ZERO, Duration.ofSeconds(-86461).withNanos(123)).toISO8601IntervalString());
    assertEquals("P-1Y-2M-3DT-0.999999877S",
            new PeriodDuration(Period.of(-1, -2, -3), Duration.ofSeconds(-1).withNanos(123)).toISO8601IntervalString());
  }
}
