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

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcTimeTest {

  @ClassRule
  public static final ErrorCollector collector = new ErrorCollector();
  final int hour = 5;
  final int minute = 6;
  final int second = 7;

  @Test
  public void testPrintingMillisNoLeadingZeroes() {
    // testing the regular case where the precision of the millisecond is 3
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(999));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    collector.checkThat(time.toString(), endsWith(".999"));
    collector.checkThat(time.getHours(), is(hour));
    collector.checkThat(time.getMinutes(), is(minute));
    collector.checkThat(time.getSeconds(), is(second));
  }

  @Test
  public void testPrintingMillisOneLeadingZeroes() {
    // test case where one leading zero needs to be added
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(99));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    collector.checkThat(time.toString(), endsWith(".099"));
    collector.checkThat(time.getHours(), is(hour));
    collector.checkThat(time.getMinutes(), is(minute));
    collector.checkThat(time.getSeconds(), is(second));
  }

  @Test
  public void testPrintingMillisTwoLeadingZeroes() {
    // test case where two leading zeroes needs to be added
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(1));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    collector.checkThat(time.toString(), endsWith(".001"));
    collector.checkThat(time.getHours(), is(hour));
    collector.checkThat(time.getMinutes(), is(minute));
    collector.checkThat(time.getSeconds(), is(second));
  }

  @Test
  public void testEquality() {
    // tests #equals and #hashCode for coverage checks
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(1));
    ArrowFlightJdbcTime time1 = new ArrowFlightJdbcTime(dateTime);
    ArrowFlightJdbcTime time2 = new ArrowFlightJdbcTime(dateTime);
    collector.checkThat(time1, is(time2));
    collector.checkThat(time1.hashCode(), is(time2.hashCode()));
  }
}
