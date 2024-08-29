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


import java.time.Duration;
import java.time.Period;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.holders.IntervalMonthDayNanoHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIntervalMonthDayNanoVector {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBasics() {
    try (final IntervalMonthDayNanoVector vector = new IntervalMonthDayNanoVector(/*name=*/"", allocator)) {
      int valueCount = 100;
      vector.setInitialCapacity(valueCount);
      vector.allocateNew();
      NullableIntervalMonthDayNanoHolder nullableHolder = new NullableIntervalMonthDayNanoHolder();
      nullableHolder.isSet = 1;
      nullableHolder.months = 2;
      nullableHolder.days = 20;
      nullableHolder.nanoseconds = 123;
      IntervalMonthDayNanoHolder holder = new IntervalMonthDayNanoHolder();
      holder.months = Integer.MIN_VALUE;
      holder.days = Integer.MIN_VALUE;
      holder.nanoseconds = Long.MIN_VALUE;


      vector.set(0, /*months=*/1, /*days=*/2, /*nanoseconds=*/-2);
      vector.setSafe(2, /*months=*/1, /*days=*/2, /*nanoseconds=*/-3);
      vector.setSafe(/*index=*/4, nullableHolder);
      vector.set(3, holder);
      nullableHolder.isSet = 0;
      vector.setSafe(/*index=*/5, nullableHolder);
      vector.setValueCount(5);

      assertEquals("P1M2D PT-0.000000002S ", vector.getAsStringBuilder(0).toString());
      assertEquals(null, vector.getAsStringBuilder(1));
      assertEquals("P1M2D PT-0.000000003S ", vector.getAsStringBuilder(2).toString());
      assertEquals(new PeriodDuration(Period.of(0, Integer.MIN_VALUE, Integer.MIN_VALUE),
          Duration.ofNanos(Long.MIN_VALUE)), vector.getObject(3));
      assertEquals("P2M20D PT0.000000123S ", vector.getAsStringBuilder(4).toString());

      assertEquals(null, vector.getObject(5));

      vector.get(1, nullableHolder);
      assertEquals(0, nullableHolder.isSet);

      vector.get(2, nullableHolder);
      assertEquals(1, nullableHolder.isSet);
      assertEquals(1, nullableHolder.months);
      assertEquals(2, nullableHolder.days);
      assertEquals(-3, nullableHolder.nanoseconds);

      IntervalMonthDayNanoVector.getDays(vector.valueBuffer, 2);
      assertEquals(1, IntervalMonthDayNanoVector.getMonths(vector.valueBuffer, 2));
      assertEquals(2, IntervalMonthDayNanoVector.getDays(vector.valueBuffer, 2));
      assertEquals(-3, IntervalMonthDayNanoVector.getNanoseconds(vector.valueBuffer, 2));

      assertEquals(0, vector.isSet(1));
      assertEquals(1, vector.isSet(2));

      assertEquals(Types.MinorType.INTERVALMONTHDAYNANO, vector.getMinorType());
      ArrowType fieldType = vector.getField().getType();
      assertEquals(ArrowType.ArrowTypeID.Interval, fieldType.getTypeID());
      ArrowType.Interval intervalType = (ArrowType.Interval) fieldType;
      assertEquals(IntervalUnit.MONTH_DAY_NANO, intervalType.getUnit());
    }
  }
}
