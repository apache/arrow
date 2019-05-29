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
import static org.junit.Assert.assertNull;

import java.time.Duration;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDurationVector {
  RootAllocator allocator;

  @Before
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @After
  public void terminate() {
    allocator.close();
  }

  @Test
  public void testSecBasics() {
    try (DurationVector secVector = TestUtils.newVector(DurationVector.class, "second",
        new ArrowType.Duration(TimeUnit.SECOND), allocator)) {

      secVector.allocateNew();
      secVector.setNull(0);
      secVector.setSafe(1, 1000);
      secVector.setValueCount(2);
      assertNull(secVector.getObject(0));
      assertEquals(Duration.ofSeconds(1000), secVector.getObject(1));
      assertNull(secVector.getAsStringBuilder(0));
      assertEquals("PT16M40S", secVector.getAsStringBuilder(1).toString());
      // Holder
      NullableDurationHolder holder = new NullableDurationHolder();
      secVector.get(0, holder);
      assertEquals(0, holder.isSet);
      secVector.get(1, holder);
      assertEquals(1 , holder.isSet);
      assertEquals(1000 , holder.value);
    }
  }

  @Test
  public void testMilliBasics() {
    try (DurationVector milliVector = TestUtils.newVector(DurationVector.class, "nanos",
        new ArrowType.Duration(TimeUnit.MILLISECOND), allocator)) {

      milliVector.allocateNew();
      milliVector.setNull(0);
      milliVector.setSafe(1, 1000);
      milliVector.setValueCount(2);
      assertNull(milliVector.getObject(0));
      assertEquals(Duration.ofSeconds(1), milliVector.getObject(1));
      assertNull(milliVector.getAsStringBuilder(0));
      assertEquals("PT1S", milliVector.getAsStringBuilder(1).toString());
      // Holder
      NullableDurationHolder holder = new NullableDurationHolder();
      milliVector.get(0, holder);
      assertEquals(0, holder.isSet);
      milliVector.get(1, holder);
      assertEquals(1 , holder.isSet);
      assertEquals(1000 , holder.value);
    }
  }

  @Test
  public void testMicroBasics() {
    try (DurationVector microVector = TestUtils.newVector(DurationVector.class, "micro",
        new ArrowType.Duration(TimeUnit.MICROSECOND), allocator)) {

      microVector.allocateNew();
      microVector.setNull(0);
      microVector.setSafe(1, 1000);
      microVector.setValueCount(2);
      assertNull(microVector.getObject(0));
      assertEquals(Duration.ofMillis(1), microVector.getObject(1));
      assertNull(microVector.getAsStringBuilder(0));
      assertEquals("PT0.001S", microVector.getAsStringBuilder(1).toString());
      // Holder
      NullableDurationHolder holder = new NullableDurationHolder();
      microVector.get(0, holder);
      assertEquals(0, holder.isSet);
      microVector.get(1, holder);
      assertEquals(1 , holder.isSet);
      assertEquals(1000 , holder.value);
    }
  }

  @Test
  public void testNanosBasics() {
    try (DurationVector nanoVector = TestUtils.newVector(DurationVector.class, "nanos",
        new ArrowType.Duration(TimeUnit.NANOSECOND), allocator)) {

      nanoVector.allocateNew();
      nanoVector.setNull(0);
      nanoVector.setSafe(1, 1000000);
      nanoVector.setValueCount(2);
      assertNull(nanoVector.getObject(0));
      assertEquals(Duration.ofMillis(1), nanoVector.getObject(1));
      assertNull(nanoVector.getAsStringBuilder(0));
      assertEquals("PT0.001S", nanoVector.getAsStringBuilder(1).toString());
      // Holder
      NullableDurationHolder holder = new NullableDurationHolder();
      nanoVector.get(0, holder);
      assertEquals(0, holder.isSet);
      nanoVector.get(1, holder);
      assertEquals(1 , holder.isSet);
      assertEquals(1000000 , holder.value);
    }
  }
}
