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

import static org.apache.arrow.vector.TestUtils.newVector;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.math.BigDecimal;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tested field types:
 *
 * NullableInt
 * NullableBigInt
 * NullableFloat4
 * NullableFloat8
 * NullableBit
 * NullableDecimal
 * NullableIntervalDay
 * NullableIntervalYear
 * NullableSmallInt
 * NullableTinyInt
 * NullableVarChar
 * NullableTimeMicro
 * NullableTimeMilli
 * NullableTimeStamp*
 */

public class TestCopyFrom {

  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test /* NullableVarChar */
  public void testCopyFromWithNulls() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final VarCharVector vector2 =
             newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      vector.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector.getObject(i).toString());
        }
      }

      vector2.allocateNew();
      capacity = vector2.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* NO reAlloc() should have happened in copyFrom */
      capacity = vector2.getValueCapacity();
      assertEquals(4095, capacity);

      vector2.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }
    }
  }

  @Test /* NullableVarChar */
  public void testCopyFromWithNulls1() {
    try (final VarCharVector vector = newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator);
         final VarCharVector vector2 =
             newVector(VarCharVector.class, EMPTY_SCHEMA_PATH, MinorType.VARCHAR, allocator)) {
      vector.allocateNew();
      int capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          continue;
        }
        byte[] b = Integer.toString(i).getBytes();
        vector.setSafe(i, b, 0, b.length);
      }

      /* NO reAlloc() should have happened in setSafe() */
      capacity = vector.getValueCapacity();
      assertEquals(4095, capacity);

      vector.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector.getObject(i).toString());
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024 * 10, 1024);

      capacity = vector2.getValueCapacity();
      assertEquals(1024, capacity);

      for (int i = 0; i < 4095; i++) {
        vector2.copyFromSafe(i, i, vector);
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }

      /* 2 reAllocs should have happened in copyFromSafe() */
      capacity = vector2.getValueCapacity();
      assertEquals(4096, capacity);

      vector2.setValueCount(4095);

      for (int i = 0; i < 4095; i++) {
        if (i % 3 == 0) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, Integer.toString(i), vector2.getObject(i).toString());
        }
      }
    }
  }

  @Test /* IntVector */
  public void testCopyFromWithNulls2() {
    try (final IntVector vector1 = new IntVector(EMPTY_SCHEMA_PATH, allocator);
         final IntVector vector2 = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, 1000 + i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, 1000 + i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, 1000 + i, vector2.get(i));
        }
      }
    }
  }

  @Test /* BigIntVector */
  public void testCopyFromWithNulls3() {
    try (final BigIntVector vector1 = new BigIntVector(EMPTY_SCHEMA_PATH, allocator);
         final BigIntVector vector2 = new BigIntVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, 10000000000L + (long)i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  10000000000L + (long)i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  10000000000L + (long)i, vector2.get(i));
        }
      }
    }
  }

  @Test /* BitVector */
  public void testCopyFromWithNulls4() {
    try (final BitVector vector1 = new BitVector(EMPTY_SCHEMA_PATH, allocator);
         final BitVector vector2 = new BitVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      int counter = 0;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        if ((counter & 1) == 0) {
          vector1.setSafe(i, 1);
        } else {
          vector1.setSafe(i, 0);
        }
        counter++;
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      counter = 0;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          if ((counter & 1) == 0) {
            assertTrue(vector1.getObject(i));
          } else {
            assertFalse(vector1.getObject(i));
          }
          counter++;
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      counter = 0;
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          if ((counter & 1) == 0) {
            assertTrue(vector2.getObject(i));
          } else {
            assertFalse(vector2.getObject(i));
          }
          counter++;
        }
      }
    }
  }

  @Test /* Float4Vector */
  public void testCopyFromWithNulls5() {
    try (final Float4Vector vector1 = new Float4Vector(EMPTY_SCHEMA_PATH, allocator);
         final Float4Vector vector2 = new Float4Vector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, 100.25f + (float)i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  100.25f + (float)i, vector1.get(i), 0);
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  100.25f + i * 1.0f, vector2.get(i), 0);
        }
      }
    }
  }

  @Test /* Float8Vector */
  public void testCopyFromWithNulls6() {
    try (final Float8Vector vector1 = new Float8Vector(EMPTY_SCHEMA_PATH, allocator);
         final Float8Vector vector2 = new Float8Vector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, 123456.7865 + (double) i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  123456.7865 + (double) i, vector1.get(i), 0);
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  123456.7865 + (double) i, vector2.get(i), 0);
        }
      }
    }
  }

  @Test /* IntervalDayVector */
  public void testCopyFromWithNulls7() {
    try (final IntervalDayVector vector1 = new IntervalDayVector(EMPTY_SCHEMA_PATH, allocator);
         final IntervalDayVector vector2 = new IntervalDayVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final int days = 10;
      final int milliseconds = 10000;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, days + i, milliseconds + i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          final Period p = vector1.getObject(i);
          assertEquals(days + i, p.getDays());
          assertEquals(milliseconds + i, p.getMillis());
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          final Period p = vector2.getObject(i);
          assertEquals(days + i, p.getDays());
          assertEquals(milliseconds + i, p.getMillis());
        }
      }
    }
  }

  @Test /* IntervalYearVector */
  public void testCopyFromWithNulls8() {
    try (final IntervalYearVector vector1 = new IntervalYearVector(EMPTY_SCHEMA_PATH, allocator);
         final IntervalYearVector vector2 = new IntervalYearVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final int interval = 30; /* 2 years 6 months */
      final Period[]  periods = new Period[4096];
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, interval + i);
        final Period p = new Period();
        final int years = (interval + i) / org.apache.arrow.vector.util.DateUtility.yearsToMonths;
        final int months = (interval + i) % org.apache.arrow.vector.util.DateUtility.yearsToMonths;
        periods[i] = p.plusYears(years).plusMonths(months);;
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          final Period p = vector1.getObject(i);
          assertEquals(interval + i, vector1.get(i));
          assertEquals(periods[i], p);
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          final Period p = vector2.getObject(i);
          assertEquals(periods[i], p);
        }
      }
    }
  }

  @Test /* SmallIntVector */
  public void testCopyFromWithNulls9() {
    try (final SmallIntVector vector1 = new SmallIntVector(EMPTY_SCHEMA_PATH, allocator);
         final SmallIntVector vector2 = new SmallIntVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final short val = 1000;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, val + (short)i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (short)i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (short)i, vector2.get(i));
        }
      }
    }
  }

  @Test /* TimeMicroVector */
  public void testCopyFromWithNulls10() {
    try (final TimeMicroVector vector1 = new TimeMicroVector(EMPTY_SCHEMA_PATH, allocator);
         final TimeMicroVector vector2 = new TimeMicroVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final long val = 100485765432L;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, val + (long)i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (long)i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (long) i, vector2.get(i));
        }
      }
    }
  }

  @Test /* TimeMilliVector */
  public void testCopyFromWithNulls11() {
    try (final TimeMilliVector vector1 = new TimeMilliVector(EMPTY_SCHEMA_PATH, allocator);
         final TimeMilliVector vector2 = new TimeMilliVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final int val = 1000;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, val + i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + i, vector2.get(i));
        }
      }
    }
  }

  @Test /* TinyIntVector */
  public void testCopyFromWithNulls12() {
    try (final TinyIntVector vector1 = new TinyIntVector(EMPTY_SCHEMA_PATH, allocator);
         final TinyIntVector vector2 = new TinyIntVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      byte val = -128;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, val);
        val++;
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      val = -128;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, val, vector1.get(i));
          val++;
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      val = -128;
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i, val, vector2.get(i));
          val++;
        }
      }
    }
  }

  @Test /* DecimalVector */
  public void testCopyFromWithNulls13() {
    try (final DecimalVector vector1 = new DecimalVector(EMPTY_SCHEMA_PATH, allocator, 30, 16);
         final DecimalVector vector2 = new DecimalVector(EMPTY_SCHEMA_PATH, allocator, 30, 16)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final double baseValue = 104567897654.876543654;
      final BigDecimal[] decimals = new BigDecimal[4096];
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        BigDecimal decimal = new BigDecimal(baseValue + (double)i);
        vector1.setSafe(i, decimal);
        decimals[i] = decimal;
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          final BigDecimal decimal = vector1.getObject(i);
          assertEquals(decimals[i], decimal);
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          final BigDecimal decimal = vector2.getObject(i);
          assertEquals(decimals[i], decimal);
        }
      }
    }
  }

  @Test /* TimeStampVector */
  public void testCopyFromWithNulls14() {
    try (final TimeStampVector vector1 = new TimeStampMicroVector(EMPTY_SCHEMA_PATH, allocator);
         final TimeStampVector vector2 = new TimeStampMicroVector(EMPTY_SCHEMA_PATH, allocator)) {

      vector1.allocateNew();
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(0, vector1.getValueCount());

      final long val = 20145678912L;
      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          continue;
        }
        vector1.setSafe(i, val + (long)i);
      }

      vector1.setValueCount(4096);

      /* No realloc should have happened in setSafe or
       * setValueCount
       */
      assertEquals(4096, vector1.getValueCapacity());
      assertEquals(4096, vector1.getValueCount());

      for (int i = 0; i < 4096; i++) {
        if ((i & 1) == 0) {
          assertNull(vector1.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (long)i, vector1.get(i));
        }
      }

      /* set lesser initial capacity than actually needed
       * to trigger reallocs in copyFromSafe()
       */
      vector2.allocateNew(1024);
      assertEquals(1024, vector2.getValueCapacity());

      for (int i = 0; i < 4096; i++) {
        vector2.copyFromSafe(i, i, vector1);
      }

      /* 2 realloc should have happened in copyFromSafe() */
      assertEquals(4096, vector2.getValueCapacity());
      vector2.setValueCount(8192);
      /* setValueCount() should have done another realloc */
      assertEquals(8192, vector2.getValueCount());
      assertEquals(8192, vector2.getValueCapacity());

      /* check vector data after copy and realloc */
      for (int i = 0; i < 8192; i++) {
        if (((i & 1) == 0) || (i >= 4096)) {
          assertNull(vector2.getObject(i));
        } else {
          assertEquals("unexpected value at index: " + i,
                  val + (long) i, vector2.get(i));
        }
      }
    }
  }
}
