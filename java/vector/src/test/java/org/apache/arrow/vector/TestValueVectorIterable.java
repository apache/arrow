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

import static org.hamcrest.MatcherAssert.assertThat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionLargeListWriter;
import org.apache.arrow.vector.complex.impl.UnionListViewWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestValueVectorIterable {
  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new DirtyRootAllocator(Long.MAX_VALUE, (byte) 100);
  }

  @AfterEach
  public void terminate() throws Exception {
    allocator.close();
  }

  @Test
  public void testBigIntVectorIterable() {
    try (final BigIntVector bigIntVector = new BigIntVector("bigInt", allocator)) {
      bigIntVector.allocateNew(3);
      bigIntVector.setSafe(0, 6);
      bigIntVector.setSafe(1, 2);
      bigIntVector.setSafe(2, 19);
      bigIntVector.setValueCount(3);

      assertThat(
          bigIntVector.getValueIterable(), IsIterableContainingInOrder.contains(6L, 2L, 19L));
    }
  }

  @Test
  public void testBitVectorIterable() {
    try (final BitVector bitVector = new BitVector("bit", allocator)) {
      bitVector.allocateNew(3);
      bitVector.setSafe(0, 0);
      bitVector.setNull(1);
      bitVector.setSafe(2, 1);
      bitVector.setValueCount(3);

      assertThat(
          bitVector.getValueIterable(), IsIterableContainingInOrder.contains(false, null, true));
    }
  }

  @Test
  public void testDateDayVectorIterable() {
    try (final DateDayVector dateDayVector = new DateDayVector("dateDay", allocator)) {
      dateDayVector.allocateNew(3);
      dateDayVector.setSafe(0, 30000);
      dateDayVector.setNull(1);
      dateDayVector.setSafe(2, 555);
      dateDayVector.setValueCount(3);

      assertThat(
          dateDayVector.getValueIterable(), IsIterableContainingInOrder.contains(30000, null, 555));
    }
  }

  @Test
  public void testDateMilliVectorIterable() {
    try (final DateMilliVector dateMilliVector = new DateMilliVector("dateMilli", allocator)) {
      dateMilliVector.allocateNew(3);
      dateMilliVector.setSafe(0, 30000L);
      dateMilliVector.setNull(1);
      dateMilliVector.setSafe(2, 555L);
      dateMilliVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(30L, 0, ZoneOffset.ofHours(0));
      final LocalDateTime value3 =
          LocalDateTime.ofEpochSecond(0L, 555000000, ZoneOffset.ofHours(0));
      assertThat(
          dateMilliVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testDecimal256VectorIterable() {
    try (final Decimal256Vector decimal256Vector =
        new Decimal256Vector("decimal256", allocator, 8, 2)) {
      decimal256Vector.allocateNew(3);
      decimal256Vector.setSafe(0, 30000L);
      decimal256Vector.setNull(1);
      decimal256Vector.setSafe(2, 555L);
      decimal256Vector.setValueCount(3);

      final BigDecimal value1 = new BigDecimal(300L).setScale(2, RoundingMode.HALF_UP);
      final BigDecimal value3 =
          new BigDecimal(555L).scaleByPowerOfTen(-2).setScale(2, RoundingMode.HALF_UP);
      assertThat(
          decimal256Vector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testDecimalVectorIterable() {
    try (final DecimalVector decimalVector = new DecimalVector("decimalDay", allocator, 8, 2)) {
      decimalVector.allocateNew(3);
      decimalVector.setSafe(0, 30000);
      decimalVector.setNull(1);
      decimalVector.setSafe(2, 555);
      decimalVector.setValueCount(3);

      final BigDecimal value1 = new BigDecimal(300L).setScale(2, RoundingMode.HALF_UP);
      final BigDecimal value3 =
          new BigDecimal(555L).scaleByPowerOfTen(-2).setScale(2, RoundingMode.HALF_UP);
      assertThat(
          decimalVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testDurationVectorIterable() {
    try (final DurationVector durationVector =
        new DurationVector(
            Field.nullablePrimitive("duration", new ArrowType.Duration(TimeUnit.MILLISECOND)),
            allocator)) {
      durationVector.allocateNew(3);
      durationVector.setSafe(0, 30000);
      durationVector.setNull(1);
      durationVector.setSafe(2, 555);
      durationVector.setValueCount(3);

      final Duration value1 = Duration.ofMillis(30000);
      final Duration value3 = Duration.ofMillis(555);
      assertThat(
          durationVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testFixedSizeBinaryVectorIterable() {
    try (final FixedSizeBinaryVector fixedSizeBinaryVector =
        new FixedSizeBinaryVector("binary", allocator, 4)) {
      final byte[] value1 = new byte[] {0, 0, 0, 1};
      final byte[] value3 = new byte[] {1, 0, 0, 0};

      fixedSizeBinaryVector.allocateNew(3);
      fixedSizeBinaryVector.setSafe(0, value1);
      fixedSizeBinaryVector.setNull(1);
      fixedSizeBinaryVector.setSafe(2, value3);
      fixedSizeBinaryVector.setValueCount(3);

      assertThat(
          fixedSizeBinaryVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testFloat2VectorIterable() {
    try (final Float2Vector float2Vector = new Float2Vector("float2", allocator)) {
      float2Vector.allocateNew(3);
      float2Vector.setSafe(0, (short) 7777);
      float2Vector.setNull(1);
      float2Vector.setSafe(2, (short) 5);
      float2Vector.setValueCount(3);

      assertThat(
          float2Vector.getValueIterable(),
          IsIterableContainingInOrder.contains((short) 7777, null, (short) 5));
    }
  }

  @Test
  public void testFloat4VectorIterable() {
    try (final Float4Vector float4Vector = new Float4Vector("float4", allocator)) {
      float4Vector.allocateNew(3);
      float4Vector.setSafe(0, 16.32f);
      float4Vector.setNull(1);
      float4Vector.setSafe(2, -10.75f);
      float4Vector.setValueCount(3);

      assertThat(
          float4Vector.getValueIterable(),
          IsIterableContainingInOrder.contains(16.32f, null, -10.75f));
    }
  }

  @Test
  public void testFloat8VectorIterable() {
    try (final Float8Vector float8Vector = new Float8Vector("float8", allocator)) {
      float8Vector.allocateNew(3);
      float8Vector.setSafe(0, 16.32);
      float8Vector.setNull(1);
      float8Vector.setSafe(2, -10.75);
      float8Vector.setValueCount(3);

      assertThat(
          float8Vector.getValueIterable(),
          IsIterableContainingInOrder.contains(16.32, null, -10.75));
    }
  }

  @Test
  public void testIntVectorIterable() {
    try (final IntVector intVector = new IntVector("int", allocator)) {
      intVector.allocateNew(3);
      intVector.setSafe(0, 78);
      intVector.setNull(1);
      intVector.setSafe(2, -93);
      intVector.setValueCount(3);

      assertThat(intVector.getValueIterable(), IsIterableContainingInOrder.contains(78, null, -93));
    }
  }

  @Test
  public void testIntervalDayVectorIterable() {
    try (final IntervalDayVector intervalDayVector =
        new IntervalDayVector("intervalDay", allocator)) {
      intervalDayVector.allocateNew(3);
      intervalDayVector.setSafe(0, 63, 0);
      intervalDayVector.setNull(1);
      intervalDayVector.setSafe(2, 555, 0);
      intervalDayVector.setValueCount(3);

      final Duration value1 = Duration.ofDays(63);
      final Duration value3 = Duration.ofDays(555);
      assertThat(
          intervalDayVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testIntervalMonthDayNanoVectorIterable() {
    try (final IntervalMonthDayNanoVector intervalMonthDayNanoVector =
        new IntervalMonthDayNanoVector("intervalMonthDayNano", allocator)) {
      intervalMonthDayNanoVector.allocateNew(3);
      intervalMonthDayNanoVector.setSafe(0, 3, 4, 0);
      intervalMonthDayNanoVector.setNull(1);
      intervalMonthDayNanoVector.setSafe(2, 7, 18, 0);
      intervalMonthDayNanoVector.setValueCount(3);

      final PeriodDuration value1 = new PeriodDuration(Period.of(0, 3, 4), Duration.ofSeconds(0));
      final PeriodDuration value3 = new PeriodDuration(Period.of(0, 7, 18), Duration.ofSeconds(0));
      assertThat(
          intervalMonthDayNanoVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testIntervalYearVectorIterable() {
    try (final IntervalYearVector intervalYearVector =
        new IntervalYearVector("intervalYear", allocator)) {
      intervalYearVector.allocateNew(3);
      intervalYearVector.setSafe(0, 3);
      intervalYearVector.setNull(1);
      intervalYearVector.setSafe(2, 17);
      intervalYearVector.setValueCount(3);

      final Period value1 = Period.ofMonths(3);
      final Period value3 = Period.ofMonths(17);
      assertThat(
          intervalYearVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testLargeVarBinaryVectorIterable() {
    try (final LargeVarBinaryVector largeVarBinaryVector =
        new LargeVarBinaryVector("largeVarBinary", allocator)) {
      final byte[] value1 = new byte[] {0, 0, 0, 1};
      final byte[] value3 = new byte[] {1, 0, 0};

      largeVarBinaryVector.allocateNew(3);
      largeVarBinaryVector.setSafe(0, value1);
      largeVarBinaryVector.setNull(1);
      largeVarBinaryVector.setSafe(2, value3);
      largeVarBinaryVector.setValueCount(3);

      assertThat(
          largeVarBinaryVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testLargeVarCharVectorIterable() {
    try (final LargeVarCharVector largeVarCharVector =
        new LargeVarCharVector("largeVarChar", allocator)) {
      final Text value1 = new Text("hello");
      final Text value3 = new Text("worlds");

      largeVarCharVector.allocateNew(3);
      largeVarCharVector.setSafe(0, value1);
      largeVarCharVector.setNull(1);
      largeVarCharVector.setSafe(2, value3);
      largeVarCharVector.setValueCount(3);

      assertThat(
          largeVarCharVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testNullVectorIterable() {
    try (final NullVector nullVector = new NullVector("null", 3)) {
      assertThat(
          nullVector.getValueIterable(), IsIterableContainingInOrder.contains(null, null, null));
    }
  }

  @Test
  public void testSmallIntVectorIterable() {
    try (final SmallIntVector smallIntVector = new SmallIntVector("smallInt", allocator)) {
      smallIntVector.allocateNew(3);
      smallIntVector.setSafe(0, 78);
      smallIntVector.setNull(1);
      smallIntVector.setSafe(2, -93);
      smallIntVector.setValueCount(3);

      assertThat(
          smallIntVector.getValueIterable(),
          IsIterableContainingInOrder.contains((short) 78, null, (short) -93));
    }
  }

  @Test
  public void testTimeMicroVectorIterable() {
    try (final TimeMicroVector timeMicroVector = new TimeMicroVector("timeMicro", allocator)) {
      timeMicroVector.allocateNew(3);
      timeMicroVector.setSafe(0, 70000);
      timeMicroVector.setNull(1);
      timeMicroVector.setSafe(2, 555);
      timeMicroVector.setValueCount(3);

      assertThat(
          timeMicroVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeMilliVectorIterable() {
    try (final TimeMilliVector timeMilliVector = new TimeMilliVector("timeMilli", allocator)) {
      timeMilliVector.allocateNew(3);
      timeMilliVector.setSafe(0, 70000);
      timeMilliVector.setNull(1);
      timeMilliVector.setSafe(2, 555);
      timeMilliVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(70L, 0, ZoneOffset.ofHours(0));
      final LocalDateTime value3 =
          LocalDateTime.ofEpochSecond(0L, 555000000, ZoneOffset.ofHours(0));
      assertThat(
          timeMilliVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testTimeNanoVectorIterable() {
    try (final TimeNanoVector timeNanoVector = new TimeNanoVector("timeNano", allocator)) {
      timeNanoVector.allocateNew(3);
      timeNanoVector.setSafe(0, 70000);
      timeNanoVector.setNull(1);
      timeNanoVector.setSafe(2, 555);
      timeNanoVector.setValueCount(3);

      assertThat(
          timeNanoVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeSecVectorIterable() {
    try (final TimeSecVector timeSecVector = new TimeSecVector("timeSec", allocator)) {
      timeSecVector.allocateNew(3);
      timeSecVector.setSafe(0, 70000);
      timeSecVector.setNull(1);
      timeSecVector.setSafe(2, 555);
      timeSecVector.setValueCount(3);

      assertThat(
          timeSecVector.getValueIterable(), IsIterableContainingInOrder.contains(70000, null, 555));
    }
  }

  @Test
  public void testTimeStampMicroTZVectorIterable() {
    try (final TimeStampMicroTZVector timeStampMicroTzVector =
        new TimeStampMicroTZVector("timeStampMicroTZ", allocator, "UTC")) {
      timeStampMicroTzVector.allocateNew(3);
      timeStampMicroTzVector.setSafe(0, 70000);
      timeStampMicroTzVector.setNull(1);
      timeStampMicroTzVector.setSafe(2, 555);
      timeStampMicroTzVector.setValueCount(3);

      assertThat(
          timeStampMicroTzVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeStampMicroVectorIterable() {
    try (final TimeStampMicroVector timeStampMicroVector =
        new TimeStampMicroVector("timeStampMicro", allocator)) {
      timeStampMicroVector.allocateNew(3);
      timeStampMicroVector.setSafe(0, 70000);
      timeStampMicroVector.setNull(1);
      timeStampMicroVector.setSafe(2, 555);
      timeStampMicroVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(0L, 70000000, ZoneOffset.ofHours(0));
      final LocalDateTime value3 = LocalDateTime.ofEpochSecond(0L, 555000, ZoneOffset.ofHours(0));
      assertThat(
          timeStampMicroVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testTimeStampMilliTZVectorIterable() {
    try (final TimeStampMilliTZVector timeStampMilliTzVector =
        new TimeStampMilliTZVector("timeStampMilliTZ", allocator, "UTC")) {
      timeStampMilliTzVector.allocateNew(3);
      timeStampMilliTzVector.setSafe(0, 70000);
      timeStampMilliTzVector.setNull(1);
      timeStampMilliTzVector.setSafe(2, 555);
      timeStampMilliTzVector.setValueCount(3);

      assertThat(
          timeStampMilliTzVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeStampMilliVectorIterable() {
    try (final TimeStampMilliVector timeStampMilliVector =
        new TimeStampMilliVector("timeStampMilli", allocator)) {
      timeStampMilliVector.allocateNew(3);
      timeStampMilliVector.setSafe(0, 70000);
      timeStampMilliVector.setNull(1);
      timeStampMilliVector.setSafe(2, 555);
      timeStampMilliVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(70L, 0, ZoneOffset.ofHours(0));
      final LocalDateTime value3 =
          LocalDateTime.ofEpochSecond(0L, 555000000, ZoneOffset.ofHours(0));
      assertThat(
          timeStampMilliVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testTimeStampNanoTZVectorIterable() {
    try (final TimeStampNanoTZVector timeStampNanoTzVector =
        new TimeStampNanoTZVector("timeStampNanoTZ", allocator, "UTC")) {
      timeStampNanoTzVector.allocateNew(3);
      timeStampNanoTzVector.setSafe(0, 70000);
      timeStampNanoTzVector.setNull(1);
      timeStampNanoTzVector.setSafe(2, 555);
      timeStampNanoTzVector.setValueCount(3);

      assertThat(
          timeStampNanoTzVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeStampNanoVectorIterable() {
    try (final TimeStampNanoVector timeStampNanoVector =
        new TimeStampNanoVector("timeStampNano", allocator)) {
      timeStampNanoVector.allocateNew(3);
      timeStampNanoVector.setSafe(0, 70000);
      timeStampNanoVector.setNull(1);
      timeStampNanoVector.setSafe(2, 555);
      timeStampNanoVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(0L, 70000, ZoneOffset.ofHours(0));
      final LocalDateTime value3 = LocalDateTime.ofEpochSecond(0L, 555, ZoneOffset.ofHours(0));
      assertThat(
          timeStampNanoVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testTimeStampSecTZVectorIterable() {
    try (final TimeStampSecTZVector timeStampSecTzVector =
        new TimeStampSecTZVector("timeStampSecTZ", allocator, "UTC")) {
      timeStampSecTzVector.allocateNew(3);
      timeStampSecTzVector.setSafe(0, 70000);
      timeStampSecTzVector.setNull(1);
      timeStampSecTzVector.setSafe(2, 555);
      timeStampSecTzVector.setValueCount(3);

      assertThat(
          timeStampSecTzVector.getValueIterable(),
          IsIterableContainingInOrder.contains(70000L, null, 555L));
    }
  }

  @Test
  public void testTimeStampSecVectorIterable() {
    try (final TimeStampSecVector timeStampSecVector =
        new TimeStampSecVector("timeStampSec", allocator)) {
      timeStampSecVector.allocateNew(3);
      timeStampSecVector.setSafe(0, 70000);
      timeStampSecVector.setNull(1);
      timeStampSecVector.setSafe(2, 555);
      timeStampSecVector.setValueCount(3);

      final LocalDateTime value1 = LocalDateTime.ofEpochSecond(70000L, 0, ZoneOffset.ofHours(0));
      final LocalDateTime value3 = LocalDateTime.ofEpochSecond(555L, 0, ZoneOffset.ofHours(0));
      assertThat(
          timeStampSecVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testTinyIntVectorIterable() {
    try (final TinyIntVector tinyIntVector = new TinyIntVector("tinyInt", allocator)) {
      tinyIntVector.allocateNew(3);
      tinyIntVector.setSafe(0, 8);
      tinyIntVector.setNull(1);
      tinyIntVector.setSafe(2, -17);
      tinyIntVector.setValueCount(3);

      assertThat(
          tinyIntVector.getValueIterable(),
          IsIterableContainingInOrder.contains((byte) 8, null, (byte) -17));
    }
  }

  @Test
  public void testUInt1VectorIterable() {
    try (final UInt1Vector uint1Vector = new UInt1Vector("uint1", allocator)) {
      uint1Vector.allocateNew(3);
      uint1Vector.setSafe(0, 8);
      uint1Vector.setNull(1);
      uint1Vector.setSafe(2, 101);
      uint1Vector.setValueCount(3);

      assertThat(
          uint1Vector.getValueIterable(),
          IsIterableContainingInOrder.contains((byte) 8, null, (byte) 101));
    }
  }

  @Test
  public void testUInt2VectorIterable() {
    try (final UInt2Vector uint2Vector = new UInt2Vector("uint2", allocator)) {
      uint2Vector.allocateNew(3);
      uint2Vector.setSafe(0, 78);
      uint2Vector.setNull(1);
      uint2Vector.setSafe(2, 3456);
      uint2Vector.setValueCount(3);

      assertThat(
          uint2Vector.getValueIterable(),
          IsIterableContainingInOrder.contains((char) 78, null, (char) 3456));
    }
  }

  @Test
  public void testUInt4VectorIterable() {
    try (final UInt4Vector uint4Vector = new UInt4Vector("uint4", allocator)) {
      uint4Vector.allocateNew(3);
      uint4Vector.setSafe(0, 78);
      uint4Vector.setNull(1);
      uint4Vector.setSafe(2, 3456);
      uint4Vector.setValueCount(3);

      assertThat(
          uint4Vector.getValueIterable(), IsIterableContainingInOrder.contains(78, null, 3456));
    }
  }

  @Test
  public void testUInt8VectorIterable() {
    try (final UInt8Vector uint8Vector = new UInt8Vector("uint8", allocator)) {
      uint8Vector.allocateNew(3);
      uint8Vector.setSafe(0, 6);
      uint8Vector.setSafe(1, 2);
      uint8Vector.setSafe(2, 19);
      uint8Vector.setValueCount(3);

      assertThat(uint8Vector.getValueIterable(), IsIterableContainingInOrder.contains(6L, 2L, 19L));
    }
  }

  @Test
  public void testVarBinaryVectorIterable() {
    try (final VarBinaryVector varBinaryVector = new VarBinaryVector("varBinary", allocator)) {
      final byte[] value1 = new byte[] {0, 0, 0, 1};
      final byte[] value3 = new byte[] {1, 0, 0};

      varBinaryVector.allocateNew(3);
      varBinaryVector.setSafe(0, value1);
      varBinaryVector.setNull(1);
      varBinaryVector.setSafe(2, value3);
      varBinaryVector.setValueCount(3);

      assertThat(
          varBinaryVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testVarCharVectorIterable() {
    try (final VarCharVector varCharVector = new VarCharVector("varChar", allocator)) {
      final Text value1 = new Text("hello");
      final Text value3 = new Text("worlds");

      varCharVector.allocateNew(3);
      varCharVector.setSafe(0, value1);
      varCharVector.setNull(1);
      varCharVector.setSafe(2, value3);
      varCharVector.setValueCount(3);

      assertThat(
          varCharVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testViewVarBinaryVectorIterable() {
    try (final ViewVarBinaryVector viewVarBinaryVector =
        new ViewVarBinaryVector("viewVarBinary", allocator)) {
      final byte[] value1 = new byte[] {0, 0, 0, 1};
      final byte[] value3 = new byte[] {1, 0, 0};

      viewVarBinaryVector.allocateNew(3);
      viewVarBinaryVector.setSafe(0, value1);
      viewVarBinaryVector.setNull(1);
      viewVarBinaryVector.setSafe(2, value3);
      viewVarBinaryVector.setValueCount(3);

      assertThat(
          viewVarBinaryVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testViewVarCharVectorIterable() {
    try (final ViewVarCharVector viewVarCharVector =
        new ViewVarCharVector("viewVarChar", allocator)) {
      final Text value1 = new Text("hello");
      final Text value3 = new Text("worlds");

      viewVarCharVector.allocateNew(3);
      viewVarCharVector.setSafe(0, value1);
      viewVarCharVector.setNull(1);
      viewVarCharVector.setSafe(2, value3);
      viewVarCharVector.setValueCount(3);

      assertThat(
          viewVarCharVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }

  @Test
  public void testFIxedSizeListVectorIterable() {
    try (final FixedSizeListVector fixedSizeListVector =
        new FixedSizeListVector(
            "fixedSizeList", allocator, FieldType.nullable(new ArrowType.FixedSizeList(3)), null)) {
      final IntVector listVector =
          (IntVector)
              fixedSizeListVector
                  .addOrGetVector(FieldType.nullable(Types.MinorType.INT.getType()))
                  .getVector();
      listVector.setSafe(0, 1);
      listVector.setSafe(1, 2);
      listVector.setSafe(2, 3);
      listVector.setSafe(3, 4);
      listVector.setSafe(4, 5);
      listVector.setSafe(5, 6);
      listVector.setValueCount(6);
      fixedSizeListVector.setValueCount(2);
      fixedSizeListVector.setNotNull(0);
      fixedSizeListVector.setNotNull(1);

      final List<Integer> list1 = new ArrayList<>();
      list1.add(1);
      list1.add(2);
      list1.add(3);
      final List<Integer> list2 = new ArrayList<>();
      list2.add(4);
      list2.add(5);
      list2.add(6);

      assertThat(
          fixedSizeListVector.getValueIterable(),
          IsIterableContainingInOrder.contains(list1, list2));
    }
  }

  @Test
  public void testLargeListVectorIterable() {
    try (final LargeListVector largeListVector = LargeListVector.empty("largeList", allocator)) {
      UnionLargeListWriter writer = largeListVector.getWriter();
      writer.allocate();

      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.integer().writeInt(3);
      writer.endList();
      writer.startList();
      writer.integer().writeInt(4);
      writer.integer().writeInt(5);
      writer.endList();

      largeListVector.setValueCount(2);

      final List<Integer> list1 = new ArrayList<>();
      list1.add(1);
      list1.add(2);
      list1.add(3);
      final List<Integer> list2 = new ArrayList<>();
      list2.add(4);
      list2.add(5);

      assertThat(
          largeListVector.getValueIterable(), IsIterableContainingInOrder.contains(list1, list2));
    }
  }

  @Test
  public void testListVectorIterable() {
    try (final ListVector listVector = ListVector.empty("largeList", allocator)) {
      UnionListWriter writer = listVector.getWriter();
      writer.allocate();

      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.integer().writeInt(3);
      writer.endList();
      writer.startList();
      writer.integer().writeInt(4);
      writer.integer().writeInt(5);
      writer.endList();

      listVector.setValueCount(2);

      final List<Integer> list1 = new ArrayList<>();
      list1.add(1);
      list1.add(2);
      list1.add(3);
      final List<Integer> list2 = new ArrayList<>();
      list2.add(4);
      list2.add(5);

      assertThat(listVector.getValueIterable(), IsIterableContainingInOrder.contains(list1, list2));
    }
  }

  @Test
  public void testListViewVectorIterable() {
    try (final ListViewVector listViewVector = ListViewVector.empty("largeList", allocator)) {
      UnionListViewWriter writer = listViewVector.getWriter();
      writer.allocate();

      writer.startList();
      writer.integer().writeInt(1);
      writer.integer().writeInt(2);
      writer.integer().writeInt(3);
      writer.endList();
      writer.startList();
      writer.integer().writeInt(4);
      writer.integer().writeInt(5);
      writer.endList();

      listViewVector.setValueCount(2);

      final List<Integer> list1 = new ArrayList<>();
      list1.add(1);
      list1.add(2);
      list1.add(3);
      final List<Integer> list2 = new ArrayList<>();
      list2.add(4);
      list2.add(5);

      assertThat(
          listViewVector.getValueIterable(), IsIterableContainingInOrder.contains(list1, list2));
    }
  }

  @Test
  public void testNonNullableStructVectorIterable() {
    try (final NonNullableStructVector nonNullableStructVector =
        NonNullableStructVector.empty("nonNullableStruct", allocator)) {
      nonNullableStructVector.setValueCount(2);

      IntVector key1Vector =
          nonNullableStructVector.addOrGet(
              "key1", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
      IntVector key2Vector =
          nonNullableStructVector.addOrGet(
              "key2", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
      key1Vector.setSafe(0, 1);
      key1Vector.setSafe(1, 3);
      key2Vector.setSafe(0, 2);
      key2Vector.setSafe(1, 4);
      key1Vector.setValueCount(2);
      key2Vector.setValueCount(2);

      final Map<String, Integer> value1 = new HashMap<>();
      value1.put("key1", 1);
      value1.put("key2", 2);
      final Map<String, Integer> value2 = new HashMap<>();
      value2.put("key1", 3);
      value2.put("key2", 4);

      assertThat(
          nonNullableStructVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, value2));
    }
  }

  @Test
  public void testStructVectorIterable() {
    try (final StructVector structVector = StructVector.empty("struct", allocator)) {
      structVector.addOrGetList("struct");
      NullableStructWriter structWriter = structVector.getWriter();
      structWriter.setPosition(0);
      structWriter.start();
      structWriter.integer("key1").writeInt(1);
      structWriter.integer("key2").writeInt(2);
      structWriter.end();
      structWriter.setPosition(2);
      structWriter.start();
      structWriter.integer("key1").writeInt(3);
      structWriter.integer("key2").writeInt(4);
      structWriter.end();
      structWriter.setValueCount(3);

      final Map<String, Integer> value1 = new HashMap<>();
      value1.put("key1", 1);
      value1.put("key2", 2);
      final Map<String, Integer> value3 = new HashMap<>();
      value3.put("key1", 3);
      value3.put("key2", 4);

      assertThat(
          structVector.getValueIterable(),
          IsIterableContainingInOrder.contains(value1, null, value3));
    }
  }
}
