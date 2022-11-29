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

package org.apache.arrow.vector.table;

import static org.apache.arrow.vector.table.TestUtils.BIGINT_INT_MAP_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.FIXEDBINARY_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.INT_LIST_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.STRUCT_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.UNION_VECTOR_NAME;
import static org.apache.arrow.vector.table.TestUtils.VARBINARY_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.VARCHAR_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.fixedWidthVectors;
import static org.apache.arrow.vector.table.TestUtils.intPlusFixedBinaryColumns;
import static org.apache.arrow.vector.table.TestUtils.intPlusLargeVarBinaryColumns;
import static org.apache.arrow.vector.table.TestUtils.intPlusLargeVarcharColumns;
import static org.apache.arrow.vector.table.TestUtils.intPlusVarBinaryColumns;
import static org.apache.arrow.vector.table.TestUtils.intPlusVarcharColumns;
import static org.apache.arrow.vector.table.TestUtils.simpleDenseUnionVector;
import static org.apache.arrow.vector.table.TestUtils.simpleListVector;
import static org.apache.arrow.vector.table.TestUtils.simpleMapVector;
import static org.apache.arrow.vector.table.TestUtils.simpleStructVector;
import static org.apache.arrow.vector.table.TestUtils.simpleUnionVector;
import static org.apache.arrow.vector.table.TestUtils.timezoneTemporalVectors;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.holders.NullableIntervalYearHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTimeMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeSecHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMicroTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoTZHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecTZHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.TestExtensionType;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RowTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(StandardCharsets.UTF_8, c.getDefaultCharacterSet());
    }
  }

  @Test
  void at() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(c.getRowNumber(), -1);
      c.setPosition(1);
      assertEquals(c.getRowNumber(), 1);
    }
  }

  @Test
  void getIntByVectorIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
    }
  }

  @Test
  void getIntByVectorName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void testNameNotFound() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertThrows(IllegalArgumentException.class,
          () -> c.getVarCharObj("wrong name"));
    }
  }

  @Test
  void testWrongType() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertThrows(ClassCastException.class,
          () -> c.getVarCharObj(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void getDecimal() {
    List<FieldVector> vectors = new ArrayList<>();
    DecimalVector decimalVector = new DecimalVector("decimal_vector", allocator, 55, 10);
    vectors.add(decimalVector);
    decimalVector.setSafe(0, new BigDecimal("0.0543278923"));
    decimalVector.setSafe(1, new BigDecimal("2.0543278923"));
    decimalVector.setValueCount(2);
    BigDecimal one = decimalVector.getObject(1);

    NullableDecimalHolder holder1 = new NullableDecimalHolder();
    NullableDecimalHolder holder2 = new NullableDecimalHolder();
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getDecimalObj("decimal_vector"));
      assertEquals(one, c.getDecimalObj(0));
      c.getDecimal(0, holder1);
      c.getDecimal("decimal_vector", holder2);
      assertEquals(holder1.buffer, holder2.buffer);
      assertEquals(c.getDecimal(0).memoryAddress(), c.getDecimal("decimal_vector").memoryAddress());
    }
  }

  @Test
  void getDuration() {
    List<FieldVector> vectors = new ArrayList<>();
    TimeUnit unit = TimeUnit.SECOND;
    final FieldType fieldType = FieldType.nullable(new ArrowType.Duration(unit));

    DurationVector durationVector = new DurationVector("duration_vector", fieldType, allocator);
    NullableDurationHolder holder1 = new NullableDurationHolder();
    NullableDurationHolder holder2 = new NullableDurationHolder();

    holder1.value = 100;
    holder1.unit = TimeUnit.SECOND;
    holder1.isSet = 1;
    holder2.value = 200;
    holder2.unit = TimeUnit.SECOND;
    holder2.isSet = 1;

    vectors.add(durationVector);
    durationVector.setSafe(0, holder1);
    durationVector.setSafe(1, holder2);
    durationVector.setValueCount(2);

    Duration one = durationVector.getObject(1);
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getDurationObj("duration_vector"));
      assertEquals(one, c.getDurationObj(0));
      c.getDuration(0, holder1);
      c.getDuration("duration_vector", holder2);
      assertEquals(holder1.value, holder2.value);
      ArrowBuf durationBuf1 = c.getDuration(0);
      ArrowBuf durationBuf2 = c.getDuration("duration_vector");
      assertEquals(durationBuf1.memoryAddress(), durationBuf2.memoryAddress());
    }
  }

  @Test
  void getIntervalDay() {
    List<FieldVector> vectors = new ArrayList<>();
    IntervalUnit unit = IntervalUnit.DAY_TIME;
    final FieldType fieldType = FieldType.nullable(new ArrowType.Interval(unit));

    IntervalDayVector intervalDayVector = new IntervalDayVector("intervalDay_vector", fieldType, allocator);
    NullableIntervalDayHolder holder1 = new NullableIntervalDayHolder();
    NullableIntervalDayHolder holder2 = new NullableIntervalDayHolder();

    holder1.days = 100;
    holder1.milliseconds = 1000;
    holder1.isSet = 1;
    holder2.days = 200;
    holder2.milliseconds = 2000;
    holder2.isSet = 1;

    vectors.add(intervalDayVector);
    intervalDayVector.setSafe(0, holder1);
    intervalDayVector.setSafe(1, holder2);
    intervalDayVector.setValueCount(2);

    Duration one = intervalDayVector.getObject(1);
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getIntervalDayObj("intervalDay_vector"));
      assertEquals(one, c.getIntervalDayObj(0));
      c.getIntervalDay(0, holder1);
      c.getIntervalDay("intervalDay_vector", holder2);
      assertEquals(holder1.days, holder2.days);
      assertEquals(holder1.milliseconds, holder2.milliseconds);
      ArrowBuf intDayBuf1 = c.getIntervalDay(0);
      ArrowBuf intDayBuf2 = c.getIntervalDay("intervalDay_vector");
      assertEquals(intDayBuf1.memoryAddress(), intDayBuf2.memoryAddress());
    }
  }

  @Test
  void getIntervalMonth() {
    List<FieldVector> vectors = new ArrayList<>();
    IntervalUnit unit = IntervalUnit.MONTH_DAY_NANO;
    final FieldType fieldType = FieldType.nullable(new ArrowType.Interval(unit));

    IntervalMonthDayNanoVector intervalMonthVector =
        new IntervalMonthDayNanoVector("intervalMonth_vector", fieldType, allocator);
    NullableIntervalMonthDayNanoHolder holder1 = new NullableIntervalMonthDayNanoHolder();
    NullableIntervalMonthDayNanoHolder holder2 = new NullableIntervalMonthDayNanoHolder();

    holder1.days = 1;
    holder1.months = 10;
    holder1.isSet = 1;
    holder2.days = 2;
    holder2.months = 20;
    holder2.isSet = 1;

    vectors.add(intervalMonthVector);
    intervalMonthVector.setSafe(0, holder1);
    intervalMonthVector.setSafe(1, holder2);
    intervalMonthVector.setValueCount(2);

    PeriodDuration one = intervalMonthVector.getObject(1);
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getIntervalMonthDayNanoObj("intervalMonth_vector"));
      assertEquals(one, c.getIntervalMonthDayNanoObj(0));
      c.getIntervalMonthDayNano(0, holder1);
      c.getIntervalMonthDayNano("intervalMonth_vector", holder2);
      assertEquals(holder1.days, holder2.days);
      assertEquals(holder1.months, holder2.months);
      ArrowBuf intMonthBuf1 = c.getIntervalMonthDayNano(0);
      ArrowBuf intMonthBuf2 = c.getIntervalMonthDayNano("intervalMonth_vector");
      assertEquals(intMonthBuf1.memoryAddress(), intMonthBuf2.memoryAddress());
    }
  }

  @Test
  void getIntervalYear() {
    List<FieldVector> vectors = new ArrayList<>();
    IntervalUnit unit = IntervalUnit.YEAR_MONTH;
    final FieldType fieldType = FieldType.nullable(new ArrowType.Interval(unit));

    IntervalYearVector intervalYearVector = new IntervalYearVector("intervalYear_vector", fieldType, allocator);
    NullableIntervalYearHolder holder1 = new NullableIntervalYearHolder();
    NullableIntervalYearHolder holder2 = new NullableIntervalYearHolder();

    holder1.value = 1;
    holder1.isSet = 1;
    holder2.value = 2;
    holder2.isSet = 1;

    vectors.add(intervalYearVector);
    intervalYearVector.setSafe(0, holder1);
    intervalYearVector.setSafe(1, holder2);
    intervalYearVector.setValueCount(2);

    Period one = intervalYearVector.getObject(1);
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getIntervalYearObj("intervalYear_vector"));
      assertEquals(one, c.getIntervalYearObj(0));
      c.getIntervalYear(0, holder1);
      c.getIntervalYear("intervalYear_vector", holder2);
      assertEquals(holder1.value, holder2.value);
      int intYear1 = c.getIntervalYear(0);
      int intYear2 = c.getIntervalYear("intervalYear_vector");
      assertEquals(2, intYear1);
      assertEquals(intYear1, intYear2);
    }
  }

  @Test
  void getBit() {
    List<FieldVector> vectors = new ArrayList<>();

    BitVector bitVector = new BitVector("bit_vector", allocator);
    NullableBitHolder holder1 = new NullableBitHolder();
    NullableBitHolder holder2 = new NullableBitHolder();

    vectors.add(bitVector);
    bitVector.setSafe(0, 0);
    bitVector.setSafe(1, 1);
    bitVector.setValueCount(2);

    int one = bitVector.get(1);
    try (Table t = new Table(vectors)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(one, c.getBit("bit_vector"));
      assertEquals(one, c.getBit(0));
      c.getBit(0, holder1);
      c.getBit("bit_vector", holder2);
      assertEquals(holder1.value, holder2.value);
    }
  }

  @Test
  void hasNext() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      assertTrue(c.hasNext());
      c.setPosition(1);
      assertFalse(c.hasNext());
    }
  }

  @Test
  void next() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(0);
      c.next();
      assertEquals(1, c.getRowNumber());
    }
  }

  @Test
  void isNull() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(0));
    }
  }

  @Test
  void isNullByFieldName() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void fixedWidthVectorTest() {
    List<FieldVector> vectorList = fixedWidthVectors(allocator, 2);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      // integer tests using vector name and index
      assertFalse(c.isNull("bigInt_vector"));
      assertEquals(c.getInt("int_vector"), c.getInt(0));
      assertEquals(c.getBigInt("bigInt_vector"), c.getBigInt(1));
      assertEquals(c.getSmallInt("smallInt_vector"), c.getSmallInt(2));
      assertEquals(c.getTinyInt("tinyInt_vector"), c.getTinyInt(3));

      // integer tests using Nullable Holders
      NullableIntHolder int4Holder = new NullableIntHolder();
      NullableTinyIntHolder int1Holder = new NullableTinyIntHolder();
      NullableSmallIntHolder int2Holder = new NullableSmallIntHolder();
      NullableBigIntHolder int8Holder = new NullableBigIntHolder();
      c.getInt(0, int4Holder);
      c.getBigInt(1, int8Holder);
      c.getSmallInt(2, int2Holder);
      c.getTinyInt(3, int1Holder);
      assertEquals(c.getInt("int_vector"), int4Holder.value);
      assertEquals(c.getBigInt("bigInt_vector"), int8Holder.value);
      assertEquals(c.getSmallInt("smallInt_vector"), int2Holder.value);
      assertEquals(c.getTinyInt("tinyInt_vector"), int1Holder.value);

      c.getInt("int_vector", int4Holder);
      c.getBigInt("bigInt_vector", int8Holder);
      c.getSmallInt("smallInt_vector", int2Holder);
      c.getTinyInt("tinyInt_vector", int1Holder);
      assertEquals(c.getInt("int_vector"), int4Holder.value);
      assertEquals(c.getBigInt("bigInt_vector"), int8Holder.value);
      assertEquals(c.getSmallInt("smallInt_vector"), int2Holder.value);
      assertEquals(c.getTinyInt("tinyInt_vector"), int1Holder.value);

      // uint tests using vector name and index
      assertEquals(c.getUInt1("uInt1_vector"), c.getUInt1(4));
      assertEquals(c.getUInt2("uInt2_vector"), c.getUInt2(5));
      assertEquals(c.getUInt4("uInt4_vector"), c.getUInt4(6));
      assertEquals(c.getUInt8("uInt8_vector"), c.getUInt8(7));

      // UInt tests using Nullable Holders
      NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
      NullableUInt1Holder uInt1Holder = new NullableUInt1Holder();
      NullableUInt2Holder uInt2Holder = new NullableUInt2Holder();
      NullableUInt8Holder uInt8Holder = new NullableUInt8Holder();
      // fill the holders using vector index and test
      c.getUInt1(4, uInt1Holder);
      c.getUInt2(5, uInt2Holder);
      c.getUInt4(6, uInt4Holder);
      c.getUInt8(7, uInt8Holder);
      assertEquals(c.getUInt1("uInt1_vector"), uInt1Holder.value);
      assertEquals(c.getUInt2("uInt2_vector"), uInt2Holder.value);
      assertEquals(c.getUInt4("uInt4_vector"), uInt4Holder.value);
      assertEquals(c.getUInt8("uInt8_vector"), uInt8Holder.value);

      // refill the holders using vector name and retest
      c.getUInt1("uInt1_vector", uInt1Holder);
      c.getUInt2("uInt2_vector", uInt2Holder);
      c.getUInt4("uInt4_vector", uInt4Holder);
      c.getUInt8("uInt8_vector", uInt8Holder);
      assertEquals(c.getUInt1("uInt1_vector"), uInt1Holder.value);
      assertEquals(c.getUInt2("uInt2_vector"), uInt2Holder.value);
      assertEquals(c.getUInt4("uInt4_vector"), uInt4Holder.value);
      assertEquals(c.getUInt8("uInt8_vector"), uInt8Holder.value);

      // tests floating point
      assertEquals(c.getFloat4("float4_vector"), c.getFloat4(8));
      assertEquals(c.getFloat8("float8_vector"), c.getFloat8(9));

      // floating point tests using Nullable Holders
      NullableFloat4Holder float4Holder = new NullableFloat4Holder();
      NullableFloat8Holder float8Holder = new NullableFloat8Holder();
      // fill the holders using vector index and test
      c.getFloat4(8, float4Holder);
      c.getFloat8(9, float8Holder);
      assertEquals(c.getFloat4("float4_vector"), float4Holder.value);
      assertEquals(c.getFloat8("float8_vector"), float8Holder.value);

      // refill the holders using vector name and retest
      c.getFloat4("float4_vector", float4Holder);
      c.getFloat8("float8_vector", float8Holder);
      assertEquals(c.getFloat4("float4_vector"), float4Holder.value);
      assertEquals(c.getFloat8("float8_vector"), float8Holder.value);

      // test time values using vector name versus vector index
      assertEquals(c.getTimeSec("timeSec_vector"), c.getTimeSec(10));
      assertEquals(c.getTimeMilli("timeMilli_vector"), c.getTimeMilli(11));
      assertEquals(c.getTimeMicro("timeMicro_vector"), c.getTimeMicro(12));
      assertEquals(c.getTimeNano("timeNano_vector"), c.getTimeNano(13));

      // time tests using Nullable Holders
      NullableTimeSecHolder timeSecHolder = new NullableTimeSecHolder();
      NullableTimeMilliHolder timeMilliHolder = new NullableTimeMilliHolder();
      NullableTimeMicroHolder timeMicroHolder = new NullableTimeMicroHolder();
      NullableTimeNanoHolder timeNanoHolder = new NullableTimeNanoHolder();
      // fill the holders using vector index and test
      c.getTimeSec(10, timeSecHolder);
      c.getTimeMilli(11, timeMilliHolder);
      c.getTimeMicro(12, timeMicroHolder);
      c.getTimeNano(13, timeNanoHolder);
      assertEquals(c.getTimeSec("timeSec_vector"), timeSecHolder.value);
      assertEquals(c.getTimeMilli("timeMilli_vector"), timeMilliHolder.value);
      assertEquals(c.getTimeMicro("timeMicro_vector"), timeMicroHolder.value);
      assertEquals(c.getTimeNano("timeNano_vector"), timeNanoHolder.value);

      LocalDateTime milliDT = c.getTimeMilliObj(11);
      assertNotNull(milliDT);
      assertEquals(milliDT, c.getTimeMilliObj("timeMilli_vector"));

      // refill the holders using vector name and retest
      c.getTimeSec("timeSec_vector", timeSecHolder);
      c.getTimeMilli("timeMilli_vector", timeMilliHolder);
      c.getTimeMicro("timeMicro_vector", timeMicroHolder);
      c.getTimeNano("timeNano_vector", timeNanoHolder);
      assertEquals(c.getTimeSec("timeSec_vector"), timeSecHolder.value);
      assertEquals(c.getTimeMilli("timeMilli_vector"), timeMilliHolder.value);
      assertEquals(c.getTimeMicro("timeMicro_vector"), timeMicroHolder.value);
      assertEquals(c.getTimeNano("timeNano_vector"), timeNanoHolder.value);

      assertEquals(c.getTimeStampSec("timeStampSec_vector"), c.getTimeStampSec(14));
      assertEquals(c.getTimeStampMilli("timeStampMilli_vector"), c.getTimeStampMilli(15));
      assertEquals(c.getTimeStampMicro("timeStampMicro_vector"), c.getTimeStampMicro(16));
      assertEquals(c.getTimeStampNano("timeStampNano_vector"), c.getTimeStampNano(17));

      // time stamp tests using Nullable Holders
      NullableTimeStampSecHolder timeStampSecHolder = new NullableTimeStampSecHolder();
      NullableTimeStampMilliHolder timeStampMilliHolder = new NullableTimeStampMilliHolder();
      NullableTimeStampMicroHolder timeStampMicroHolder = new NullableTimeStampMicroHolder();
      NullableTimeStampNanoHolder timeStampNanoHolder = new NullableTimeStampNanoHolder();
      // fill the holders using vector index and test
      c.getTimeStampSec(14, timeStampSecHolder);
      c.getTimeStampMilli(15, timeStampMilliHolder);
      c.getTimeStampMicro(16, timeStampMicroHolder);
      c.getTimeStampNano(17, timeStampNanoHolder);
      assertEquals(c.getTimeStampSec("timeStampSec_vector"), timeStampSecHolder.value);
      assertEquals(c.getTimeStampMilli("timeStampMilli_vector"), timeStampMilliHolder.value);
      assertEquals(c.getTimeStampMicro("timeStampMicro_vector"), timeStampMicroHolder.value);
      assertEquals(c.getTimeStampNano("timeStampNano_vector"), timeStampNanoHolder.value);

      LocalDateTime secDT = c.getTimeStampSecObj(14);
      assertNotNull(secDT);
      assertEquals(secDT, c.getTimeStampSecObj("timeStampSec_vector"));

      LocalDateTime milliDT1 = c.getTimeStampMilliObj(15);
      assertNotNull(milliDT1);
      assertEquals(milliDT1, c.getTimeStampMilliObj("timeStampMilli_vector"));

      LocalDateTime microDT = c.getTimeStampMicroObj(16);
      assertNotNull(microDT);
      assertEquals(microDT, c.getTimeStampMicroObj("timeStampMicro_vector"));

      LocalDateTime nanoDT = c.getTimeStampNanoObj(17);
      assertNotNull(nanoDT);
      assertEquals(nanoDT, c.getTimeStampNanoObj("timeStampNano_vector"));

      // refill the holders using vector name and retest
      c.getTimeStampSec("timeStampSec_vector", timeStampSecHolder);
      c.getTimeStampMilli("timeStampMilli_vector", timeStampMilliHolder);
      c.getTimeStampMicro("timeStampMicro_vector", timeStampMicroHolder);
      c.getTimeStampNano("timeStampNano_vector", timeStampNanoHolder);
      assertEquals(c.getTimeStampSec("timeStampSec_vector"), timeStampSecHolder.value);
      assertEquals(c.getTimeStampMilli("timeStampMilli_vector"), timeStampMilliHolder.value);
      assertEquals(c.getTimeStampMicro("timeStampMicro_vector"), timeStampMicroHolder.value);
      assertEquals(c.getTimeStampNano("timeStampNano_vector"), timeStampNanoHolder.value);
    }
  }

  @Test
  void timestampsWithTimezones() {
    List<FieldVector> vectorList = timezoneTemporalVectors(allocator, 2);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);

      assertEquals(c.getTimeStampSecTZ("timeStampSecTZ_vector"), c.getTimeStampSecTZ(0));
      assertEquals(c.getTimeStampMilliTZ("timeStampMilliTZ_vector"), c.getTimeStampMilliTZ(1));
      assertEquals(c.getTimeStampMicroTZ("timeStampMicroTZ_vector"), c.getTimeStampMicroTZ(2));
      assertEquals(c.getTimeStampNanoTZ("timeStampNanoTZ_vector"), c.getTimeStampNanoTZ(3));

      // time stamp tests using Nullable Holders
      NullableTimeStampSecTZHolder timeStampSecHolder = new NullableTimeStampSecTZHolder();
      NullableTimeStampMilliTZHolder timeStampMilliHolder = new NullableTimeStampMilliTZHolder();
      NullableTimeStampMicroTZHolder timeStampMicroHolder = new NullableTimeStampMicroTZHolder();
      NullableTimeStampNanoTZHolder timeStampNanoHolder = new NullableTimeStampNanoTZHolder();

      // fill the holders using vector index and test
      c.getTimeStampSecTZ(0, timeStampSecHolder);
      c.getTimeStampMilliTZ(1, timeStampMilliHolder);
      c.getTimeStampMicroTZ(2, timeStampMicroHolder);
      c.getTimeStampNanoTZ(3, timeStampNanoHolder);

      long tsSec = timeStampSecHolder.value;
      long tsMil = timeStampMilliHolder.value;
      long tsMic = timeStampMicroHolder.value;
      long tsNan = timeStampNanoHolder.value;

      assertEquals(c.getTimeStampSecTZ("timeStampSecTZ_vector"), timeStampSecHolder.value);
      assertEquals(c.getTimeStampMilliTZ("timeStampMilliTZ_vector"), timeStampMilliHolder.value);
      assertEquals(c.getTimeStampMicroTZ("timeStampMicroTZ_vector"), timeStampMicroHolder.value);
      assertEquals(c.getTimeStampNanoTZ("timeStampNanoTZ_vector"), timeStampNanoHolder.value);

      // fill the holders using vector index and test
      c.getTimeStampSecTZ("timeStampSecTZ_vector", timeStampSecHolder);
      c.getTimeStampMilliTZ("timeStampMilliTZ_vector", timeStampMilliHolder);
      c.getTimeStampMicroTZ("timeStampMicroTZ_vector", timeStampMicroHolder);
      c.getTimeStampNanoTZ("timeStampNanoTZ_vector", timeStampNanoHolder);

      assertEquals(tsSec, timeStampSecHolder.value);
      assertEquals(tsMil, timeStampMilliHolder.value);
      assertEquals(tsMic, timeStampMicroHolder.value);
      assertEquals(tsNan, timeStampNanoHolder.value);
    }
  }

  @Test
  void getVarChar() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(c.getVarCharObj(1), "two");
      assertEquals(c.getVarCharObj(1), c.getVarCharObj(VARCHAR_VECTOR_NAME_1));
      assertArrayEquals("two".getBytes(), c.getVarChar(VARCHAR_VECTOR_NAME_1));
      assertArrayEquals("two".getBytes(), c.getVarChar(1));
    }
  }

  @Test
  void getVarBinary() {
    List<FieldVector> vectorList = intPlusVarBinaryColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertArrayEquals(c.getVarBinary(1), "two".getBytes());
      assertArrayEquals(c.getVarBinary(1), c.getVarBinary(VARBINARY_VECTOR_NAME_1));
    }
  }

  @Test
  void getLargeVarBinary() {
    List<FieldVector> vectorList = intPlusLargeVarBinaryColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertArrayEquals(c.getLargeVarBinary(1), "two".getBytes());
      assertArrayEquals(c.getLargeVarBinary(1), c.getLargeVarBinary(VARBINARY_VECTOR_NAME_1));
    }
  }

  @Test
  void getLargeVarChar() {
    List<FieldVector> vectorList = intPlusLargeVarcharColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertEquals(c.getLargeVarCharObj(1), "two");
      assertEquals(c.getLargeVarCharObj(1), c.getLargeVarCharObj(VARCHAR_VECTOR_NAME_1));
      assertArrayEquals("two".getBytes(), c.getLargeVarChar(VARCHAR_VECTOR_NAME_1));
      assertArrayEquals("two".getBytes(), c.getLargeVarChar(1));
    }
  }

  @Test
  void getFixedBinary() {
    List<FieldVector> vectorList = intPlusFixedBinaryColumns(allocator);
    try (Table t = new Table(vectorList)) {
      Row c = t.immutableRow();
      c.setPosition(1);
      assertArrayEquals(c.getFixedSizeBinary(1), "two".getBytes());
      assertArrayEquals(c.getFixedSizeBinary(1), c.getFixedSizeBinary(FIXEDBINARY_VECTOR_NAME_1));
    }
  }

  @Test
  void testSimpleListVector1() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<Integer> list = (List<Integer>) c.getList(INT_LIST_VECTOR_NAME);
        assertEquals(10, list.size());
      }
    }
  }

  @Test
  void testSimpleListVector2() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<Integer> list = (List<Integer>) c.getList(0);
        assertEquals(10, list.size());
      }
    }
  }

  @Test
  void testSimpleStructVector1() {
    try (StructVector structVector = simpleStructVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(structVector);
        Table table = new Table(vectorSchemaRoot)) {
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        JsonStringHashMap<String, ?> struct =
            (JsonStringHashMap<String, ?>) c.getStruct(STRUCT_VECTOR_NAME);
        @SuppressWarnings("unchecked")
        JsonStringHashMap<String, ?> struct1 =
            (JsonStringHashMap<String, ?>) c.getStruct(0);
        int a = (int) struct.get("struct_int_child");
        double b = (double) struct.get("struct_flt_child");
        int a1 = (int) struct1.get("struct_int_child");
        double b1 = (double) struct1.get("struct_flt_child");
        assertNotNull(struct);
        assertEquals(a, a1);
        assertEquals(b, b1);
        assertTrue(a >= 0);
        assertTrue(b <= a, String.format("a = %s and b = %s", a, b));
      }
    }
  }

  @Test
  void testSimpleUnionVector() {
    try (UnionVector unionVector = simpleUnionVector(allocator);
        VectorSchemaRoot vsr = VectorSchemaRoot.of(unionVector);
        Table table = new Table(vsr)) {
      Row c = table.immutableRow();
      c.setPosition(0);
      Object object0 = c.getUnion(UNION_VECTOR_NAME);
      Object object1 = c.getUnion(0);
      assertEquals(object0, object1);
      c.setPosition(1);
      assertNull(c.getUnion(UNION_VECTOR_NAME));
      c.setPosition(2);
      Object object2 = c.getUnion(UNION_VECTOR_NAME);
      assertEquals(100, object0);
      assertEquals(100, object2);
    }
  }

  @Test
  void testSimpleDenseUnionVector() {
    try (DenseUnionVector unionVector = simpleDenseUnionVector(allocator);
        VectorSchemaRoot vsr = VectorSchemaRoot.of(unionVector);
        Table table = new Table(vsr)) {
      Row c = table.immutableRow();
      c.setPosition(0);
      Object object0 = c.getDenseUnion(UNION_VECTOR_NAME);
      Object object1 = c.getDenseUnion(0);
      assertEquals(object0, object1);
      c.setPosition(1);
      assertNull(c.getDenseUnion(UNION_VECTOR_NAME));
      c.setPosition(2);
      Object object2 = c.getDenseUnion(UNION_VECTOR_NAME);
      assertEquals(100, object0);
      assertEquals(100, object2);
    }
  }

  @Test
  void testExtensionTypeVector() {
    TestExtensionType.LocationVector vector = new TestExtensionType.LocationVector("location", allocator);
    vector.allocateNew();
    vector.set(0, 34.073814f, -118.240784f);
    vector.setValueCount(1);

    try (VectorSchemaRoot vsr = VectorSchemaRoot.of(vector);
         Table table = new Table(vsr)) {
      Row c = table.immutableRow();
      c.setPosition(0);
      Object object0 = c.getExtensionType("location");
      Object object1 = c.getExtensionType(0);
      assertEquals(object0, object1);
      @SuppressWarnings("unchecked")
     JsonStringHashMap<String, ?> struct0 =
          (JsonStringHashMap<String, ?>) object0;
      assertEquals(34.073814f, struct0.get("Latitude"));
    }
  }

  @Test
  void testSimpleMapVector1() {
    try (MapVector mapVector = simpleMapVector(allocator);
        Table table = Table.of(mapVector)) {

      int i = 1;
      for (Row c : table) {
        @SuppressWarnings("unchecked")
        List<JsonStringHashMap<String, ?>> list =
            (List<JsonStringHashMap<String, ?>>) c.getMap(BIGINT_INT_MAP_VECTOR_NAME);
        @SuppressWarnings("unchecked")
        List<JsonStringHashMap<String, ?>> list1 =
            (List<JsonStringHashMap<String, ?>>) c.getMap(0);
        for (int j = 0; j < list1.size(); j++) {
          assertEquals(list.get(j), list1.get(j));
        }
        if (list != null && !list.isEmpty()) {
          assertEquals(i, list.size());
          for (JsonStringHashMap<String, ?> sv : list) {
            assertEquals(2, sv.size());
            Long o1 = (Long) sv.get("key");
            Integer o2 = (Integer) sv.get("value");
            assertEquals(o1, o2.longValue());
          }
        }
        i++;
      }
    }
  }

  @Test
  void resetPosition() {
    try (ListVector listVector = simpleListVector(allocator);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(listVector);
        Table table = new Table(vectorSchemaRoot)) {
      Row row = table.immutableRow();
      row.next();
      assertEquals(0, row.rowNumber);
      row.resetPosition();
      assertEquals(-1, row.rowNumber);
    }
  }
}
