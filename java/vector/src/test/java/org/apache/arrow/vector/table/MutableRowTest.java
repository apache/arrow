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

import static org.apache.arrow.vector.table.TestUtils.INT_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.VARCHAR_VECTOR_NAME_1;
import static org.apache.arrow.vector.table.TestUtils.intPlusVarcharColumns;
import static org.apache.arrow.vector.table.TestUtils.intervalVectors;
import static org.apache.arrow.vector.table.TestUtils.numericVectors;
import static org.apache.arrow.vector.table.TestUtils.simpleTemporalVectors;
import static org.apache.arrow.vector.table.TestUtils.twoIntColumns;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
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
import org.apache.arrow.vector.holders.NullableTimeStampMilliHolder;
import org.apache.arrow.vector.holders.NullableTimeStampNanoHolder;
import org.apache.arrow.vector.holders.NullableTimeStampSecHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.holders.NullableUInt1Holder;
import org.apache.arrow.vector.holders.NullableUInt2Holder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableUInt8Holder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MutableRowTest {

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Test
  void constructor() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      Row c = t.mutableRow();
      assertEquals(StandardCharsets.UTF_8, c.getDefaultCharacterSet());
    }
  }

  @Test
  void at() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      Row c = t.immutableRow();
      assertEquals(c.getRowNumber(), -1);
      c.setPosition(1);
      assertEquals(c.getRowNumber(), 1);
      assertEquals(2, c.getInt(0));
    }
  }

  @Test
  void setNullByColumnIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(0));
      c.setNull(0);
      assertTrue(c.isNull(0));
    }
  }

  @Test
  void setNullByColumnName() {
    // TODO: Test setNull using complex types (and dictionaries?)
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertFalse(c.isNull(INT_VECTOR_NAME_1));
      c.setNull(INT_VECTOR_NAME_1);
      assertTrue(c.isNull(INT_VECTOR_NAME_1));
    }
  }

  @Test
  void setIntByColumnIndex() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertNotEquals(132, c.getInt(0));
      c.setInt(0, 132).setInt(1, 146);
      assertEquals(132, c.getInt(0));
      assertEquals(146, c.getInt(1));
    }
  }

  @Test
  void setTinyInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTinyInt("tinyInt_vector", (byte) 1);
      assertEquals(1, c.getTinyInt("tinyInt_vector"));
      c.setTinyInt("tinyInt_vector", (byte) 2);
      assertEquals(2, c.getTinyInt("tinyInt_vector"));
      c.setTinyInt(3, (byte) 3);
      assertEquals(3, c.getTinyInt("tinyInt_vector"));

      // test with holder
      NullableTinyIntHolder holder = new NullableTinyIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTinyInt("tinyInt_vector", holder);
      assertEquals(4, c.getTinyInt("tinyInt_vector"));
      holder.value = 5;
      c.setTinyInt(3, holder);
      assertEquals(5, c.getTinyInt(3));
    }
  }

  @Test
  void setUint1() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt1("uInt1_vector", (byte) 1);
      assertEquals(1, c.getUInt1("uInt1_vector"));
      c.setUInt1("uInt1_vector", (byte) 2);
      assertEquals(2, c.getUInt1("uInt1_vector"));
      c.setUInt1(4, (byte) 3);
      assertEquals(3, c.getUInt1("uInt1_vector"));

      // test with holder
      NullableUInt1Holder holder = new NullableUInt1Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt1("uInt1_vector", holder);
      assertEquals(4, c.getUInt1("uInt1_vector"));
      holder.value = 5;
      c.setUInt1(4, holder);
      assertEquals(5, c.getUInt1(4));
    }
  }

  @Test
  void setSmallInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setSmallInt("smallInt_vector", (short) 1);
      assertEquals(1, c.getSmallInt("smallInt_vector"));
      c.setSmallInt("smallInt_vector", (short) 2);
      assertEquals(2, c.getSmallInt("smallInt_vector"));
      c.setSmallInt(2, (short) 3);
      assertEquals(3, c.getSmallInt("smallInt_vector"));

      // test with holder
      NullableSmallIntHolder holder = new NullableSmallIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setSmallInt("smallInt_vector", holder);
      assertEquals(4, c.getSmallInt("smallInt_vector"));
      holder.value = 5;
      c.setSmallInt(2, holder);
      assertEquals(5, c.getSmallInt(2));
    }
  }

  @Test
  void setUint2() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt2("uInt2_vector", (short) 1);
      assertEquals(1, c.getUInt2("uInt2_vector"));
      c.setUInt2("uInt2_vector", (short) 2);
      assertEquals(2, c.getUInt2("uInt2_vector"));
      c.setUInt2(5, (short) 3);
      assertEquals(3, c.getUInt2("uInt2_vector"));

      // test with holder
      NullableUInt2Holder holder = new NullableUInt2Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt2("uInt2_vector", holder);
      assertEquals(4, c.getUInt2("uInt2_vector"));
      holder.value = 5;
      c.setUInt2(5, holder);
      assertEquals(5, c.getUInt2(5));
    }
  }

  @Test
  void setBigInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setBigInt("bigInt_vector", 1);
      assertEquals(1, c.getBigInt("bigInt_vector"));
      c.setBigInt("bigInt_vector", 2);
      assertEquals(2, c.getBigInt("bigInt_vector"));
      c.setBigInt(1, 3);
      assertEquals(3, c.getBigInt("bigInt_vector"));

      // test with holder
      NullableBigIntHolder holder = new NullableBigIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setBigInt("bigInt_vector", holder);
      assertEquals(4, c.getBigInt("bigInt_vector"));
      holder.value = 5;
      c.setBigInt(1, holder);
      assertEquals(5, c.getBigInt(1));
    }
  }

  @Test
  void setUint8() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt8("uInt8_vector", 1);
      assertEquals(1, c.getUInt8("uInt8_vector"));
      c.setUInt8("uInt8_vector", 2);
      assertEquals(2, c.getUInt8("uInt8_vector"));
      c.setUInt8(7, 3);
      assertEquals(3, c.getUInt8("uInt8_vector"));

      // test with holder
      NullableUInt8Holder holder = new NullableUInt8Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt8("uInt8_vector", holder);
      assertEquals(4, c.getUInt8("uInt8_vector"));
      holder.value = 5;
      c.setUInt8(7, holder);
      assertEquals(5, c.getUInt8(7));
    }
  }

  @Test
  void setInt() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setInt("int_vector", 1);
      assertEquals(1, c.getInt("int_vector"));
      c.setInt("int_vector", 2);
      assertEquals(2, c.getInt("int_vector"));
      c.setInt(0, 3);
      assertEquals(3, c.getInt("int_vector"));

      // test with holder
      NullableIntHolder holder = new NullableIntHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setInt("int_vector", holder);
      assertEquals(4, c.getInt("int_vector"));
      holder.value = 5;
      c.setInt(0, holder);
      assertEquals(5, c.getInt(0));
    }
  }

  @Test
  void setUint4() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setUInt4("uInt4_vector", 1);
      assertEquals(1, c.getUInt4("uInt4_vector"));
      c.setUInt4("uInt4_vector", 2);
      assertEquals(2, c.getUInt4("uInt4_vector"));
      c.setUInt4(6, 3);
      assertEquals(3, c.getUInt4("uInt4_vector"));

      // test with holder
      NullableUInt4Holder holder = new NullableUInt4Holder();
      holder.value = 4;
      holder.isSet = 1;
      c.setUInt4("uInt4_vector", holder);
      assertEquals(4, c.getUInt4("uInt4_vector"));
      holder.value = 5;
      c.setUInt4(6, holder);
      assertEquals(5, c.getUInt4(6));
    }
  }

  @Test
  void setFloat4() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setFloat4("float4_vector", 1.1f);
      assertEquals(1.1, c.getFloat4("float4_vector"), 0.0001);
      c.setFloat4("float4_vector", 2.2f);
      assertEquals(2.2, c.getFloat4("float4_vector"), 0.0001);
      c.setFloat4(8, 3.2f);
      assertEquals(3.2, c.getFloat4("float4_vector"), 0.0001);

      // test with holder
      NullableFloat4Holder holder = new NullableFloat4Holder();
      holder.value = 4.4f;
      holder.isSet = 1;
      c.setFloat4("float4_vector", holder);
      assertEquals(4.4, c.getFloat4("float4_vector"), 0.0001);
      holder.value = 5.5f;
      c.setFloat4(8, holder);
      assertEquals(5.5, c.getFloat4(8), 0.0001);
    }
  }

  @Test
  void setFloat8() {
    List<FieldVector> vectorList = numericVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setFloat8("float8_vector", 1.1);
      assertEquals(1.1, c.getFloat8("float8_vector"));
      c.setFloat8("float8_vector", 2.2);
      assertEquals(2.2, c.getFloat8("float8_vector"));
      c.setFloat8(9, 3.2);
      assertEquals(3.2, c.getFloat8("float8_vector"));

      // test with holder
      NullableFloat8Holder holder = new NullableFloat8Holder();
      holder.value = 4.4;
      holder.isSet = 1;
      c.setFloat8("float8_vector", holder);
      assertEquals(4.4, c.getFloat8("float8_vector"));
      holder.value = 5.5;
      c.setFloat8(9, holder);
      assertEquals(5.5, c.getFloat8(9));
    }
  }

  @Test
  void setTimeSec() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeSec("timeSec_vector", 1);
      assertEquals(1, c.getTimeSec("timeSec_vector"));
      c.setTimeSec("timeSec_vector", 2);
      assertEquals(2, c.getTimeSec("timeSec_vector"));
      c.setTimeSec(0, 3);
      assertEquals(3, c.getTimeSec("timeSec_vector"));

      // test with holder
      NullableTimeSecHolder holder = new NullableTimeSecHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeSec("timeSec_vector", holder);
      assertEquals(4, c.getTimeSec("timeSec_vector"));
      holder.value = 5;
      c.setTimeSec(0, holder);
      assertEquals(5, c.getTimeSec(0));
    }
  }

  @Test
  void setTimeMilli() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeMilli("timeMilli_vector", 1);
      assertEquals(1, c.getTimeMilli("timeMilli_vector"));
      c.setTimeMilli("timeMilli_vector", 2);
      assertEquals(2, c.getTimeMilli("timeMilli_vector"));
      c.setTimeMilli(1, 3);
      assertEquals(3, c.getTimeMilli("timeMilli_vector"));

      // test with holder
      NullableTimeMilliHolder holder = new NullableTimeMilliHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeMilli("timeMilli_vector", holder);
      assertEquals(4, c.getTimeMilli("timeMilli_vector"));
      holder.value = 5;
      c.setTimeMilli(1, holder);
      assertEquals(5, c.getTimeMilli(1));
    }
  }

  @Test
  void setTimeMicro() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeMicro("timeMicro_vector", 1);
      assertEquals(1, c.getTimeMicro("timeMicro_vector"));
      c.setTimeMicro("timeMicro_vector", 2);
      assertEquals(2, c.getTimeMicro("timeMicro_vector"));
      c.setTimeMicro(2, 3);
      assertEquals(3, c.getTimeMicro("timeMicro_vector"));

      // test with holder
      NullableTimeMicroHolder holder = new NullableTimeMicroHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeMicro("timeMicro_vector", holder);
      assertEquals(4, c.getTimeMicro("timeMicro_vector"));
      holder.value = 5;
      c.setTimeMicro(2, holder);
      assertEquals(5, c.getTimeMicro(2));
    }
  }

  @Test
  void setTimeNano() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeNano("timeNano_vector", 1);
      assertEquals(1, c.getTimeNano("timeNano_vector"));
      c.setTimeNano("timeNano_vector", 2);
      assertEquals(2, c.getTimeNano("timeNano_vector"));
      c.setTimeNano(3, 3);
      assertEquals(3, c.getTimeNano("timeNano_vector"));

      // test with holder
      NullableTimeNanoHolder holder = new NullableTimeNanoHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeNano("timeNano_vector", holder);
      assertEquals(4, c.getTimeNano("timeNano_vector"));
      holder.value = 5;
      c.setTimeNano(3, holder);
      assertEquals(5, c.getTimeNano(3));
    }
  }

  @Test
  void setTimeStampSec() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    int vectorPosition = 4;
    String vectorName = "timeStampSec_vector";

    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeStampSec(vectorName, 1);
      assertEquals(1, c.getTimeStampSec(vectorName));
      c.setTimeStampSec(vectorName, 2);
      assertEquals(2, c.getTimeStampSec(vectorName));
      c.setTimeStampSec(vectorPosition, 3);
      assertEquals(3, c.getTimeStampSec(vectorName));

      // test with holder
      NullableTimeStampSecHolder holder = new NullableTimeStampSecHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeStampSec(vectorName, holder);
      assertEquals(4, c.getTimeStampSec(vectorName));
      holder.value = 5;
      c.setTimeStampSec(vectorPosition, holder);
      assertEquals(5, c.getTimeStampSec(vectorPosition));
    }
  }

  @Test
  void setTimeStampMilli() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    int vectorPosition = 5;
    String vectorName = "timeStampMilli_vector";

    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeStampMilli(vectorName, 1);
      assertEquals(1, c.getTimeStampMilli(vectorName));
      c.setTimeStampMilli(vectorName, 2);
      assertEquals(2, c.getTimeStampMilli(vectorName));
      c.setTimeStampMilli(vectorPosition, 3);
      assertEquals(3, c.getTimeStampMilli(vectorName));

      // test with holder
      NullableTimeStampMilliHolder holder = new NullableTimeStampMilliHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeStampMilli(vectorName, holder);
      assertEquals(4, c.getTimeStampMilli(vectorName));
      holder.value = 5;
      c.setTimeStampMilli(vectorPosition, holder);
      assertEquals(5, c.getTimeStampMilli(vectorPosition));
    }
  }

  @Test
  void setTimeStampMicro() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    int vectorPosition = 6;
    String vectorName = "timeStampMicro_vector";

    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeStampMicro(vectorName, 1);
      assertEquals(1, c.getTimeStampMicro(vectorName));
      c.setTimeStampMicro(vectorName, 2);
      assertEquals(2, c.getTimeStampMicro(vectorName));
      c.setTimeStampMicro(vectorPosition, 3);
      assertEquals(3, c.getTimeStampMicro(vectorName));

      // test with holder
      NullableTimeStampMicroHolder holder = new NullableTimeStampMicroHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeStampMicro(vectorName, holder);
      assertEquals(4, c.getTimeStampMicro(vectorName));
      holder.value = 5;
      c.setTimeStampMicro(vectorPosition, holder);
      assertEquals(5, c.getTimeStampMicro(vectorPosition));
    }
  }

  @Test
  void setTimeStampNano() {
    List<FieldVector> vectorList = simpleTemporalVectors(allocator, 2);
    int vectorPosition = 7;
    String vectorName = "timeStampNano_vector";

    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setTimeStampNano(vectorName, 1);
      assertEquals(1, c.getTimeStampNano(vectorName));
      c.setTimeStampNano(vectorName, 2);
      assertEquals(2, c.getTimeStampNano(vectorName));
      c.setTimeStampNano(vectorPosition, 3);
      assertEquals(3, c.getTimeStampNano(vectorName));

      // test with holder
      NullableTimeStampNanoHolder holder = new NullableTimeStampNanoHolder();
      holder.value = 4;
      holder.isSet = 1;
      c.setTimeStampNano(vectorName, holder);
      assertEquals(4, c.getTimeStampNano(vectorName));
      holder.value = 5;
      c.setTimeStampNano(vectorPosition, holder);
      assertEquals(5, c.getTimeStampNano(vectorPosition));
    }
  }

  @Test
  void setIntervalDay() {
    String vectorName = "intervalDay_vector";
    int vectorPosition = 0;
    List<FieldVector> vectorList = intervalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setIntervalDay(vectorName, 1, 11);
      assertEquals(Duration.ofDays(1).plusMillis(11), c.getIntervalDayObj(vectorName));
      c.setIntervalDay(vectorName, 2, 22);
      assertEquals(Duration.ofDays(2).plusMillis(22), c.getIntervalDayObj(vectorName));
      c.setIntervalDay(vectorPosition, 3, 33);
      assertEquals(Duration.ofDays(3).plusMillis(33), c.getIntervalDayObj(vectorName));

      // test with holder
      NullableIntervalDayHolder holder = new NullableIntervalDayHolder();
      holder.days = 4;
      holder.isSet = 1;
      c.setIntervalDay(vectorName, holder);
      assertEquals(Duration.ofDays(4), c.getIntervalDayObj(vectorName));
      holder.days = 5;
      c.setIntervalDay(vectorPosition, holder);
      assertEquals(Duration.ofDays(5), c.getIntervalDayObj(vectorPosition));
    }
  }

  @Test
  void setDuration() {
    String vectorName = "duration_vector";
    int vectorPosition = 3;
    List<FieldVector> vectorList = intervalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setDuration(vectorName, 11_111);
      assertEquals(Duration.ofSeconds(11_111), c.getDurationObj(vectorName));
      c.setDuration(vectorName, 22_222);
      assertEquals(Duration.ofSeconds(22_222), c.getDurationObj(vectorName));
      c.setDuration(vectorPosition, 33_333);
      assertEquals(Duration.ofSeconds(33_333), c.getDurationObj(vectorName));

      // test with holder
      NullableDurationHolder holder = new NullableDurationHolder();
      holder.value = 4;
      holder.unit = TimeUnit.SECOND;
      holder.isSet = 1;
      c.setDuration(vectorName, holder);
      assertEquals(Duration.ofSeconds(4), c.getDurationObj(vectorName));
      holder.value = 5;
      c.setDuration(vectorPosition, holder);
      assertEquals(Duration.ofSeconds(5), c.getDurationObj(vectorPosition));
    }
  }

  @Test
  void setIntervalYear() {
    String vectorName = "intervalYear_vector";
    int vectorPosition = 1;
    List<FieldVector> vectorList = intervalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      c.setIntervalYear(vectorName, 1);
      assertEquals(1, c.getIntervalYear(vectorName));
      c.setIntervalYear(vectorName, 2);
      assertEquals(2, c.getIntervalYear(vectorName));
      c.setIntervalYear(vectorPosition, 3);
      assertEquals(3, c.getIntervalYear(vectorName));

      // test with holder
      NullableIntervalYearHolder holder = new NullableIntervalYearHolder();
      holder.value = 24;
      holder.isSet = 1;

      c.setIntervalYear(vectorName, holder);
      assertEquals(Period.ofMonths(24), c.getIntervalYearObj(vectorName));
      holder.value = 36;
      c.setIntervalYear(vectorPosition, holder);
      assertEquals(Period.ofMonths(36), c.getIntervalYearObj(vectorPosition));
    }
  }

  @Test
  void setIntervalMonthDayNano() {
    String vectorName = "intervalMonthDayNano_vector";
    int vectorPosition = 2;
    List<FieldVector> vectorList = intervalVectors(allocator, 2);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(1);

      PeriodDuration periodDurationA = new PeriodDuration(Period.ofMonths(6).plusDays(3), Duration.ofNanos(303));
      PeriodDuration periodDurationB = new PeriodDuration(Period.ofMonths(7).plusDays(4), Duration.ofNanos(304));
      PeriodDuration periodDurationC = new PeriodDuration(Period.ofMonths(8).plusDays(5), Duration.ofNanos(305));

      c.setIntervalMonthDayNano(vectorName, 6, 3, 303);
      assertEquals(periodDurationA, c.getIntervalMonthDayNanoObj(vectorName));
      c.setIntervalMonthDayNano(vectorName, 7, 4, 304);
      assertEquals(periodDurationB, c.getIntervalMonthDayNanoObj(vectorName));
      c.setIntervalMonthDayNano(vectorName, 8, 5, 305);
      assertEquals(periodDurationC, c.getIntervalMonthDayNanoObj(vectorName));

      // test with holder
      NullableIntervalMonthDayNanoHolder holder = new NullableIntervalMonthDayNanoHolder();
      holder.months = 24;
      holder.days = 12;
      holder.nanoseconds = 101;
      holder.isSet = 1;

      c.setIntervalMonthDayNano(vectorName, holder);
      PeriodDuration periodDuration1 = new PeriodDuration(Period.ofMonths(24).plusDays(12), Duration.ofNanos(101));
      assertEquals(periodDuration1, c.getIntervalMonthDayNanoObj(vectorName));
      holder.months = 48;
      holder.days = 24;
      holder.nanoseconds = 202;
      PeriodDuration periodDuration2 = new PeriodDuration(Period.ofMonths(48).plusDays(24), Duration.ofNanos(202));
      c.setIntervalMonthDayNano(vectorPosition, holder);
      assertEquals(periodDuration2, c.getIntervalMonthDayNanoObj(vectorPosition));
    }
  }

  @Test
  void setVarCharByColumnIndex() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(1));
      c.setVarChar(1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(1));
    }
  }

  @Test
  void setVarCharByColumnName() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));
      c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));

      // ensure iteration works correctly
      List<String> values = new ArrayList<>();
      c.resetPosition();
      while (c.hasNext()) {
        c.next();
        values.add(new String(c.getVarChar(VARCHAR_VECTOR_NAME_1)));
      }
      assertTrue(values.contains("one"));
      assertTrue(values.contains("2"));
      assertEquals(2, values.size());
    }
  }

  @Test
  void setVarCharByColumnNameUsingDictionary() {
    List<FieldVector> vectorList = intPlusVarcharColumns(allocator);
    VarCharVector v1 = (VarCharVector) vectorList.get(1);
    Dictionary numbersDictionary = new Dictionary(v1,
        new DictionaryEncoding(1L, false, new ArrowType.Int(8, true)));

    try (MutableTable t = new MutableTable(vectorList)) {
      assertEquals(2, t.rowCount);
      MutableRow c = t.mutableRow();
      c.setPosition(1);
      assertEquals(2, c.getInt(0));
      assertEquals("two", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));
      c.setVarChar(VARCHAR_VECTOR_NAME_1, "2");
      c.setPosition(1);
      assertTrue(c.isRowDeleted());
      c.setPosition(2);
      assertEquals("2", c.getVarCharObj(VARCHAR_VECTOR_NAME_1));

      // ensure iteration works correctly
      List<String> values = new ArrayList<>();
      c.resetPosition();
      while (c.hasNext()) {
        c.next();
        values.add(new String(c.getVarChar(VARCHAR_VECTOR_NAME_1)));
      }
      assertTrue(values.contains("one"));
      assertTrue(values.contains("2"));
      assertEquals(2, values.size());
    }
  }

  @Test
  void delete() {
    List<FieldVector> vectorList = twoIntColumns(allocator);
    try (MutableTable t = new MutableTable(vectorList)) {
      MutableRow c = t.mutableRow();
      c.setPosition(0);
      assertFalse(c.isRowDeleted());
      c.deleteCurrentRow();
      assertTrue(c.isRowDeleted());
      assertTrue(t.isRowDeleted(0));
    }
  }

  @Test
  void compact() {
    // TODO: Implement
  }
}
