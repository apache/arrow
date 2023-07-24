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

package org.apache.arrow.adapter.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.arrow.adapter.jdbc.binder.ColumnBinder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JdbcParameterBinderTest {
  private static final long MILLIS_PER_DAY = 86_400_000;
  BufferAllocator allocator;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
  }

  @AfterEach
  void afterEach() {
    allocator.close();
  }

  @Test
  void bindOrder() throws SQLException {
    final Schema schema =
        new Schema(
            Arrays.asList(
                Field.nullable("ints0", new ArrowType.Int(32, true)),
                Field.nullable("ints1", new ArrowType.Int(32, true)),
                Field.nullable("ints2", new ArrowType.Int(32, true))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root)
              .bind(/*paramIndex=*/ 1, /*colIndex=*/ 2)
              .bind(/*paramIndex=*/ 2, /*colIndex=*/ 0)
              .build();
      assertThat(binder.next()).isFalse();

      final IntVector ints0 = (IntVector) root.getVector(0);
      final IntVector ints1 = (IntVector) root.getVector(1);
      final IntVector ints2 = (IntVector) root.getVector(2);
      ints0.setSafe(0, 4);
      ints0.setNull(1);
      ints1.setNull(0);
      ints1.setSafe(1, -8);
      ints2.setNull(0);
      ints2.setSafe(1, 12);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParamValue(2)).isEqualTo(4);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(12);
      assertThat(statement.getParamValue(2)).isNull();
      assertThat(statement.getParamType(2)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isFalse();

      binder.reset();

      ints0.setNull(0);
      ints0.setSafe(1, -2);
      ints2.setNull(0);
      ints2.setSafe(1, 6);
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(Types.INTEGER);
      assertThat(statement.getParamValue(2)).isNull();
      assertThat(statement.getParamType(2)).isEqualTo(Types.INTEGER);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(6);
      assertThat(statement.getParamValue(2)).isEqualTo(-2);
      assertThat(statement.getParam(3)).isNull();
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void customBinder() throws SQLException {
    final Schema schema =
        new Schema(Collections.singletonList(
            Field.nullable("ints0", new ArrowType.Int(32, true))));

    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root)
              .bind(
                  /*paramIndex=*/ 1,
                  new ColumnBinder() {
                    private final IntVector vector = (IntVector) root.getVector(0);
                    @Override
                    public void bind(PreparedStatement statement, int parameterIndex, int rowIndex)
                        throws SQLException {
                      Integer value = vector.getObject(rowIndex);
                      if (value == null) {
                        statement.setString(parameterIndex, "null");
                      } else {
                        statement.setString(parameterIndex, Integer.toString(value));
                      }
                    }

                    @Override
                    public int getJdbcType() {
                      return Types.INTEGER;
                    }

                    @Override
                    public FieldVector getVector() {
                      return vector;
                    }
                  })
              .build();
      assertThat(binder.next()).isFalse();

      final IntVector ints = (IntVector) root.getVector(0);
      ints.setSafe(0, 4);
      ints.setNull(1);

      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo("4");
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo("null");
      assertThat(binder.next()).isFalse();
    }
  }

  @Test
  void bool() throws SQLException {
    testSimpleType(ArrowType.Bool.INSTANCE, Types.BOOLEAN,
        (BitVector vector, Integer index, Boolean value) -> vector.setSafe(index, value ? 1 : 0),
        BitVector::setNull,
        Arrays.asList(true, false, true));
  }

  @Test
  void int8() throws SQLException {
    testSimpleType(new ArrowType.Int(8, true), Types.TINYINT,
        TinyIntVector::setSafe, TinyIntVector::setNull,
        Arrays.asList(Byte.MAX_VALUE, Byte.MIN_VALUE, (byte) 42));
  }

  @Test
  void int16() throws SQLException {
    testSimpleType(new ArrowType.Int(16, true), Types.SMALLINT,
        SmallIntVector::setSafe, SmallIntVector::setNull,
        Arrays.asList(Short.MAX_VALUE, Short.MIN_VALUE, (short) 42));
  }

  @Test
  void int32() throws SQLException {
    testSimpleType(new ArrowType.Int(32, true), Types.INTEGER,
        IntVector::setSafe, IntVector::setNull,
        Arrays.asList(Integer.MAX_VALUE, Integer.MIN_VALUE, 42));
  }

  @Test
  void int64() throws SQLException {
    testSimpleType(new ArrowType.Int(64, true), Types.BIGINT,
        BigIntVector::setSafe, BigIntVector::setNull,
        Arrays.asList(Long.MAX_VALUE, Long.MIN_VALUE, 42L));
  }

  @Test
  void float32() throws SQLException {
    testSimpleType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), Types.REAL,
        Float4Vector::setSafe, Float4Vector::setNull,
        Arrays.asList(Float.MIN_VALUE, Float.MAX_VALUE, Float.POSITIVE_INFINITY));
  }

  @Test
  void float64() throws SQLException {
    testSimpleType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), Types.DOUBLE,
        Float8Vector::setSafe, Float8Vector::setNull,
        Arrays.asList(Double.MIN_VALUE, Double.MAX_VALUE, Double.POSITIVE_INFINITY));
  }

  @Test
  void time32() throws SQLException {
    testSimpleType(new ArrowType.Time(TimeUnit.SECOND, 32), Types.TIME,
        (valueVectors, index, value) -> valueVectors.setSafe(index, (int) (value.getTime() / 1_000)),
        TimeSecVector::setNull,
        Arrays.asList(new Time(-128_000), new Time(104_000), new Time(-42_000)));
    testSimpleType(new ArrowType.Time(TimeUnit.MILLISECOND, 32), Types.TIME,
        (valueVectors, index, value) -> valueVectors.setSafe(index, (int) value.getTime()),
        TimeMilliVector::setNull,
        Arrays.asList(new Time(-128_000), new Time(104_000), new Time(-42_000)));
  }

  @Test
  void time64() throws SQLException {
    testSimpleType(new ArrowType.Time(TimeUnit.MICROSECOND, 64), Types.TIME,
        (valueVectors, index, value) -> valueVectors.setSafe(index, (int) (value.getTime() * 1_000)),
        TimeMicroVector::setNull,
        Arrays.asList(new Time(-128_000), new Time(104_000), new Time(-42_000)));
    testSimpleType(new ArrowType.Time(TimeUnit.NANOSECOND, 64), Types.TIME,
        (valueVectors, index, value) -> valueVectors.setSafe(index, (int) (value.getTime() * 1_000_000)),
        TimeNanoVector::setNull,
        Arrays.asList(new Time(-128), new Time(104), new Time(-42)));
  }

  @Test
  void date32() throws SQLException {
    testSimpleType(new ArrowType.Date(DateUnit.DAY), Types.DATE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, (int) (value.getTime() / MILLIS_PER_DAY)),
        DateDayVector::setNull,
        Arrays.asList(new Date(-5 * MILLIS_PER_DAY), new Date(2 * MILLIS_PER_DAY), new Date(MILLIS_PER_DAY)));
  }

  @Test
  void date64() throws SQLException {
    testSimpleType(new ArrowType.Date(DateUnit.MILLISECOND), Types.DATE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime()),
        DateMilliVector::setNull,
        Arrays.asList(new Date(-5 * MILLIS_PER_DAY), new Date(2 * MILLIS_PER_DAY), new Date(MILLIS_PER_DAY)));
  }

  @Test
  void timestamp() throws SQLException {
    List<Timestamp> values = Arrays.asList(new Timestamp(-128_000), new Timestamp(104_000), new Timestamp(-42_000));
    testSimpleType(new ArrowType.Timestamp(TimeUnit.SECOND, null), Types.TIMESTAMP,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() / 1_000),
        TimeStampSecVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), Types.TIMESTAMP,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime()),
        TimeStampMilliVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), Types.TIMESTAMP,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() * 1_000),
        TimeStampMicroVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null), Types.TIMESTAMP,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() * 1_000_000),
        TimeStampNanoVector::setNull, values);
  }

  @Test
  void timestampTz() throws SQLException {
    List<Timestamp> values = Arrays.asList(new Timestamp(-128_000), new Timestamp(104_000), new Timestamp(-42_000));
    testSimpleType(new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"), Types.TIMESTAMP_WITH_TIMEZONE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() / 1_000),
        TimeStampSecTZVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), Types.TIMESTAMP_WITH_TIMEZONE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime()),
        TimeStampMilliTZVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"), Types.TIMESTAMP_WITH_TIMEZONE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() * 1_000),
        TimeStampMicroTZVector::setNull, values);
    testSimpleType(new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), Types.TIMESTAMP_WITH_TIMEZONE,
        (valueVectors, index, value) -> valueVectors.setSafe(index, value.getTime() * 1_000_000),
        TimeStampNanoTZVector::setNull, values);
  }

  @Test
  void utf8() throws SQLException {
    testSimpleType(ArrowType.Utf8.INSTANCE, Types.VARCHAR,
        (VarCharVector vector, Integer index, String value) ->
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8)),
        BaseVariableWidthVector::setNull,
        Arrays.asList("", "foobar", "abc"));
  }

  @Test
  void largeUtf8() throws SQLException {
    testSimpleType(ArrowType.LargeUtf8.INSTANCE, Types.LONGVARCHAR,
        (LargeVarCharVector vector, Integer index, String value) ->
            vector.setSafe(index, value.getBytes(StandardCharsets.UTF_8)),
        BaseLargeVariableWidthVector::setNull,
        Arrays.asList("", "foobar", "abc"));
  }

  @Test
  void binary() throws SQLException {
    testSimpleType(ArrowType.Binary.INSTANCE, Types.VARBINARY,
        (VarBinaryVector vector, Integer index, byte[] value) ->
            vector.setSafe(index, value),
        BaseVariableWidthVector::setNull,
        Arrays.asList(new byte[0], new byte[] {2, -4}, new byte[] {0, -1, 127, -128}));
  }

  @Test
  void largeBinary() throws SQLException {
    testSimpleType(ArrowType.LargeBinary.INSTANCE, Types.LONGVARBINARY,
        (LargeVarBinaryVector vector, Integer index, byte[] value) ->
            vector.setSafe(index, value),
        BaseLargeVariableWidthVector::setNull,
        Arrays.asList(new byte[0], new byte[] {2, -4}, new byte[] {0, -1, 127, -128}));
  }

  @Test
  void fixedSizeBinary() throws SQLException {
    testSimpleType(new ArrowType.FixedSizeBinary(3), Types.BINARY,
        FixedSizeBinaryVector::setSafe, FixedSizeBinaryVector::setNull,
        Arrays.asList(new byte[3], new byte[] {1, 2, -4}, new byte[] {-1, 127, -128}));
  }

  @Test
  void decimal128() throws SQLException {
    testSimpleType(new ArrowType.Decimal(/*precision*/ 12, /*scale*/3, 128), Types.DECIMAL,
        DecimalVector::setSafe, DecimalVector::setNull,
        Arrays.asList(new BigDecimal("120.429"), new BigDecimal("-10590.123"), new BigDecimal("0.000")));
  }

  @Test
  void decimal256() throws SQLException {
    testSimpleType(new ArrowType.Decimal(/*precision*/ 12, /*scale*/3, 256), Types.DECIMAL,
        Decimal256Vector::setSafe, Decimal256Vector::setNull,
        Arrays.asList(new BigDecimal("120.429"), new BigDecimal("-10590.123"), new BigDecimal("0.000")));
  }

  @Test
  void listOfDouble() throws SQLException {
    TriConsumer<ListVector, Integer, Double[]> setValue = (listVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionListWriter writer = listVector.getWriter();
      writer.setPosition(index);
      writer.startList();
      Arrays.stream(values).forEach(doubleValue -> writer.float8().writeFloat8(doubleValue));
      writer.endList();
      listVector.setLastSet(index);
    };
    List<Double[]> values = Arrays.asList(new Double[]{0.0, Math.PI}, new Double[]{1.1, -352346.2, 2355.6},
                                          new Double[]{-1024.3}, new Double[]{});
    testListType(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), setValue, ListVector::setNull, values);
  }

  @Test
  void listOfInt64() throws SQLException {
    TriConsumer<ListVector, Integer, Long[]> setValue = (listVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionListWriter writer = listVector.getWriter();
      writer.setPosition(index);
      writer.startList();
      Arrays.stream(values).forEach(longValue -> writer.bigInt().writeBigInt(longValue));
      writer.endList();
      listVector.setLastSet(index);
    };
    List<Long[]> values = Arrays.asList(new Long[]{1L, 2L, 3L}, new Long[]{4L, 5L},
            new Long[]{512L, 1024L, 2048L, 4096L}, new Long[]{});
    testListType((ArrowType) new ArrowType.Int(64, true), setValue, ListVector::setNull, values);
  }

  @Test
  void listOfInt32() throws SQLException {
    TriConsumer<ListVector, Integer, Integer[]> setValue = (listVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionListWriter writer = listVector.getWriter();
      writer.setPosition(index);
      writer.startList();
      Arrays.stream(values).forEach(integerValue -> writer.integer().writeInt(integerValue));
      writer.endList();
      listVector.setLastSet(index);
    };
    List<Integer[]> values = Arrays.asList(new Integer[]{1, 2, 3}, new Integer[]{4, 5},
            new Integer[]{512, 1024, 2048, 4096}, new Integer[]{});
    testListType((ArrowType) new ArrowType.Int(32, true), setValue, ListVector::setNull, values);
  }

  @Test
  void listOfBoolean() throws SQLException {
    TriConsumer<ListVector, Integer, Boolean[]> setValue = (listVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionListWriter writer = listVector.getWriter();
      writer.setPosition(index);
      writer.startList();
      Arrays.stream(values).forEach(booleanValue -> writer.bit().writeBit(booleanValue ? 1 : 0));
      writer.endList();
      listVector.setLastSet(index);
    };
    List<Boolean[]> values = Arrays.asList(new Boolean[]{true, false},
            new Boolean[]{false, false}, new Boolean[]{true, true, false, true}, new Boolean[]{});
    testListType((ArrowType) new ArrowType.Bool(), setValue, ListVector::setNull, values);
  }

  @Test
  void listOfString() throws SQLException {
    TriConsumer<ListVector, Integer, String[]> setValue = (listVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionListWriter writer = listVector.getWriter();
      writer.setPosition(index);
      writer.startList();
      Arrays.stream(values).forEach(stringValue -> {
        if (stringValue != null) {
          byte[] stringValueBytes = stringValue.getBytes(StandardCharsets.UTF_8);
          try (ArrowBuf stringBuffer = allocator.buffer(stringValueBytes.length)) {
            stringBuffer.writeBytes(stringValueBytes);
            writer.varChar().writeVarChar(0, stringValueBytes.length, stringBuffer);
          }
        } else {
          writer.varChar().writeNull();
        }
      });
      writer.endList();
      listVector.setLastSet(index);
    };
    List<String[]> values = Arrays.asList(new String[]{"aaaa", "b1"},
            new String[]{"c", null, "d"}, new String[]{"e", "f", "g", "h"}, new String[]{});
    testListType((ArrowType) new ArrowType.Utf8(), setValue, ListVector::setNull, values);
  }

  @Test
  void mapOfString() throws SQLException {
    TriConsumer<MapVector, Integer, Map<String, String>> setValue = (mapVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionMapWriter mapWriter = mapVector.getWriter();
      mapWriter.setPosition(index);
      mapWriter.startMap();
      values.entrySet().forEach(mapValue -> {
        if (mapValue != null) {
          byte[] keyBytes = mapValue.getKey().getBytes(StandardCharsets.UTF_8);
          byte[] valueBytes = mapValue.getValue().getBytes(StandardCharsets.UTF_8);
          try (
              ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
              ArrowBuf valueBuf = allocator.buffer(valueBytes.length);
          ) {
            mapWriter.startEntry();
            keyBuf.writeBytes(keyBytes);
            valueBuf.writeBytes(valueBytes);
            mapWriter.key().varChar().writeVarChar(0, keyBytes.length, keyBuf);
            mapWriter.value().varChar().writeVarChar(0, valueBytes.length, valueBuf);
            mapWriter.endEntry();
          }
        } else {
          mapWriter.writeNull();
        }
      });
      mapWriter.endMap();
    };

    JsonStringHashMap<String, String> value1 = new JsonStringHashMap<String, String>();
    value1.put("a", "b");
    value1.put("c", "d");
    JsonStringHashMap<String, String> value2 = new JsonStringHashMap<String, String>();
    value2.put("d", "e");
    value2.put("f", "g");
    value2.put("k", "l");
    JsonStringHashMap<String, String> value3 = new JsonStringHashMap<String, String>();
    value3.put("y", "z");
    value3.put("arrow", "cool");
    List<Map<String, String>> values = Arrays.asList(value1, value2, value3, Collections.emptyMap());
    testMapType(new ArrowType.Map(true), setValue, MapVector::setNull, values, new ArrowType.Utf8());
  }

  @Test
  void mapOfInteger() throws SQLException {
    TriConsumer<MapVector, Integer, Map<Integer, Integer>> setValue = (mapVector, index, values) -> {
      org.apache.arrow.vector.complex.impl.UnionMapWriter mapWriter = mapVector.getWriter();
      mapWriter.setPosition(index);
      mapWriter.startMap();
      values.entrySet().forEach(mapValue -> {
        if (mapValue != null) {
          mapWriter.startEntry();
          mapWriter.key().integer().writeInt(mapValue.getKey());
          mapWriter.value().integer().writeInt(mapValue.getValue());
          mapWriter.endEntry();
        } else {
          mapWriter.writeNull();
        }
      });
      mapWriter.endMap();
    };

    JsonStringHashMap<Integer, Integer> value1 = new JsonStringHashMap<Integer, Integer>();
    value1.put(1, 2);
    value1.put(3, 4);
    JsonStringHashMap<Integer, Integer> value2 = new JsonStringHashMap<Integer, Integer>();
    value2.put(5, 6);
    value2.put(7, 8);
    value2.put(9, 1024);
    JsonStringHashMap<Integer, Integer> value3 = new JsonStringHashMap<Integer, Integer>();
    value3.put(Integer.MIN_VALUE, Integer.MAX_VALUE);
    value3.put(0, 4096);
    List<Map<Integer, Integer>> values = Arrays.asList(value1, value2, value3, Collections.emptyMap());
    testMapType(new ArrowType.Map(true), setValue, MapVector::setNull, values, new ArrowType.Int(32, true));
  }

  @FunctionalInterface
  interface TriConsumer<T, U, V> {
    void accept(T value1, U value2, V value3);
  }

  <T, V extends FieldVector> void testSimpleType(ArrowType arrowType, int jdbcType, TriConsumer<V, Integer, T> setValue,
                          BiConsumer<V, Integer> setNull, List<T> values) throws SQLException {
    Schema schema = new Schema(Collections.singletonList(Field.nullable("field", arrowType)));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);
      final ColumnBinder columnBinder = ColumnBinder.forVector(vector);
      assertThat(columnBinder.getJdbcType()).isEqualTo(jdbcType);

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      setNull.accept(vector, 2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isFalse();

      binder.reset();

      setNull.accept(vector, 0);
      setValue.accept(vector, 1, values.get(2));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();
    }

    // Non-nullable (since some types have a specialized binder)
    schema = new Schema(Collections.singletonList(Field.notNullable("field", arrowType)));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);
      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();

      binder.reset();

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(2));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();
    }
  }

  <T, V extends FieldVector> void testListType(ArrowType arrowType, TriConsumer<V, Integer, T> setValue,
                          BiConsumer<V, Integer> setNull, List<T> values) throws SQLException {
    int jdbcType = Types.ARRAY;
    Schema schema = new Schema(Collections.singletonList(new Field("field", FieldType.nullable(
            new ArrowType.List()), Collections.singletonList(
            new Field("element", FieldType.notNullable(arrowType), null)
    ))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);
      final ColumnBinder columnBinder = ColumnBinder.forVector(vector);
      assertThat(columnBinder.getJdbcType()).isEqualTo(jdbcType);

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      setNull.accept(vector, 2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isFalse();

      binder.reset();

      setNull.accept(vector, 0);
      setValue.accept(vector, 1, values.get(3));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(3));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();
    }

    // Non-nullable (since some types have a specialized binder)
    schema = new Schema(Collections.singletonList(new Field("field", FieldType.notNullable(
            new ArrowType.List()), Collections.singletonList(
            new Field("element", FieldType.notNullable(arrowType), null)
    ))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);
      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();

      binder.reset();

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(2));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();
    }
  }

  <T, V extends FieldVector> void testMapType(ArrowType arrowType, TriConsumer<V, Integer, T> setValue,
                                              BiConsumer<V, Integer> setNull, List<T> values,
                                              ArrowType elementType) throws SQLException {
    int jdbcType = Types.VARCHAR;
    FieldType keyType = new FieldType(false, elementType, null, null);
    FieldType mapType = new FieldType(false, ArrowType.Struct.INSTANCE, null, null);
    Schema schema = new Schema(Collections.singletonList(new Field("field", FieldType.nullable(arrowType),
            Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                    Arrays.asList(new Field(MapVector.KEY_NAME, keyType, null),
                            new Field(MapVector.VALUE_NAME, keyType, null)))))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bindAll().build();
      assertThat(binder.next()).isFalse();

      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);
      final ColumnBinder columnBinder = ColumnBinder.forVector(vector);
      assertThat(columnBinder.getJdbcType()).isEqualTo(jdbcType);

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      setNull.accept(vector, 2);
      root.setRowCount(3);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0).toString());
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1).toString());
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isFalse();

      binder.reset();

      setNull.accept(vector, 0);
      setValue.accept(vector, 1, values.get(3));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isNull();
      assertThat(statement.getParamType(1)).isEqualTo(jdbcType);
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(3).toString());
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0).toString());
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2).toString());
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1).toString());
      assertThat(binder.next()).isFalse();
    }

    // Non-nullable (since some types have a specialized binder)
    schema = new Schema(Collections.singletonList(new Field("field", FieldType.notNullable(arrowType),
            Collections.singletonList(new Field(MapVector.KEY_NAME, mapType,
                    Arrays.asList(new Field(MapVector.KEY_NAME, keyType, null),
                            new Field(MapVector.VALUE_NAME, keyType, null)))))));
    try (final MockPreparedStatement statement = new MockPreparedStatement();
         final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      @SuppressWarnings("unchecked")
      final V vector = (V) root.getVector(0);

      final JdbcParameterBinder binder =
          JdbcParameterBinder.builder(statement, root).bind(1,
                  new org.apache.arrow.adapter.jdbc.binder.MapBinder((MapVector) vector, Types.OTHER)).build();
      assertThat(binder.next()).isFalse();

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(1));
      root.setRowCount(2);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();

      binder.reset();

      setValue.accept(vector, 0, values.get(0));
      setValue.accept(vector, 1, values.get(2));
      setValue.accept(vector, 2, values.get(0));
      setValue.accept(vector, 3, values.get(2));
      setValue.accept(vector, 4, values.get(1));
      root.setRowCount(5);

      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(0));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(2));
      assertThat(binder.next()).isTrue();
      assertThat(statement.getParamValue(1)).isEqualTo(values.get(1));
      assertThat(binder.next()).isFalse();
    }
  }
}
