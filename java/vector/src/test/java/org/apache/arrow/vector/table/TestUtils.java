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

import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.OFFSET_WIDTH;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
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
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.Float8Writer;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestUtils {

  public static final String INT_VECTOR_NAME = "intCol";
  public static final String INT_VECTOR_NAME_1 = "intCol1";
  public static final String VARCHAR_VECTOR_NAME_1 = "varcharCol1";
  public static final String VARBINARY_VECTOR_NAME_1 = "varbinaryCol1";
  public static final String FIXEDBINARY_VECTOR_NAME_1 = "varbinaryCol1";
  public static final String INT_VECTOR_NAME_2 = "intCol2";
  public static final String INT_LIST_VECTOR_NAME = "int list vector";
  public static final String BIGINT_INT_MAP_VECTOR_NAME = "bigint-int map vector";
  public static final String STRUCT_VECTOR_NAME = "struct_vector";
  public static final String UNION_VECTOR_NAME = "union_vector";

  /**
   * Returns a list of two IntVectors to be used to instantiate Tables for testing. Each IntVector
   * has two values set.
   */
  static List<FieldVector> twoIntColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    IntVector v2 = new IntVector(INT_VECTOR_NAME_2, allocator);
    v2.allocateNew(2);
    v2.set(0, 3);
    v2.set(1, 4);
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  /**
   * Returns a list of two FieldVectors to be used to instantiate Tables for testing. The first
   * vector is an IntVector and the second is a VarCharVector. Each vector has two values set.
   */
  static List<FieldVector> intPlusVarcharColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    VarCharVector v2 = new VarCharVector(VARCHAR_VECTOR_NAME_1, allocator);
    v2.allocateNew(2);
    v2.set(0, "one".getBytes());
    v2.set(1, "two".getBytes());
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  /**
   * Returns a list of two FieldVectors to be used to instantiate Tables for testing. The first
   * vector is an IntVector and the second is a LargeVarCharVector. Each vector has two values set.
   */
  static List<FieldVector> intPlusLargeVarcharColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    LargeVarCharVector v2 = new LargeVarCharVector(VARCHAR_VECTOR_NAME_1, allocator);
    v2.allocateNew(2);
    v2.set(0, "one".getBytes());
    v2.set(1, "two".getBytes());
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  /**
   * Returns a list of two FieldVectors to be used to instantiate Tables for testing. The first
   * vector is an IntVector and the second is a VarBinaryVector. Each vector has two values set.
   * The large binary vectors values are "one" and "two" encoded with UTF-8
   */
  static List<FieldVector> intPlusVarBinaryColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    VarBinaryVector v2 = new VarBinaryVector(VARBINARY_VECTOR_NAME_1, allocator);
    v2.allocateNew(2);
    v2.set(0, "one".getBytes());
    v2.set(1, "two".getBytes());
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  /**
   * Returns a list of two FieldVectors to be used to instantiate Tables for testing. The first
   * vector is an IntVector and the second is a VarBinaryVector. Each vector has two values set.
   * The large binary vectors values are "one" and "two" encoded with UTF-8
   */
  static List<FieldVector> intPlusLargeVarBinaryColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    LargeVarBinaryVector v2 = new LargeVarBinaryVector(VARBINARY_VECTOR_NAME_1, allocator);
    v2.allocateNew(2);
    v2.set(0, "one".getBytes());
    v2.set(1, "two".getBytes());
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  /**
   * Returns a list of two FieldVectors to be used to instantiate Tables for testing. The first
   * vector is an IntVector and the second is a FixedSizeBinary vector. Each vector has two values set.
   * The large binary vectors values are "one" and "two" encoded with UTF-8
   */
  static List<FieldVector> intPlusFixedBinaryColumns(BufferAllocator allocator) {
    List<FieldVector> vectorList = new ArrayList<>();
    IntVector v1 = getSimpleIntVector(allocator);
    FixedSizeBinaryVector v2 = new FixedSizeBinaryVector(FIXEDBINARY_VECTOR_NAME_1, allocator, 3);
    v2.allocateNew(2);
    v2.set(0, "one".getBytes());
    v2.set(1, "two".getBytes());
    v2.setValueCount(2);
    vectorList.add(v1);
    vectorList.add(v2);
    return vectorList;
  }

  private static IntVector getSimpleIntVector(BufferAllocator allocator) {
    IntVector v1 = new IntVector(INT_VECTOR_NAME_1, allocator);
    v1.allocateNew(2);
    v1.set(0, 1);
    v1.set(1, 2);
    v1.setValueCount(2);
    return v1;
  }

  /**
   * Returns a list of fixed-width vectors for testing. It includes
   * <ol>
   *   <li>all integral and floating point types</li>
   *   <li>all basic times and timestamps (second, milli, micro, nano</li>
   * </ol>
   *
   * The vector names are based on their type name (e.g. BigIntVector is called "bigInt_vector"
   */
  static List<FieldVector> fixedWidthVectors(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    numericVectors(vectors, allocator, rowCount);
    simpleTemporalVectors(vectors, allocator, rowCount);
    return vectors;
  }

  /**
   * Returns a list of all integral and floating point vectors.
   * The vector names are based on their type name (e.g. BigIntVector is called "bigInt_vector"
   */
  static List<FieldVector> numericVectors(
      List<FieldVector> vectors, BufferAllocator allocator, int rowCount) {
    vectors.add(new IntVector("int_vector", allocator));
    vectors.add(new BigIntVector("bigInt_vector", allocator));
    vectors.add(new SmallIntVector("smallInt_vector", allocator));
    vectors.add(new TinyIntVector("tinyInt_vector", allocator));
    vectors.add(new UInt1Vector("uInt1_vector", allocator));
    vectors.add(new UInt2Vector("uInt2_vector", allocator));
    vectors.add(new UInt4Vector("uInt4_vector", allocator));
    vectors.add(new UInt8Vector("uInt8_vector", allocator));
    vectors.add(new Float4Vector("float4_vector", allocator));
    vectors.add(new Float8Vector("float8_vector", allocator));
    vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
    return vectors;
  }

  static List<FieldVector> numericVectors(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    return numericVectors(vectors, allocator, rowCount);
  }

  static List<FieldVector> simpleTemporalVectors(
      List<FieldVector> vectors, BufferAllocator allocator, int rowCount) {
    vectors.add(new TimeSecVector("timeSec_vector", allocator));
    vectors.add(new TimeMilliVector("timeMilli_vector", allocator));
    vectors.add(new TimeMicroVector("timeMicro_vector", allocator));
    vectors.add(new TimeNanoVector("timeNano_vector", allocator));

    vectors.add(new TimeStampSecVector("timeStampSec_vector", allocator));
    vectors.add(new TimeStampMilliVector("timeStampMilli_vector", allocator));
    vectors.add(new TimeStampMicroVector("timeStampMicro_vector", allocator));
    vectors.add(new TimeStampNanoVector("timeStampNano_vector", allocator));

    vectors.add(new DateMilliVector("dateMilli_vector", allocator));
    vectors.add(new DateDayVector("dateDay_vector", allocator));

    vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
    return vectors;
  }

  static List<FieldVector> simpleTemporalVectors(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    return simpleTemporalVectors(vectors, allocator, rowCount);
  }

  static List<FieldVector> timezoneTemporalVectors(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(new TimeStampSecTZVector("timeStampSecTZ_vector", allocator, "UTC"));
    vectors.add(new TimeStampMilliTZVector("timeStampMilliTZ_vector", allocator, "UTC"));
    vectors.add(new TimeStampMicroTZVector("timeStampMicroTZ_vector", allocator, "UTC"));
    vectors.add(new TimeStampNanoTZVector("timeStampNanoTZ_vector", allocator, "UTC"));
    vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
    return vectors;
  }

  static List<FieldVector> intervalVectors(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(new IntervalDayVector("intervalDay_vector", allocator));
    vectors.add(new IntervalYearVector("intervalYear_vector", allocator));
    vectors.add(new IntervalMonthDayNanoVector("intervalMonthDayNano_vector", allocator));
    vectors.add(new DurationVector("duration_vector",
        new FieldType(true, new ArrowType.Duration(TimeUnit.SECOND), null), allocator));
    vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
    return vectors;
  }

  /** Returns a list vector of ints. */
  static ListVector simpleListVector(BufferAllocator allocator) {
    ListVector listVector = ListVector.empty(INT_LIST_VECTOR_NAME, allocator);
    final int innerCount = 80; // total number of values
    final int outerCount = 8; // total number of values in the list vector itself
    final int listLength = innerCount / outerCount; // length of an individual list

    Types.MinorType type = Types.MinorType.INT;
    listVector.addOrGetVector(FieldType.nullable(type.getType()));

    listVector.allocateNew();
    IntVector dataVector = (IntVector) listVector.getDataVector();

    for (int i = 0; i < innerCount; i++) {
      dataVector.set(i, i);
    }
    dataVector.setValueCount(innerCount);

    for (int i = 0; i < outerCount; i++) {
      BitVectorHelper.setBit(listVector.getValidityBuffer(), i);
      listVector.getOffsetBuffer().setInt(i * OFFSET_WIDTH, i * listLength);
      listVector.getOffsetBuffer().setInt((i + 1) * OFFSET_WIDTH, (i + 1) * listLength);
    }
    listVector.setLastSet(outerCount - 1);
    listVector.setValueCount(outerCount);
    return listVector;
  }

  static StructVector simpleStructVector(BufferAllocator allocator) {
    final String INT_COL = "struct_int_child";
    final String FLT_COL = "struct_flt_child";
    StructVector structVector = StructVector.empty(STRUCT_VECTOR_NAME, allocator);
    final int size = 6; // number of structs

    NullableStructWriter structWriter = structVector.getWriter();
    structVector.addOrGet(
        INT_COL, FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
    structVector.addOrGet(
        FLT_COL, FieldType.nullable(Types.MinorType.INT.getType()), IntVector.class);
    structVector.allocateNew();
    IntWriter intWriter = structWriter.integer(INT_COL);
    Float8Writer float8Writer = structWriter.float8(FLT_COL);

    for (int i = 0; i < size; i++) {
      structWriter.setPosition(i);
      structWriter.start();
      intWriter.writeInt(i);
      float8Writer.writeFloat8(i * .1);
      structWriter.end();
    }

    structWriter.setValueCount(size);

    return structVector;
  }

  /** Returns a MapVector of longs to doubles. */
  static MapVector simpleMapVector(BufferAllocator allocator) {
    MapVector mapVector = MapVector.empty(BIGINT_INT_MAP_VECTOR_NAME, allocator, false);
    mapVector.allocateNew();
    int count = 5;
    UnionMapWriter mapWriter = mapVector.getWriter();
    for (int i = 0; i < count; i++) {
      mapWriter.startMap();
      for (int j = 0; j < i + 1; j++) {
        mapWriter.startEntry();
        mapWriter.key().bigInt().writeBigInt(j);
        mapWriter.value().integer().writeInt(j);
        mapWriter.endEntry();
      }
      mapWriter.endMap();
    }
    mapWriter.setValueCount(count);
    return mapVector;
  }

  static List<FieldVector> decimalVector(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(new DecimalVector("decimal_vector",
        new FieldType(true, new ArrowType.Decimal(38, 10, 128), null),
        allocator));
    vectors.forEach(vec -> generateDecimalData((DecimalVector) vec, rowCount));
    return vectors;
  }

  static List<FieldVector> bitVector(BufferAllocator allocator, int rowCount) {
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(new BitVector("bit_vector", allocator));
    vectors.forEach(vec -> GenerateSampleData.generateTestData(vec, rowCount));
    return vectors;
  }

  /** Returns a UnionVector. */
  static UnionVector simpleUnionVector(BufferAllocator allocator) {
    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    UnionVector unionVector = new UnionVector(UNION_VECTOR_NAME, allocator, null, null);
    unionVector.allocateNew();

    // write some data
    unionVector.setType(0, Types.MinorType.UINT4);
    unionVector.setSafe(0, uInt4Holder);
    unionVector.setType(2, Types.MinorType.UINT4);
    unionVector.setSafe(2, uInt4Holder);
    unionVector.setValueCount(4);
    return unionVector;
  }

  /** Returns a DenseUnionVector. */
  static DenseUnionVector simpleDenseUnionVector(BufferAllocator allocator) {
    final NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
    uInt4Holder.value = 100;
    uInt4Holder.isSet = 1;

    DenseUnionVector unionVector = new DenseUnionVector(UNION_VECTOR_NAME, allocator, null, null);
    unionVector.allocateNew();

    // write some data
    byte uint4TypeId =
        unionVector.registerNewTypeId(Field.nullable("", Types.MinorType.UINT4.getType()));
    unionVector.setTypeId(0, uint4TypeId);
    unionVector.setSafe(0, uInt4Holder);
    unionVector.setTypeId(2, uint4TypeId);
    unionVector.setSafe(2, uInt4Holder);
    unionVector.setValueCount(4);
    return unionVector;
  }

  private static void generateDecimalData(DecimalVector vector, int valueCount) {
    final BigDecimal even = new BigDecimal("0.0543278923");
    final BigDecimal odd = new BigDecimal("2.0543278923");
    for (int i = 0; i < valueCount; i++) {
      if (i % 2 == 0) {
        vector.setSafe(i, even);
      } else {
        vector.setSafe(i, odd);
      }
    }
    vector.setValueCount(valueCount);
  }

}
