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

package org.apache.arrow.vector.validate;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Supplier;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for {@link ValidateVectorTypeVisitor}.
 */
public class TestValidateVectorTypeVisitor {

  private BufferAllocator allocator;

  private ValidateVectorTypeVisitor visitor = new ValidateVectorTypeVisitor();

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @After
  public void terminate() throws Exception {
    allocator.close();
  }

  private void testPositiveCase(Supplier<ValueVector> vectorGenerator) {
    try (ValueVector vector = vectorGenerator.get();) {
      vector.accept(visitor, null);
    }
  }

  private void testNegativeCase(Supplier<ValueVector> vectorGenerator) {
    try (ValueVector vector = vectorGenerator.get()) {
      assertThrows(ValidateUtil.ValidateException.class, () -> {
        vector.accept(visitor, null);
      });
    }
  }

  @Test
  public void testFixedWidthVectorsPositive() {
    // integer vectors
    testPositiveCase(() -> new TinyIntVector("vector", allocator));
    testPositiveCase(() -> new SmallIntVector("vector", allocator));
    testPositiveCase(() -> new IntVector("vector", allocator));
    testPositiveCase(() -> new BigIntVector("vector", allocator));
    testPositiveCase(() -> new UInt1Vector("vector", allocator));
    testPositiveCase(() -> new UInt2Vector("vector", allocator));
    testPositiveCase(() -> new UInt4Vector("vector", allocator));
    testPositiveCase(() -> new UInt8Vector("vector", allocator));

    testPositiveCase(() -> new BitVector("vector", allocator));
    testPositiveCase(() -> new DecimalVector("vector", allocator, 30, 16));

    // date vectors
    testPositiveCase(() -> new DateDayVector("vector", allocator));
    testPositiveCase(() -> new DateMilliVector("vector", allocator));

    testPositiveCase(() -> new DurationVector(
        "vector", FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)), allocator));

    // float vectors
    testPositiveCase(() -> new Float4Vector("vector", allocator));
    testPositiveCase(() -> new Float8Vector("vector", allocator));

    // interval vectors
    testPositiveCase(() -> new IntervalDayVector("vector", allocator));
    testPositiveCase(() -> new IntervalYearVector("vector", allocator));

    // time vectors
    testPositiveCase(() -> new TimeMicroVector("vector", allocator));
    testPositiveCase(() -> new TimeMilliVector("vector", allocator));
    testPositiveCase(() -> new TimeMicroVector("vector", allocator));
    testPositiveCase(() -> new TimeSecVector("vector", allocator));

    // time stamp vectors
    testPositiveCase(() -> new TimeStampMicroTZVector("vector", allocator, "cn"));
    testPositiveCase(() -> new TimeStampMicroVector("vector", allocator));
    testPositiveCase(() -> new TimeStampMilliTZVector("vector", allocator, "cn"));
    testPositiveCase(() -> new TimeStampMilliVector("vector", allocator));
    testPositiveCase(() -> new TimeStampNanoTZVector("vector", allocator, "cn"));
    testPositiveCase(() -> new TimeStampNanoVector("vector", allocator));
    testPositiveCase(() -> new TimeStampSecTZVector("vector", allocator, "cn"));
    testPositiveCase(() -> new TimeStampSecVector("vector", allocator));

    testPositiveCase(() -> new FixedSizeBinaryVector("vector", allocator, 5));
  }

  @Test
  public void testFixedWidthVectorsNegative() {
    // integer vectors
    testNegativeCase(
        () -> new TinyIntVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
    testNegativeCase(
        () -> new SmallIntVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
    testNegativeCase(
        () -> new BigIntVector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));
    testNegativeCase(
        () -> new BigIntVector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));
    testNegativeCase(
        () -> new UInt1Vector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));
    testNegativeCase(
        () -> new UInt2Vector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));
    testNegativeCase(
        () -> new UInt4Vector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));
    testNegativeCase(
        () -> new UInt8Vector("vector", FieldType.nullable(Types.MinorType.SMALLINT.getType()), allocator));

    testNegativeCase(
        () -> new BitVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new DecimalVector("vector", allocator, 30, -16));

    // date vectors
    testNegativeCase(
        () -> new DateDayVector("vector", FieldType.nullable(Types.MinorType.FLOAT4.getType()), allocator));
    testNegativeCase(
        () -> new DateMilliVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));

    // float pont vectors
    testNegativeCase(
        () -> new Float4Vector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new Float8Vector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));

    // interval vectors
    testNegativeCase(
        () -> new IntervalDayVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
    testNegativeCase(
        () -> new IntervalYearVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));

    // time vectors
    testNegativeCase(
        () -> new TimeMilliVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeMicroVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeNanoVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeSecVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));

    // time stamp vectors
    testNegativeCase(
        () -> new TimeStampMicroTZVector("vector", allocator, null));
    testNegativeCase(
        () -> new TimeStampMicroVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeStampMilliTZVector("vector", allocator, null));
    testNegativeCase(
        () -> new TimeStampMilliVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeStampNanoTZVector("vector", allocator, null));
    testNegativeCase(
        () -> new TimeStampNanoVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
    testNegativeCase(
        () -> new TimeStampSecTZVector("vector", allocator, null));
    testNegativeCase(
        () -> new TimeStampSecVector("vector", FieldType.nullable(Types.MinorType.BIGINT.getType()), allocator));
  }

  @Test
  public void testDecimalVector() {
    testPositiveCase(() ->
            new DecimalVector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 10, 128)), allocator));
    testPositiveCase(() ->
            new DecimalVector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(38, 10, 128)), allocator));
    testPositiveCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(50, 10, 256)), allocator));
    testPositiveCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(76, 10, 256)), allocator));
    testNegativeCase(() ->
            new DecimalVector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(50, 10, 128)), allocator));
    testNegativeCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(100, 10, 256)), allocator));
    testNegativeCase(() ->
            new DecimalVector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(0, 10, 128)), allocator));
    testNegativeCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(-1, 10, 256)), allocator));
    testNegativeCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(30, 10, 64)), allocator));
    testNegativeCase(() ->
            new Decimal256Vector("dec", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 20, 256)), allocator));
  }

  @Test
  public void testVariableWidthVectorsPositive() {
    testPositiveCase(() -> new VarCharVector("vector", allocator));
    testPositiveCase(() -> new VarBinaryVector("vector", allocator));
  }

  @Test
  public void testVariableWidthVectorsNegative() {
    testNegativeCase(
        () -> new VarCharVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
    testNegativeCase(
        () -> new VarBinaryVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
  }

  @Test
  public void testLargeVariableWidthVectorsPositive() {
    testPositiveCase(() -> new LargeVarCharVector("vector", allocator));
    testPositiveCase(() -> new LargeVarBinaryVector("vector", allocator));
  }

  @Test
  public void testLargeVariableWidthVectorsNegative() {
    testNegativeCase(
        () -> new LargeVarCharVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
    testNegativeCase(
        () -> new LargeVarBinaryVector("vector", FieldType.nullable(Types.MinorType.INT.getType()), allocator));
  }

  @Test
  public void testListVector() {
    testPositiveCase(() -> ListVector.empty("vector", allocator));

    testNegativeCase(
        () -> new ListVector("vector", allocator, FieldType.nullable(Types.MinorType.INT.getType()), null));
  }

  @Test
  public void testLargeListVector() {
    testPositiveCase(() -> LargeListVector.empty("vector", allocator));

    testNegativeCase(
        () -> new LargeListVector("vector", allocator, FieldType.nullable(Types.MinorType.INT.getType()), null));
  }

  @Test
  public void testFixedSizeListVector() {
    testPositiveCase(() -> FixedSizeListVector.empty("vector", 10, allocator));
  }

  @Test
  public void testStructVector() {
    testPositiveCase(() -> StructVector.empty("vector", allocator));

    testNegativeCase(
        () -> new StructVector("vector", allocator, FieldType.nullable(Types.MinorType.INT.getType()), null));
  }

  @Test
  public void testUnionVector() {
    testPositiveCase(() -> UnionVector.empty("vector", allocator));
  }

  @Test
  public void testDenseUnionVector() {
    testPositiveCase(() -> DenseUnionVector.empty("vector", allocator));
  }

  @Test
  public void testNullVector() {
    testPositiveCase(() -> new NullVector("null vec"));
  }
}
