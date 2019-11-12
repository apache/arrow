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

package org.apache.arrow;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.junit.Test;

public class AvroLogicalTypesTest extends AvroTestBase {

  @Test
  public void testTimestampMicros() throws Exception {
    Schema schema = getSchema("logical/test_timestamp_micros.avsc");

    List<Long> data = Arrays.asList(10000L, 20000L, 30000L, 40000L, 50000L);
    List<LocalDateTime> expected = Arrays.asList(
        DateUtility.getLocalDateTimeFromEpochMicro(10000),
        DateUtility.getLocalDateTimeFromEpochMicro(20000),
        DateUtility.getLocalDateTimeFromEpochMicro(30000),
        DateUtility.getLocalDateTimeFromEpochMicro(40000),
        DateUtility.getLocalDateTimeFromEpochMicro(50000)
    );

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testTimestampMillis() throws Exception {
    Schema schema = getSchema("logical/test_timestamp_millis.avsc");

    List<Long> data = Arrays.asList(10000L, 20000L, 30000L, 40000L, 50000L);
    List<LocalDateTime> expected = Arrays.asList(
        DateUtility.getLocalDateTimeFromEpochMilli(10000),
        DateUtility.getLocalDateTimeFromEpochMilli(20000),
        DateUtility.getLocalDateTimeFromEpochMilli(30000),
        DateUtility.getLocalDateTimeFromEpochMilli(40000),
        DateUtility.getLocalDateTimeFromEpochMilli(50000)
    );

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testTimeMicros() throws Exception {
    Schema schema = getSchema("logical/test_time_micros.avsc");

    List<Long> data = Arrays.asList(10000L, 20000L, 30000L, 40000L, 50000L);

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testTimeMillis() throws Exception {
    Schema schema = getSchema("logical/test_time_millis.avsc");

    List<Integer> data = Arrays.asList(100, 200, 300, 400, 500);
    List<LocalDateTime> expected = Arrays.asList(
        DateUtility.getLocalDateTimeFromEpochMilli(100),
        DateUtility.getLocalDateTimeFromEpochMilli(200),
        DateUtility.getLocalDateTimeFromEpochMilli(300),
        DateUtility.getLocalDateTimeFromEpochMilli(400),
        DateUtility.getLocalDateTimeFromEpochMilli(500)
    );

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testDate() throws Exception {
    Schema schema = getSchema("logical/test_date.avsc");

    List<Integer> data = Arrays.asList(100, 200, 300, 400, 500);

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testDecimalWithOriginalBytes() throws Exception {
    Schema schema = getSchema("logical/test_decimal_with_original_bytes.avsc");
    List<ByteBuffer> data = new ArrayList<>();
    List<BigDecimal> expected = new ArrayList<>();

    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();

    for (int i = 0; i < 5; i++) {
      BigDecimal value = new BigDecimal(i * i).setScale(2);
      ByteBuffer buffer = conversion.toBytes(value, schema, schema.getLogicalType());
      data.add(buffer);
      expected.add(value);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);
    checkPrimitiveResult(expected, vector);

  }

  @Test
  public void testDecimalWithOriginalFixed() throws Exception {
    Schema schema = getSchema("logical/test_decimal_with_original_fixed.avsc");

    List<GenericFixed> data = new ArrayList<>();
    List<BigDecimal> expected = new ArrayList<>();

    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();

    for (int i = 0; i < 5; i++) {
      BigDecimal value = new BigDecimal(i * i).setScale(2);
      GenericFixed fixed = conversion.toFixed(value, schema, schema.getLogicalType());
      data.add(fixed);
      expected.add(value);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);
    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testInvalidDecimalPrecision() throws Exception {
    Schema schema = getSchema("logical/test_decimal_invalid1.avsc");
    List<ByteBuffer> data = new ArrayList<>();

    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();

    for (int i = 0; i < 5; i++) {
      BigDecimal value = new BigDecimal(i * i).setScale(2);
      ByteBuffer buffer = conversion.toBytes(value, schema, schema.getLogicalType());
      data.add(buffer);
    }

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> writeAndRead(schema, data));
    assertTrue(e.getMessage().contains("Precision must be in range of 1 to 38"));

  }

  @Test
  public void testFailedToCreateDecimalLogicalType() throws Exception {
    // For decimal logical type, if avro validate schema failed, it will not create logical type,
    // and the schema will be treated as its original type.

    // java.lang.IllegalArgumentException: Invalid decimal scale: -1 (must be positive)
    Schema schema1 = getSchema("logical/test_decimal_invalid2.avsc");
    assertNull(schema1.getLogicalType());

    // java.lang.IllegalArgumentException: Invalid decimal scale: 40 (greater than precision: 20)
    Schema schema2 = getSchema("logical/test_decimal_invalid3.avsc");
    assertNull(schema2.getLogicalType());

    // java.lang.IllegalArgumentException: fixed(1) cannot store 30 digits (max 2)
    Schema schema3 = getSchema("logical/test_decimal_invalid4.avsc");
    assertNull(schema3.getLogicalType());
  }

}
