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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.DateUtility;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
    List<ByteBuffer> data = Arrays.asList(
        ByteBuffer.wrap(new BigDecimal("10").unscaledValue().toByteArray()),
        ByteBuffer.wrap(new BigDecimal("1234").unscaledValue().toByteArray()),
        ByteBuffer.wrap(new BigDecimal("123456").unscaledValue().toByteArray()),
        ByteBuffer.wrap(new BigDecimal("111122").unscaledValue().toByteArray()));

    List<BigDecimal> expected = Arrays.asList(
        new BigDecimal(new BigInteger("10"), 2),
        new BigDecimal(new BigInteger("1234"), 2),
        new BigDecimal(new BigInteger("123456"), 2),
        new BigDecimal(new BigInteger("111122"), 2));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);
    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testDecimalWithOriginalFixed() throws Exception {
    Schema schema = getSchema("logical/test_decimal_with_original_fixed.avsc");

    List<GenericData.Fixed> data = new ArrayList<>();
    List<BigDecimal> expected = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      BigDecimal bigDecimal = new BigDecimal(i * i).setScale(2);
      byte fillByte = (byte) (bigDecimal.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = bigDecimal.unscaledValue().toByteArray();
      byte[] bytes = new byte[schema.getFixedSize()];
      int offset = bytes.length - unscaled.length;
      for (int k = 0; k < bytes.length; k++) {
        if (k < offset) {
          bytes[k] = fillByte;
        } else {
          bytes[k] = unscaled[k - offset];
        }
      }
      expected.add(bigDecimal);
      GenericData.Fixed fixed = new GenericData.Fixed(schema);
      fixed.bytes(bytes);
      data.add(fixed);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);
    checkPrimitiveResult(expected, vector);
  }
}
