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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroToArrowTest {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  private BaseAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  private Schema getSchema(String schemaName) throws Exception {
    Path schemaPath = Paths.get(TestWriteReadAvroRecord.class.getResource("/").getPath(),
        "schema", schemaName);
    return new Schema.Parser().parse(schemaPath.toFile());
  }

  private VectorSchemaRoot writeAndReadPrimitive(Schema schema, List data) throws Exception {
    File dataFile = TMP.newFile();

    BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(new FileOutputStream(dataFile), null);
    DatumWriter writer = new GenericDatumWriter(schema);
    BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(new FileInputStream(dataFile), null);

    for (Object value : data) {
      writer.write(value, encoder);
    }

    return AvroToArrow.avroToArrow(schema, decoder, allocator);
  }

  @Test
  public void testStringType() throws Exception {
    Schema schema = getSchema("test_primitive_string.avsc");
    ArrayList<String> data = new ArrayList(Arrays.asList("v1", "v2", "v3", "v4", "v5"));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testIntType() throws Exception {
    Schema schema = getSchema("test_primitive_int.avsc");
    ArrayList<Integer> data = new ArrayList(Arrays.asList(1, 2, 3, 4, 5));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testLongType() throws Exception {
    Schema schema = getSchema("test_primitive_long.avsc");
    ArrayList<Long> data = new ArrayList(Arrays.asList(1L, 2L, 3L, 4L, 5L));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testFloatType() throws Exception {
    Schema schema = getSchema("test_primitive_float.avsc");
    ArrayList<Float> data = new ArrayList(Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testDoubleType() throws Exception {
    Schema schema = getSchema("test_primitive_double.avsc");
    ArrayList<Double> data = new ArrayList(Arrays.asList(1.1, 2.2, 3.3, 4.4, 5.5));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testBytesType() throws Exception {
    Schema schema = getSchema("test_primitive_bytes.avsc");
    ArrayList<ByteBuffer> data = new ArrayList(Arrays.asList(
        ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value4".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value5".getBytes(StandardCharsets.UTF_8))));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  @Test
  public void testBooleanType() throws Exception {
    Schema schema = getSchema("test_primitive_boolean.avsc");
    ArrayList<Boolean> data = new ArrayList(Arrays.asList(true, false, true, false, true));

    VectorSchemaRoot root = writeAndReadPrimitive(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(schema, data, vector);
  }

  private void checkPrimitiveResult(Schema schema, ArrayList data, FieldVector vector) {
    assertEquals(data.size(), vector.getValueCount());
    for (int i = 0; i < data.size(); i++) {
      Object value1 = data.get(i);
      Object value2 = vector.getObject(i);
      if (schema.getType() == Schema.Type.BYTES) {
        value2 = ByteBuffer.wrap((byte[]) value2);
      } else if (schema.getType() == Schema.Type.STRING) {
        value2 = value2.toString();
      }
      assertTrue(Objects.equals(value1, value2));
    }
  }
}
