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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.Text;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
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

  private VectorSchemaRoot writeAndRead(Schema schema, List data) throws Exception {
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

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableStringType() throws Exception {
    Schema schema = getSchema("test_nullable_string.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? "test" + i : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testRecordType() throws Exception {
    Schema schema = getSchema("test_record.avsc");
    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i);
      record.put(2, i % 2 == 0);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testIntType() throws Exception {
    Schema schema = getSchema("test_primitive_int.avsc");
    ArrayList<Integer> data = new ArrayList(Arrays.asList(1, 2, 3, 4, 5));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableIntType() throws Exception {
    Schema schema = getSchema("test_nullable_int.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? i : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testLongType() throws Exception {
    Schema schema = getSchema("test_primitive_long.avsc");
    ArrayList<Long> data = new ArrayList(Arrays.asList(1L, 2L, 3L, 4L, 5L));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableLongType() throws Exception {
    Schema schema = getSchema("test_nullable_long.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? (long) i : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testFloatType() throws Exception {
    Schema schema = getSchema("test_primitive_float.avsc");
    ArrayList<Float> data = new ArrayList(Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableFloatType() throws Exception {
    Schema schema = getSchema("test_nullable_float.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? i + 0.1f : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testDoubleType() throws Exception {
    Schema schema = getSchema("test_primitive_double.avsc");
    ArrayList<Double> data = new ArrayList(Arrays.asList(1.1, 2.2, 3.3, 4.4, 5.5));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableDoubleType() throws Exception {
    Schema schema = getSchema("test_nullable_double.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? i + 0.1 : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
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

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableBytesType() throws Exception {
    Schema schema = getSchema("test_nullable_bytes.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? ByteBuffer.wrap(("test" + i).getBytes(StandardCharsets.UTF_8)) : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testBooleanType() throws Exception {
    Schema schema = getSchema("test_primitive_boolean.avsc");
    ArrayList<Boolean> data = new ArrayList(Arrays.asList(true, false, true, false, true));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(data, vector);
  }

  @Test
  public void testNullableBooleanType() throws Exception {
    Schema schema = getSchema("test_nullable_boolean.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? true : null);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(schema, data, root);
  }

  @Test
  public void testArrayType() throws Exception {
    Schema schema = getSchema("test_array.avsc");
    List<List> data = new ArrayList(Arrays.asList(
        Arrays.asList("11", "222", "999"),
        Arrays.asList("12222", "2333", "1000"),
        Arrays.asList("1rrr", "2ggg"),
        Arrays.asList("1vvv", "2bbb"),
        Arrays.asList("1fff", "2")));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkArrayResult(data, (ListVector) vector);
  }

  @Test
  public void testMapType() throws Exception {
    Schema schema = getSchema("test_map.avsc");

    List keys = Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6");
    List vals = Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6");

    List<LinkedHashMap> data = new ArrayList<>();
    LinkedHashMap map1 = new LinkedHashMap();
    map1.put(keys.get(0), vals.get(0));
    map1.put(keys.get(1), vals.get(1));
    data.add(map1);

    LinkedHashMap map2 = new LinkedHashMap();
    map2.put(keys.get(2), vals.get(2));
    map2.put(keys.get(3), vals.get(3));
    data.add(map2);

    LinkedHashMap map3 = new LinkedHashMap();
    map3.put(keys.get(4), vals.get(4));
    map3.put(keys.get(5), vals.get(5));
    data.add(map3);

    VectorSchemaRoot root = writeAndRead(schema, data);
    MapVector vector = (MapVector) root.getFieldVectors().get(0);

    checkPrimitiveResult(keys, vector.getDataVector().getChildrenFromFields().get(0));
    checkPrimitiveResult(vals, vector.getDataVector().getChildrenFromFields().get(1));
    assertEquals(0, vector.getOffsetBuffer().getInt(0));
    assertEquals(2, vector.getOffsetBuffer().getInt(1 * 4));
    assertEquals(4, vector.getOffsetBuffer().getInt(2 * 4));
    assertEquals(6, vector.getOffsetBuffer().getInt(3 * 4));
  }

  @Test
  public void testFixedType() throws Exception {
    Schema schema = getSchema("test_fixed.avsc");

    List<GenericData.Fixed> data = new ArrayList<>();
    List<byte[]> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      byte[] value = ("value" + i).getBytes(StandardCharsets.UTF_8);
      expected.add(value);
      GenericData.Fixed fixed = new GenericData.Fixed(schema);
      fixed.bytes(value);
      data.add(fixed);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testUnionType() throws Exception {
    Schema schema = getSchema("test_union.avsc");
    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<Object> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0 ? "test" + i : i);
      expected.add(i % 2 == 0 ? "test" + i : i);
      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  @Test
  public void testNullableUnionType() throws Exception {
    Schema schema = getSchema("test_nullable_union.avsc");
    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<Object> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      if (i % 3 == 0) {
        record.put(0, "test" + i);
        expected.add("test" + i);
        data.add(record);
      } else if (i % 3 == 1) {
        record.put(0, i);
        expected.add(i);
        data.add(record);
      } else {
        record.put(0, null);
        expected.add(null);
        data.add(record);
      }
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expected, vector);
  }

  private void checkArrayResult(List<List> expected, ListVector vector) {
    assertEquals(expected.size(), vector.getValueCount());
    for (int i = 0; i < expected.size(); i++) {
      checkPrimitiveResult(expected.get(i), (JsonStringArrayList) vector.getObject(i));
    }
  }

  private void checkPrimitiveResult(List expected, List actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Object value1 = expected.get(i);
      Object value2 = actual.get(i);
      if (value1 == null) {
        assertTrue(value2 == null);
        continue;
      }
      if (value2 instanceof byte[]) {
        value2 = ByteBuffer.wrap((byte[]) value2);
      } else if (value2 instanceof Text) {
        value2 = value2.toString();
      }
      assertTrue(Objects.equals(value1, value2));
    }
  }

  private void checkPrimitiveResult(List data, FieldVector vector) {
    assertEquals(data.size(), vector.getValueCount());
    for (int i = 0; i < data.size(); i++) {
      Object value1 = data.get(i);
      Object value2 = vector.getObject(i);
      if (value1 == null) {
        assertTrue(value2 == null);
        continue;
      }
      if (value2 instanceof byte[]) {
        value2 = ByteBuffer.wrap((byte[]) value2);
        if (value1 instanceof byte[]) {
          value1 = ByteBuffer.wrap((byte[]) value1);
        }
      } else if (value2 instanceof Text) {
        value2 = value2.toString();
      }
      assertTrue(Objects.equals(value1, value2));
    }
  }

  private void checkRecordResult(Schema schema, ArrayList<GenericRecord> data, VectorSchemaRoot root) {
    assertEquals(data.size(), root.getRowCount());
    assertEquals(schema.getFields().size(), root.getFieldVectors().size());

    for (int i = 0; i < schema.getFields().size(); i++) {
      ArrayList fieldData = new ArrayList();
      for (GenericRecord record : data) {
        fieldData.add(record.get(i));
      }

      checkPrimitiveResult(fieldData, root.getFieldVectors().get(i));
    }

  }
}
