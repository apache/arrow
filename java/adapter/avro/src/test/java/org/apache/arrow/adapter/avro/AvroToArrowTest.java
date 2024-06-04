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

package org.apache.arrow.adapter.avro;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroToArrowTest extends AvroTestBase {

  @Test
  public void testStringType() throws Exception {
    Schema schema = getSchema("test_primitive_string.avsc");
    List<String> data = Arrays.asList("v1", "v2", "v3", "v4", "v5");

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
  public void testFixedAttributes() throws Exception {
    Schema schema = getSchema("attrs/test_fixed_attr.avsc");

    List<GenericData.Fixed> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      byte[] value = ("value" + i).getBytes(StandardCharsets.UTF_8);
      GenericData.Fixed fixed = new GenericData.Fixed(schema);
      fixed.bytes(value);
      data.add(fixed);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    Map<String, String> metadata = vector.getField().getMetadata();
    assertEquals("fixed doc", metadata.get("doc"));
    assertEquals("[\"alias1\",\"alias2\"]", metadata.get("aliases"));
  }

  @Test
  public void testEnumAttributes() throws Exception {
    Schema schema = getSchema("attrs/test_enum_attrs.avsc");
    List<GenericData.EnumSymbol> data = Arrays.asList(
        new GenericData.EnumSymbol(schema, "SPADES"),
        new GenericData.EnumSymbol(schema, "HEARTS"),
        new GenericData.EnumSymbol(schema, "DIAMONDS"),
        new GenericData.EnumSymbol(schema, "CLUBS"),
        new GenericData.EnumSymbol(schema, "SPADES"));

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    Map<String, String> metadata = vector.getField().getMetadata();
    assertEquals("enum doc", metadata.get("doc"));
    assertEquals("[\"alias1\",\"alias2\"]", metadata.get("aliases"));
  }

  @Test
  public void testRecordAttributes() throws Exception {
    Schema schema = getSchema("attrs/test_record_attrs.avsc");
    Schema nestedSchema = schema.getFields().get(0).schema();
    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
      nestedRecord.put(0, "test" + i);
      nestedRecord.put(1, i);
      record.put(0, nestedRecord);

      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);

    StructVector structVector = (StructVector) root.getFieldVectors().get(0);
    Map<String, String> structMeta = structVector.getField().getMetadata();
    Map<String, String> childMeta1 = structVector.getChildByOrdinal(0).getField().getMetadata();
    Map<String, String> childMeta2 = structVector.getChildByOrdinal(1).getField().getMetadata();

    assertEquals("f0 doc", structMeta.get("doc"));
    assertEquals("[\"f0.a1\"]", structMeta.get("aliases"));
    assertEquals("f1 doc", childMeta1.get("doc"));
    assertEquals("[\"f1.a1\",\"f1.a2\"]", childMeta1.get("aliases"));
    assertEquals("f2 doc", childMeta2.get("doc"));
    assertEquals("[\"f2.a1\",\"f2.a2\"]", childMeta2.get("aliases"));
  }

  @Test
  public void testNestedRecordType() throws Exception {
    Schema schema = getSchema("test_nested_record.avsc");
    Schema nestedSchema = schema.getFields().get(0).schema();
    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
      nestedRecord.put(0, "test" + i);
      nestedRecord.put(1, i);
      record.put(0, nestedRecord);

      data.add(record);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkNestedRecordResult(schema, data, root);
  }

  @Test
  public void testEnumType() throws Exception {
    Schema schema = getSchema("test_primitive_enum.avsc");
    List<GenericData.EnumSymbol> data = Arrays.asList(
        new GenericData.EnumSymbol(schema, "SPADES"),
        new GenericData.EnumSymbol(schema, "HEARTS"),
        new GenericData.EnumSymbol(schema, "DIAMONDS"),
        new GenericData.EnumSymbol(schema, "CLUBS"),
        new GenericData.EnumSymbol(schema, "SPADES"));

    List<Integer> expectedIndices = Arrays.asList(0, 1, 2, 3, 0);

    VectorSchemaRoot root = writeAndRead(schema, data);
    FieldVector vector = root.getFieldVectors().get(0);

    checkPrimitiveResult(expectedIndices, vector);

    VarCharVector dictVector = (VarCharVector) config.getProvider().lookup(0).getVector();
    assertEquals(4, dictVector.getValueCount());

    assertEquals("SPADES", dictVector.getObject(0).toString());
    assertEquals("HEARTS", dictVector.getObject(1).toString());
    assertEquals("DIAMONDS", dictVector.getObject(2).toString());
    assertEquals("CLUBS", dictVector.getObject(3).toString());
  }

  @Test
  public void testIntType() throws Exception {
    Schema schema = getSchema("test_primitive_int.avsc");
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

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
    List<Long> data = Arrays.asList(1L, 2L, 3L, 4L, 5L);

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
    List<Float> data = Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f, 5.5f);

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
    List<Double> data = Arrays.asList(1.1, 2.2, 3.3, 4.4, 5.5);

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
    List<ByteBuffer> data = Arrays.asList(
        ByteBuffer.wrap("value1".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value2".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value3".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value4".getBytes(StandardCharsets.UTF_8)),
        ByteBuffer.wrap("value5".getBytes(StandardCharsets.UTF_8)));

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
    List<Boolean> data = Arrays.asList(true, false, true, false, true);

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
    List<List<?>> data = Arrays.asList(
        Arrays.asList("11", "222", "999"),
        Arrays.asList("12222", "2333", "1000"),
        Arrays.asList("1rrr", "2ggg"),
        Arrays.asList("1vvv", "2bbb"),
        Arrays.asList("1fff", "2"));

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

}
