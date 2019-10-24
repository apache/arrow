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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

public class AvroToArrowTest extends AvroTestBase {

  private VectorSchemaRoot writeAndRead(Schema schema, List data) throws Exception {
    File dataFile = TMP.newFile();

    BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(new FileOutputStream(dataFile), null);
    DatumWriter writer = new GenericDatumWriter(schema);
    BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(new FileInputStream(dataFile), null);

    for (Object value : data) {
      writer.write(value, encoder);
    }

    return AvroToArrow.avroToArrow(schema, decoder, config);
  }

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
  public void testSkipUnionWithOneField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_union_before.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_union_one_field_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i % 2 == 0 ? "test" + i : null);
      record.put(2, i % 2 == 0 ? "test" + i : i);
      record.put(3, i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(1));
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedData.add(expectedRecord);
    }
    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipUnionWithNullableOneField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_union_before.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_union_nullable_field_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i % 2 == 0 ? "test" + i : null);
      record.put(2, i % 2 == 0 ? "test" + i : i);
      record.put(3, i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedData.add(expectedRecord);
    }
    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipUnionWithMultiFields() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f2");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_union_before.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_union_multi_fields_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i % 2 == 0 ? "test" + i : null);
      record.put(2, i % 2 == 0 ? "test" + i : i);
      record.put(3, i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(1));
      expectedRecord.put(2, record.get(3));
      expectedData.add(expectedRecord);
    }
    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipArrayField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_array_before.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_array_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, Arrays.asList("test" + i, "test" + i));
      record.put(2, i % 2 == 0);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(2));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipMultiFields() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    skipFieldNames.add("f2");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("test_record.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_multi_fields_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i);
      record.put(2, i % 2 == 0);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("test_record.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_single_field_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, i);
      record.put(2, i % 2 == 0);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(2));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipNestedFields() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0.f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("test_nested_record.avsc");
    Schema nestedSchema = schema.getFields().get(0).schema();
    ArrayList<GenericRecord> data = new ArrayList<>();

    Schema expectedSchema = getSchema("skip/test_skip_second_level_expected.avsc");
    Schema expectedNestedSchema = expectedSchema.getFields().get(0).schema();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
      nestedRecord.put(0, "test" + i);
      nestedRecord.put(1, i);
      record.put(0, nestedRecord);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      GenericRecord expectedNestedRecord = new GenericData.Record(expectedNestedSchema);
      expectedNestedRecord.put(0, nestedRecord.get(1));
      expectedRecord.put(0, expectedNestedRecord);
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkNestedRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipThirdLevelField() throws Exception {
    Schema firstLevelSchema = getSchema("skip/test_skip_third_level_expected.avsc");
    Schema secondLevelSchema = firstLevelSchema.getFields().get(0).schema();
    Schema thirdLevelSchema = secondLevelSchema.getFields().get(0).schema();

    ArrayList<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord firstLevelRecord = new GenericData.Record(firstLevelSchema);
      GenericRecord secondLevelRecord = new GenericData.Record(secondLevelSchema);
      GenericRecord thirdLevelRecord = new GenericData.Record(thirdLevelSchema);

      thirdLevelRecord.put(0, i);
      thirdLevelRecord.put(1, "test" + i);
      thirdLevelRecord.put(2, i % 2 == 0);

      secondLevelRecord.put(0, thirdLevelRecord);
      firstLevelRecord.put(0, secondLevelRecord);
      data.add(firstLevelRecord);
    }

    // do not skip any fields first
    VectorSchemaRoot root1 = writeAndRead(firstLevelSchema, data);

    assertEquals(1, root1.getFieldVectors().size());
    assertEquals(Types.MinorType.STRUCT, root1.getFieldVectors().get(0).getMinorType());
    StructVector secondLevelVector = (StructVector) root1.getFieldVectors().get(0);
    assertEquals(1, secondLevelVector.getChildrenFromFields().size());
    assertEquals(Types.MinorType.STRUCT, secondLevelVector.getChildrenFromFields().get(0).getMinorType());
    StructVector thirdLevelVector = (StructVector) secondLevelVector.getChildrenFromFields().get(0);
    assertEquals(3, thirdLevelVector.getChildrenFromFields().size());

    // skip third level field and validate
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0.f0.f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    VectorSchemaRoot root2 = writeAndRead(firstLevelSchema, data);

    assertEquals(1, root2.getFieldVectors().size());
    assertEquals(Types.MinorType.STRUCT, root2.getFieldVectors().get(0).getMinorType());
    StructVector secondStruct = (StructVector) root2.getFieldVectors().get(0);
    assertEquals(1, secondStruct.getChildrenFromFields().size());
    assertEquals(Types.MinorType.STRUCT, secondStruct.getChildrenFromFields().get(0).getMinorType());
    StructVector thirdStruct = (StructVector) secondStruct.getChildrenFromFields().get(0);
    assertEquals(2, thirdStruct.getChildrenFromFields().size());

    assertEquals(Types.MinorType.INT, thirdStruct.getChildrenFromFields().get(0).getMinorType());
    assertEquals(Types.MinorType.BIT, thirdStruct.getChildrenFromFields().get(1).getMinorType());
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

    List<Byte> expectedIndices = Arrays.asList((byte)0, (byte)1, (byte)2, (byte)3, (byte)0);

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
