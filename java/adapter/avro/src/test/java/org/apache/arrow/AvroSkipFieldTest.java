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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class AvroSkipFieldTest extends AvroTestBase {

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
  public void testSkipMapField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_map_before.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_map_expected.avsc");

    HashMap map = new HashMap();
    map.put("key1", "value1");
    map.put("key2", "value3");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, "test" + i);
      record.put(1, map);
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
  public void testSkipStringField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f2");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base1.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_string_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final byte[] testBytes = ("test" + i).getBytes();
      GenericRecord record = new GenericData.Record(schema);
      GenericData.Fixed fixed = new GenericData.Fixed(schema.getField("f0").schema());
      fixed.bytes(testBytes);
      record.put(0, fixed);
      GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(schema.getField("f1").schema(), "TEST" + i % 2);
      record.put(1, symbol);
      record.put(2, "testtest" + i);
      record.put(3, ByteBuffer.wrap(testBytes));
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, testBytes);
      expectedRecord.put(1, (byte) i % 2);
      expectedRecord.put(2, testBytes);
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipBytesField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f3");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base1.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_bytes_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final byte[] testBytes = ("test" + i).getBytes();
      GenericRecord record = new GenericData.Record(schema);
      GenericData.Fixed fixed = new GenericData.Fixed(schema.getField("f0").schema());
      fixed.bytes(testBytes);
      record.put(0, fixed);
      GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(schema.getField("f1").schema(), "TEST" + i % 2);
      record.put(1, symbol);
      record.put(2, "testtest" + i);
      record.put(3, ByteBuffer.wrap(testBytes));
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, testBytes);
      expectedRecord.put(1, (byte) i % 2);
      expectedRecord.put(2, record.get(2));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipFixedField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base1.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_fixed_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final byte[] testBytes = ("test" + i).getBytes();
      GenericRecord record = new GenericData.Record(schema);
      GenericData.Fixed fixed = new GenericData.Fixed(schema.getField("f0").schema());
      fixed.bytes(testBytes);
      record.put(0, fixed);
      GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(schema.getField("f1").schema(), "TEST" + i % 2);
      record.put(1, symbol);
      record.put(2, "testtest" + i);
      record.put(3, ByteBuffer.wrap(testBytes));
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, (byte) i % 2);
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipEnumField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base1.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_fixed_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final byte[] testBytes = ("test" + i).getBytes();
      GenericRecord record = new GenericData.Record(schema);
      GenericData.Fixed fixed = new GenericData.Fixed(schema.getField("f0").schema());
      fixed.bytes(testBytes);
      record.put(0, fixed);
      GenericData.EnumSymbol symbol = new GenericData.EnumSymbol(schema.getField("f1").schema(), "TEST" + i % 2);
      record.put(1, symbol);
      record.put(2, "testtest" + i);
      record.put(3, ByteBuffer.wrap(testBytes));
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, testBytes);
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipBooleanField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base2.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_boolean_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0);
      record.put(1, i);
      record.put(2, (long) i);
      record.put(3, (float) i);
      record.put(4, (double) i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(1));
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedRecord.put(3, record.get(4));

      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipIntField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f1");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base2.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_int_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0);
      record.put(1, i);
      record.put(2, (long) i);
      record.put(3, (float) i);
      record.put(4, (double) i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(2));
      expectedRecord.put(2, record.get(3));
      expectedRecord.put(3, record.get(4));

      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipLongField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f2");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base2.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_long_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0);
      record.put(1, i);
      record.put(2, (long) i);
      record.put(3, (float) i);
      record.put(4, (double) i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(1));
      expectedRecord.put(2, record.get(3));
      expectedRecord.put(3, record.get(4));

      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipFloatField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f3");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base2.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_float_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0);
      record.put(1, i);
      record.put(2, (long) i);
      record.put(3, (float) i);
      record.put(4, (double) i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(1));
      expectedRecord.put(2, record.get(2));
      expectedRecord.put(3, record.get(4));

      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipDoubleField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f4");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_base2.avsc");
    Schema expectedSchema = getSchema("skip/test_skip_double_expected.avsc");

    ArrayList<GenericRecord> data = new ArrayList<>();
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put(0, i % 2 == 0);
      record.put(1, i);
      record.put(2, (long) i);
      record.put(3, (float) i);
      record.put(4, (double) i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, record.get(0));
      expectedRecord.put(1, record.get(1));
      expectedRecord.put(2, record.get(2));
      expectedRecord.put(3, record.get(3));

      expectedData.add(expectedRecord);
    }

    VectorSchemaRoot root = writeAndRead(schema, data);
    checkRecordResult(expectedSchema, expectedData, root);
  }

  @Test
  public void testSkipRecordField() throws Exception {
    Set<String> skipFieldNames = new HashSet<>();
    skipFieldNames.add("f0");
    config = new AvroToArrowConfigBuilder(config.getAllocator()).setSkipFieldNames(skipFieldNames).build();
    Schema schema = getSchema("skip/test_skip_record_before.avsc");
    Schema nestedSchema = schema.getFields().get(0).schema();
    ArrayList<GenericRecord> data = new ArrayList<>();

    Schema expectedSchema = getSchema("skip/test_skip_record_expected.avsc");
    ArrayList<GenericRecord> expectedData = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      GenericRecord nestedRecord = new GenericData.Record(nestedSchema);
      nestedRecord.put(0, "test" + i);
      nestedRecord.put(1, i);
      record.put(0, nestedRecord);
      record.put(1, i);
      data.add(record);

      GenericRecord expectedRecord = new GenericData.Record(expectedSchema);
      expectedRecord.put(0, i);
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
}
