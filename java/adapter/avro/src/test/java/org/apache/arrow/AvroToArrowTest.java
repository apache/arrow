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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
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

  @Test
  public void testStringType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_string.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", "value" + i);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testIntType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_int.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", i);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testBooleanType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_boolean.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", true);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testLongType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_long.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", (long) i);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testFloatType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_float.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    float suffix = 0.33f;
    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", i + suffix);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testDoubleType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_double.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    double suffix = 0.33;
    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", i + suffix);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testMixedType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_mixed_primitive.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    double suffix = 0.33;
    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("f0", "test" + i);
      record.put("f1", i);
      record.put("f2", i + suffix);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  @Test
  public void testBytesType() throws Exception {
    File dataFile = TMP.newFile();
    Path schemaPath = Paths
        .get(TestWriteReadAvroRecord.class.getResource("/").getPath(), "schema", "test_bytes.avsc");
    Schema schema = new Schema.Parser().parse(schemaPath.toFile());

    //write data to disk
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, dataFile);

    List<GenericRecord> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRecord record = new GenericData.Record(schema);
      ByteBuffer value = ByteBuffer.wrap(("test" + i).getBytes(StandardCharsets.UTF_8));
      record.put("f0", value);
      dataFileWriter.append(record);
      data.add(record);
    }
    dataFileWriter.close();

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    DataFileReader<GenericRecord>
        dataFileReader = new DataFileReader<GenericRecord>(dataFile, datumReader);

    VectorSchemaRoot root = AvroToArrow.readToArrow(dataFileReader, allocator);

    checkResult(schema, data, root);
  }

  private void checkResult(Schema schema, List<GenericRecord> rawRecords, VectorSchemaRoot vectorSchemaRoot) {
    List<Schema.Field> fields = schema.getFields();

    for (int i = 0; i < rawRecords.size(); i++) {
      GenericRecord record = rawRecords.get(i);
      for (Schema.Field field: fields) {
        Object value = record.get(field.name());
        FieldVector vector = vectorSchemaRoot.getVector(field.name());
        if (value == null) {
          assertTrue(vector.isNull(i));
        } else {
          Object value1 = value;
          Object value2 = vector.getObject(i);
          if (field.schema().getType() == Schema.Type.STRING) {
            value1 = value1.toString();
            value2 = value2.toString();
          } else if (field.schema().getType() == Schema.Type.BYTES) {
            //convert to ByteBuffer for equals check
            value2 = ByteBuffer.wrap((byte[]) value2);
          }
          assertEquals(value1, value2);
        }
      }
    }

  }

}
