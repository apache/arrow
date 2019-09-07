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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
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

public class AvroToArrowIteratorTest extends AvroTestBase {

  @Override
  public void init() {
    final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    this.config = new AvroToArrowConfig(allocator, /*targetBatchSize=*/3);
  }

  private AvroToArrowVectorIterator writeAndRead(Schema schema, List data) throws Exception {
    File dataFile = TMP.newFile();

    BinaryEncoder
        encoder = new EncoderFactory().directBinaryEncoder(new FileOutputStream(dataFile), null);
    DatumWriter writer = new GenericDatumWriter(schema);
    BinaryDecoder
        decoder = new DecoderFactory().directBinaryDecoder(new FileInputStream(dataFile), null);

    for (Object value : data) {
      writer.write(value, encoder);
    }

    return AvroToArrow.avroToArrowIterator(schema, decoder, config);
  }

  @Test
  public void testStringType() throws Exception {
    Schema schema = getSchema("test_primitive_string.avsc");
    List<String> data = Arrays.asList("v1", "v2", "v3", "v4", "v5");

    List<VectorSchemaRoot> roots = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    try (AvroToArrowVectorIterator iterator = writeAndRead(schema, data)) {
      while (iterator.hasNext()) {
        VectorSchemaRoot root = iterator.next();
        FieldVector vector = root.getFieldVectors().get(0);
        roots.add(root);
        vectors.add(vector);
      }
    }
    checkPrimitiveResult(data, vectors);
    AutoCloseables.close(roots);
  }

  @Test
  public void testNullableStringType() throws Exception {
    Schema schema = getSchema("test_nullable_string.avsc");

    List<GenericRecord> data = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = new GenericData.Record(schema);
      String value = i % 2 == 0 ? "test" + i : null;
      record.put(0, value);
      expected.add(value);
      data.add(record);
    }

    List<VectorSchemaRoot> roots = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    try (AvroToArrowVectorIterator iterator = writeAndRead(schema, data);) {
      while (iterator.hasNext()) {
        VectorSchemaRoot root = iterator.next();
        FieldVector vector = root.getFieldVectors().get(0);
        roots.add(root);
        vectors.add(vector);
      }
    }
    checkPrimitiveResult(expected, vectors);
    AutoCloseables.close(roots);

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

    List<VectorSchemaRoot> roots = new ArrayList<>();
    try (AvroToArrowVectorIterator iterator = writeAndRead(schema, data)) {
      while (iterator.hasNext()) {
        roots.add(iterator.next());
      }
    }
    checkRecordResult(schema, data, roots);
    AutoCloseables.close(roots);

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

    List<VectorSchemaRoot> roots = new ArrayList<>();
    List<ListVector> vectors = new ArrayList<>();
    try (AvroToArrowVectorIterator iterator = writeAndRead(schema, data)) {
      while (iterator.hasNext()) {
        VectorSchemaRoot root = iterator.next();
        roots.add(root);
        vectors.add((ListVector) root.getFieldVectors().get(0));
      }
    }
    checkArrayResult(data, vectors);
    AutoCloseables.close(roots);
  }
}
