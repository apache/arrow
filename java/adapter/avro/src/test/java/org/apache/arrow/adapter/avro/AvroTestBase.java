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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.Text;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

public class AvroTestBase {

  @ClassRule
  public static final TemporaryFolder TMP = new TemporaryFolder();

  protected AvroToArrowConfig config;

  @Before
  public void init() {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    config = new AvroToArrowConfigBuilder(allocator).build();
  }

  public static Schema getSchema(String schemaName) throws Exception {
    try {
      // Attempt to use JDK 9 behavior of getting the module then the resource stream from the module.
      // Note that this code is caller-sensitive.
      Method getModuleMethod = Class.class.getMethod("getModule");
      Object module = getModuleMethod.invoke(TestWriteReadAvroRecord.class);
      Method getResourceAsStreamFromModule = module.getClass().getMethod("getResourceAsStream", String.class);
      try (InputStream is = (InputStream) getResourceAsStreamFromModule.invoke(module, "/schema/" + schemaName)) {
        return new Schema.Parser()
            .parse(is);
      }
    } catch (NoSuchMethodException ex) {
      // Use JDK8 behavior.
      try (InputStream is = TestWriteReadAvroRecord.class.getResourceAsStream("/schema/" + schemaName)) {
        return new Schema.Parser().parse(is);
      }
    }
  }

  protected VectorSchemaRoot writeAndRead(Schema schema, List data) throws Exception {
    File dataFile = TMP.newFile();

    BinaryEncoder
        encoder = new EncoderFactory().directBinaryEncoder(new FileOutputStream(dataFile), null);
    DatumWriter writer = new GenericDatumWriter(schema);
    BinaryDecoder
        decoder = new DecoderFactory().directBinaryDecoder(new FileInputStream(dataFile), null);

    for (Object value : data) {
      writer.write(value, encoder);
    }

    return AvroToArrow.avroToArrow(schema, decoder, config);
  }

  protected void checkArrayResult(List<List<?>> expected, ListVector vector) {
    assertEquals(expected.size(), vector.getValueCount());
    for (int i = 0; i < expected.size(); i++) {
      checkArrayElement(expected.get(i), vector.getObject(i));
    }
  }

  protected void checkArrayElement(List expected, List actual) {
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
      assertEquals(value1, value2);
    }
  }

  protected void checkPrimitiveResult(List data, FieldVector vector) {
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
      } else if (value2 instanceof Byte) {
        value2 = ((Byte) value2).intValue();
      }
      assertEquals(value1, value2);
    }
  }

  protected void checkRecordResult(Schema schema, ArrayList<GenericRecord> data, VectorSchemaRoot root) {
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

  protected void checkNestedRecordResult(Schema schema, List<GenericRecord> data, VectorSchemaRoot root) {
    assertEquals(data.size(), root.getRowCount());
    assertTrue(schema.getFields().size() == 1);

    final Schema nestedSchema = schema.getFields().get(0).schema();
    final StructVector structVector = (StructVector) root.getFieldVectors().get(0);

    for (int i = 0; i < nestedSchema.getFields().size(); i++) {
      ArrayList fieldData = new ArrayList();
      for (GenericRecord record : data) {
        GenericRecord nestedRecord = (GenericRecord) record.get(0);
        fieldData.add(nestedRecord.get(i));
      }

      checkPrimitiveResult(fieldData, structVector.getChildrenFromFields().get(i));
    }

  }


  // belows are for iterator api

  protected void checkArrayResult(List<List<?>> expected, List<ListVector> vectors) {
    int valueCount = vectors.stream().mapToInt(v -> v.getValueCount()).sum();
    assertEquals(expected.size(), valueCount);

    int index = 0;
    for (ListVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        checkArrayElement(expected.get(index++), vector.getObject(i));
      }
    }
  }

  protected void checkRecordResult(Schema schema, ArrayList<GenericRecord> data, List<VectorSchemaRoot> roots) {
    roots.forEach(root -> {
      assertEquals(schema.getFields().size(), root.getFieldVectors().size());
    });

    for (int i = 0; i < schema.getFields().size(); i++) {
      List fieldData = new ArrayList();
      List<FieldVector> vectors = new ArrayList<>();
      for (GenericRecord record : data) {
        fieldData.add(record.get(i));
      }
      final int columnIndex = i;
      roots.forEach(root -> vectors.add(root.getFieldVectors().get(columnIndex)));

      checkPrimitiveResult(fieldData, vectors);
    }

  }

  protected void checkPrimitiveResult(List data, List<FieldVector> vectors) {
    int valueCount = vectors.stream().mapToInt(v -> v.getValueCount()).sum();
    assertEquals(data.size(), valueCount);

    int index = 0;
    for (FieldVector vector : vectors) {
      for (int i = 0; i < vector.getValueCount(); i++) {
        Object value1 = data.get(index++);
        Object value2 = vector.getObject(i);
        if (value1 == null) {
          assertNull(value2);
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
        assertEquals(value1, value2);
      }
    }
  }
}
