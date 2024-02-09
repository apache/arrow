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

package org.apache.arrow.vector.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.ComplexWriterImpl;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJSONFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestJSONFile.class);

  @Test
  public void testNoBatches() throws IOException {
    File file = new File("target/no_batches.json");

    try (BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      BaseWriter.ComplexWriter writer = new ComplexWriterImpl("root", parent);
      BaseWriter.StructWriter rootWriter = writer.rootAsStruct();
      rootWriter.integer("int");
      rootWriter.uInt1("uint1");
      rootWriter.bigInt("bigInt");
      rootWriter.float4("float");
      JsonFileWriter jsonWriter = new JsonFileWriter(file, JsonFileWriter.config().pretty(true));
      jsonWriter.start(new VectorSchemaRoot(parent.getChild("root")).getSchema(), null);
      jsonWriter.close();
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);
    }
  }

  @Test
  public void testWriteRead() throws IOException {
    File file = new File("target/mytest.json");
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeData(count, parent);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateContent(count, root);
      }
    }
  }

  @Test
  public void testWriteReadComplexJSON() throws IOException {
    File file = new File("target/mytest_complex.json");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeComplexData(count, parent);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator);
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateComplexContent(count, root);
      }
    }
  }

  @Test
  public void testWriteComplexJSON() throws IOException {
    File file = new File("target/mytest_write_complex.json");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeComplexData(count, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateComplexContent(root.getRowCount(), root);
      writeJSON(file, root, null);
    }
  }

  public void writeJSON(File file, VectorSchemaRoot root, DictionaryProvider provider) throws IOException {
    JsonFileWriter writer = new JsonFileWriter(file, JsonFileWriter.config().pretty(true));
    writer.start(root.getSchema(), provider);
    writer.write(root);
    writer.close();
  }


  @Test
  public void testWriteReadUnionJSON() throws IOException {
    File file = new File("target/mytest_write_union.json");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeUnionData(count, parent);
      printVectors(parent.getChildrenFromFields());

      try (VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"))) {
        validateUnionData(count, root);
        writeJSON(file, root, null);

        // read
        try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE)) {
          JsonFileReader reader = new JsonFileReader(file, readerAllocator);

          Schema schema = reader.start();
          LOGGER.debug("reading schema: " + schema);

          try (VectorSchemaRoot rootFromJson = reader.read();) {
            validateUnionData(count, rootFromJson);
            Validator.compareVectorSchemaRoot(root, rootFromJson);
          }
        }
      }
    }
  }

  @Test
  public void testWriteReadDateTimeJSON() throws IOException {
    File file = new File("target/mytest_datetime.json");
    int count = COUNT;

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {

      writeDateTimeData(count, parent);

      printVectors(parent.getChildrenFromFields());

      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateDateTimeContent(count, root);

      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateDateTimeContent(count, root);
      }
    }
  }

  @Test
  public void testWriteReadDictionaryJSON() throws IOException {
    File file = new File("target/mytest_dictionary.json");

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)
    ) {
      MapDictionaryProvider provider = new MapDictionaryProvider();

      try (VectorSchemaRoot root = writeFlatDictionaryData(vectorAllocator, provider)) {
        printVectors(root.getFieldVectors());
        validateFlatDictionary(root, provider);
        writeJSON(file, root, provider);
      }

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateFlatDictionary(root, reader);
      }
    }
  }

  @Test
  public void testWriteReadNestedDictionaryJSON() throws IOException {
    File file = new File("target/mytest_dict_nested.json");

    // data being written:
    // [['foo', 'bar'], ['foo'], ['bar']] -> [[0, 1], [0], [1]]

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)
    ) {
      MapDictionaryProvider provider = new MapDictionaryProvider();

      try (VectorSchemaRoot root = writeNestedDictionaryData(vectorAllocator, provider)) {
        printVectors(root.getFieldVectors());
        validateNestedDictionary(root, provider);
        writeJSON(file, root, provider);
      }

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateNestedDictionary(root, reader);
      }
    }
  }

  @Test
  public void testWriteReadDecimalJSON() throws IOException {
    File file = new File("target/mytest_decimal.json");

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot root = writeDecimalData(vectorAllocator)) {
      printVectors(root.getFieldVectors());
      validateDecimalData(root);
      writeJSON(file, root, null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateDecimalData(root);
      }
    }
  }

  @Test
  public void testSetStructLength() throws IOException {
    File file = new File("../../docs/source/format/integration_json_examples/struct.json");
    if (!file.exists()) {
      file = new File("../docs/source/format/integration_json_examples/struct.json");
    }
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        FieldVector vector = root.getVector("struct_nullable");
        Assert.assertEquals(7, vector.getValueCount());
      }
    }
  }

  @Test
  public void testWriteReadVarBinJSON() throws IOException {
    File file = new File("target/mytest_varbin.json");
    int count = COUNT;

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeVarBinaryData(count, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateVarBinary(count, root);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateVarBinary(count, root);
      }
    }
  }

  @Test
  public void testWriteReadMapJSON() throws IOException {
    File file = new File("target/mytest_map.json");

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot root = writeMapData(vectorAllocator)) {
      printVectors(root.getFieldVectors());
      validateMapData(root);
      writeJSON(file, root, null);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)) {
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateMapData(root);
      }
    }
  }

  @Test
  public void testWriteReadNullJSON() throws IOException {
    File file = new File("target/mytest_null.json");
    int valueCount = 10;

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        VectorSchemaRoot root = writeNullData(valueCount)) {
      printVectors(root.getFieldVectors());
      validateNullData(root, valueCount);
      writeJSON(file, root, null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        JsonFileReader reader = new JsonFileReader(file, readerAllocator)
    ) {

      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateNullData(root, valueCount);
      }
    }
  }

  /** Regression test for ARROW-17107. */
  @Test
  public void testRoundtripEmptyVector() throws Exception {
    final List<Field> fields = Arrays.asList(
        Field.nullable("utf8", ArrowType.Utf8.INSTANCE),
        Field.nullable("largeutf8", ArrowType.LargeUtf8.INSTANCE),
        Field.nullable("binary", ArrowType.Binary.INSTANCE),
        Field.nullable("largebinary", ArrowType.LargeBinary.INSTANCE),
        Field.nullable("fixedsizebinary", new ArrowType.FixedSizeBinary(2)),
        Field.nullable("decimal128", new ArrowType.Decimal(3, 2, 128)),
        Field.nullable("decimal128", new ArrowType.Decimal(3, 2, 256)),
        new Field("list", FieldType.nullable(ArrowType.List.INSTANCE),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("largelist", FieldType.nullable(ArrowType.LargeList.INSTANCE),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("map", FieldType.nullable(new ArrowType.Map(/*keyssorted*/ false)),
            Collections.singletonList(new Field("items", FieldType.notNullable(ArrowType.Struct.INSTANCE),
                Arrays.asList(Field.notNullable("keys", new ArrowType.Int(32, true)),
                    Field.nullable("values", new ArrowType.Int(32, true)))))),
        new Field("fixedsizelist", FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("denseunion", FieldType.nullable(new ArrowType.Union(UnionMode.Dense, new int[] {0})),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true)))),
        new Field("sparseunion", FieldType.nullable(new ArrowType.Union(UnionMode.Sparse, new int[] {0})),
            Collections.singletonList(Field.nullable("items", new ArrowType.Int(32, true))))
    );

    for (final Field field : fields) {
      final Schema schema = new Schema(Collections.singletonList(field));
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        Path outputPath = Files.createTempFile("arrow-", ".json");
        File outputFile = outputPath.toFile();
        outputFile.deleteOnExit();

        // Try with no allocation
        try (final JsonFileWriter jsonWriter = new JsonFileWriter(outputFile, JsonFileWriter.config().pretty(true))) {
          jsonWriter.start(schema, null);
          jsonWriter.write(root);
        } catch (Exception e) {
          throw new RuntimeException("Test failed for empty vector of type " + field, e);
        }

        try (JsonFileReader reader = new JsonFileReader(outputFile, allocator)) {
          final Schema readSchema = reader.start();
          assertEquals(schema, readSchema);
          try (final VectorSchemaRoot data = reader.read()) {
            assertNotNull(data);
            assertEquals(0, data.getRowCount());
          }
          assertNull(reader.read());
        }

        // Try with an explicit allocation
        root.allocateNew();
        root.setRowCount(0);
        try (final JsonFileWriter jsonWriter = new JsonFileWriter(outputFile, JsonFileWriter.config().pretty(true))) {
          jsonWriter.start(schema, null);
          jsonWriter.write(root);
        } catch (Exception e) {
          throw new RuntimeException("Test failed for empty vector of type " + field, e);
        }

        try (JsonFileReader reader = new JsonFileReader(outputFile, allocator)) {
          final Schema readSchema = reader.start();
          assertEquals(schema, readSchema);
          try (final VectorSchemaRoot data = reader.read()) {
            assertNotNull(data);
            assertEquals(0, data.getRowCount());
          }
          assertNull(reader.read());
        }
      }
    }
  }
}
