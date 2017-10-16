/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.file.json;

import java.io.File;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.file.BaseFileTest;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Validator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJSONFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestJSONFile.class);

  @Test
  public void testWriteReadComplexJSON() throws IOException {
    File file = new File("target/mytest_complex.json");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = MapVector.empty("parent", originalVectorAllocator)) {
      writeComplexData(count, parent);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateComplexContent(count, root);
      }
      reader.close();
    }
  }

  @Test
  public void testWriteComplexJSON() throws IOException {
    File file = new File("target/mytest_write_complex.json");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = NullableMapVector.empty("parent", vectorAllocator)) {
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
        NullableMapVector parent = NullableMapVector.empty("parent", vectorAllocator)) {
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
        NullableMapVector parent = NullableMapVector.empty("parent", vectorAllocator)) {

      writeDateTimeData(count, parent);

      printVectors(parent.getChildrenFromFields());

      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateDateTimeContent(count, root);

      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateDateTimeContent(count, root);
      }
      reader.close();
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
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateFlatDictionary(root, reader);
      }
      reader.close();
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
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateNestedDictionary(root, reader);
      }
      reader.close();
    }
  }

  @Test
  public void testWriteReadDecimalJSON() throws IOException {
    File file = new File("target/mytest_decimal.json");

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)
    ) {

      try (VectorSchemaRoot root = writeDecimalData(vectorAllocator)) {
        printVectors(root.getFieldVectors());
        validateDecimalData(root);
        writeJSON(file, root, null);
      }
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateDecimalData(root);
      }
      reader.close();
    }
  }

  @Test
  public void testSetStructLength() throws IOException {
    File file = new File("../../integration/data/struct_example.json");
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
    ) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
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
        NullableMapVector parent = NullableMapVector.empty("parent", vectorAllocator)) {
      writeVarBinaryData(count, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateVarBinary(count, root);
      writeJSON(file, new VectorSchemaRoot(parent.getChild("root")), null);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE)) {
      JsonFileReader reader = new JsonFileReader(file, readerAllocator);
      Schema schema = reader.start();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorSchemaRoot root = reader.read();) {
        validateVarBinary(count, root);
      }
      reader.close();
    }
  }
}
