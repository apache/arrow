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

import static java.nio.channels.Channels.newChannel;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestArrowFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestArrowFile.class);

  @Test
  public void testWrite() throws IOException {
    File file = new File("target/mytest_write.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, new ByteArrayOutputStream());
    }
  }

  @Test
  public void testWriteComplex() throws IOException {
    File file = new File("target/mytest_write_complex.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeComplexData(count, parent);
      FieldVector root = parent.getChild("root");
      validateComplexContent(count, new VectorSchemaRoot(root));
      write(root, file, new ByteArrayOutputStream());
    }
  }

  @Test
  public void testMultiBatchWithOneDictionary() throws Exception {
    File file = new File("target/mytest_multi_dictionary.arrow");
    writeSingleDictionary(file);

    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadNextBatch());

      FieldVector encoded = root.getVector("vector");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());
      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("foo", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("bar", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }

      assertTrue(reader.loadNextBatch());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("bar", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("foo", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }

      assertTrue(reader.loadNextBatch());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("baz", decoded.getObject(0).toString());
        assertEquals("baz", decoded.getObject(1).toString());
      }
    }

    // load just the 3rd batch.
    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadRecordBatch(reader.getRecordBlocks().get(2)));

      FieldVector encoded = root.getVector("vector");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("baz", decoded.getObject(0).toString());
        assertEquals("baz", decoded.getObject(1).toString());
      }
    }

    // load just the first batch.
    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadRecordBatch(reader.getRecordBlocks().get(0)));

      FieldVector encoded = root.getVector("vector");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("foo", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("bar", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }
    }
  }

  @Test
  public void testMultiBatchWithTwoDictionaries() throws Exception {
    File file = new File("target/mytest_multi_dictionaries.arrow");
    writeTwoDictionaries(file);

    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadNextBatch());

      FieldVector encoded = root.getVector("vector1");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());
      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("foo", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("bar", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }

      FieldVector encoded2 = root.getVector("vector2");
      DictionaryEncoding dictionaryEncoding2 = encoded2.getField().getDictionary();
      Dictionary dictionary2 = reader.getDictionaryVectors().get(dictionaryEncoding2.getId());
      try (ValueVector decoded = DictionaryEncoder.decode(encoded2, dictionary2)) {
        assertNull(decoded.getObject(0));
        assertNull(decoded.getObject(1));
        assertEquals("bur", decoded.getObject(2).toString());
        assertNull(decoded.getObject(3));
      }

      assertTrue(reader.loadNextBatch());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("bar", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("foo", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }

      try (ValueVector decoded = DictionaryEncoder.decode(encoded2, dictionary2)) {
        assertEquals("arg", decoded.getObject(0).toString());
        assertNull(decoded.getObject(1));
        assertNull(decoded.getObject(2));
        assertNull(decoded.getObject(3));
      }

      assertTrue(reader.loadNextBatch());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("baz", decoded.getObject(0).toString());
        assertEquals("baz", decoded.getObject(1).toString());
      }

      try (ValueVector decoded = DictionaryEncoder.decode(encoded2, dictionary2)) {
        for (int i = 0; i < 4; i++) {
          assertNull(decoded.getObject(i));
        }
      }
    }

    // load just the 3rd batch.
    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadRecordBatch(reader.getRecordBlocks().get(2)));

      FieldVector encoded = root.getVector("vector1");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("baz", decoded.getObject(0).toString());
        assertEquals("baz", decoded.getObject(1).toString());
      }
    }

    // load just the first batch.
    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      assertEquals(reader.getRecordBlocks().size(), 3);
      assertTrue(reader.loadRecordBatch(reader.getRecordBlocks().get(0)));

      FieldVector encoded = root.getVector("vector1");
      DictionaryEncoding dictionaryEncoding = encoded.getField().getDictionary();
      Dictionary dictionary = reader.getDictionaryVectors().get(dictionaryEncoding.getId());

      try (ValueVector decoded = DictionaryEncoder.decode(encoded, dictionary)) {
        assertEquals("foo", decoded.getObject(0).toString());
        assertEquals("bar", decoded.getObject(1).toString());
        assertEquals("bar", decoded.getObject(2).toString());
        assertEquals("foo", decoded.getObject(3).toString());
      }
    }
  }

  /**
   * Writes the contents of parents to file. If outStream is non-null, also writes it
   * to outStream in the streaming serialized format.
   */
  private void write(FieldVector parent, File file, OutputStream outStream) throws IOException {
    VectorSchemaRoot root = new VectorSchemaRoot(parent);

    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());) {
      LOGGER.debug("writing schema: " + root.getSchema());
      arrowWriter.start();
      arrowWriter.writeBatch();
      arrowWriter.end();
    }

    // Also try serializing to the stream writer.
    if (outStream != null) {
      try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(root, null, outStream)) {
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
    }
  }

  @Test
  public void testFileStreamHasEos() throws IOException {
    try (VarCharVector vector1 = newVarCharVector("varchar1", allocator)) {
      vector1.allocateNewSafe();
      vector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      vector1.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      vector1.set(3, "baz".getBytes(StandardCharsets.UTF_8));
      vector1.set(4, "bar".getBytes(StandardCharsets.UTF_8));
      vector1.set(5, "baz".getBytes(StandardCharsets.UTF_8));
      vector1.setValueCount(6);

      List<Field> fields = Arrays.asList(vector1.getField());
      List<FieldVector> vectors = Collections2.asImmutableList(vector1);
      VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector1.getValueCount());

      // write data
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out));
      writer.start();
      writer.writeBatch();
      writer.end();

      byte[] bytes = out.toByteArray();
      byte[] bytesWithoutMagic = new byte[bytes.length - 8];
      System.arraycopy(bytes, 8, bytesWithoutMagic, 0, bytesWithoutMagic.length);

      try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytesWithoutMagic), allocator)) {
        assertTrue(reader.loadNextBatch());
        // here will throw exception if read footer instead of eos.
        assertFalse(reader.loadNextBatch());
      }
    }
  }

  private void writeSingleDictionary(File file) throws Exception {
    Map<String, Integer> stringToIndex = new HashMap<>();

    try (VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator)) {
      DictionaryEncoding dictionaryEncoding = new DictionaryEncoding(42, false, new ArrowType.Int(16, false));

      Dictionary dictionary = new Dictionary(dictionaryVector, dictionaryEncoding);
      DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
      provider.put(dictionary);

      try (UInt2Vector vector = new UInt2Vector(
          "vector",
          new FieldType(false, new ArrowType.Int(16, false), dictionaryEncoding),
          allocator)) {
        vector.allocateNew(4);
        dictionaryVector.allocateNew(2);

        dictionaryVector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
        stringToIndex.put("foo", 0);
        dictionaryVector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
        stringToIndex.put("bar", 1);

        vector.set(0, stringToIndex.get("foo"));
        vector.set(1, stringToIndex.get("bar"));
        vector.set(2, stringToIndex.get("bar"));
        vector.set(3, stringToIndex.get("foo"));

        VectorSchemaRoot root = VectorSchemaRoot.of(dictionaryVector, vector);
        root.setRowCount(4);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ArrowFileWriter arrowWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());) {

          // batch 1
          arrowWriter.start();
          arrowWriter.writeBatch();
          dictionaryVector.reset();
          vector.reset();
          stringToIndex.clear();

          // batch 2
          // note the order is different for the strings
          dictionaryVector.set(0, "bar".getBytes(StandardCharsets.UTF_8));
          stringToIndex.put("bar", 0);
          dictionaryVector.set(1, "foo".getBytes(StandardCharsets.UTF_8));
          stringToIndex.put("foo", 1);

          vector.set(0, stringToIndex.get("bar"));
          vector.set(1, stringToIndex.get("bar"));
          vector.set(2, stringToIndex.get("foo"));
          vector.set(3, stringToIndex.get("foo"));

          root.setRowCount(4);

          arrowWriter.writeBatch();

          // batch 3
          dictionaryVector.reset();
          vector.reset();

          stringToIndex.clear();
          dictionaryVector.set(0, "baz".getBytes(StandardCharsets.UTF_8));
          stringToIndex.put("baz", 0);

          vector.set(0, stringToIndex.get("baz"));
          vector.set(1, stringToIndex.get("baz"));

          root.setRowCount(2);

          arrowWriter.writeBatch();
          arrowWriter.end();
        }
      }
    }
  }

  private void writeTwoDictionaries(File file) throws Exception {
    Map<String, Integer> stringToIndex1 = new HashMap<>();
    Map<String, Integer> stringToIndex2 = new HashMap<>();

    try (VarCharVector dictionaryVector1 = new VarCharVector("dict1", allocator);
         VarCharVector dictionaryVector2 = new VarCharVector("dict2", allocator)) {
      DictionaryEncoding dictionaryEncoding1 = new DictionaryEncoding(1, false, new ArrowType.Int(16, false));
      DictionaryEncoding dictionaryEncoding2 = new DictionaryEncoding(2, false, new ArrowType.Int(16, false));

      Dictionary dictionary1 = new Dictionary(dictionaryVector1, dictionaryEncoding1);
      Dictionary dictionary2 = new Dictionary(dictionaryVector2, dictionaryEncoding2);
      DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
      provider.put(dictionary1);
      provider.put(dictionary2);

      try (UInt2Vector vector1 = new UInt2Vector(
          "vector1",
          new FieldType(false, new ArrowType.Int(16, false), dictionaryEncoding1),
          allocator);
           UInt2Vector vector2 = new UInt2Vector(
               "vector2",
               new FieldType(false, new ArrowType.Int(16, false), dictionaryEncoding2),
               allocator)) {
        vector1.allocateNew(4);
        vector2.allocateNew(4);
        dictionaryVector1.allocateNew(2);
        dictionaryVector2.allocateNew(1);

        dictionaryVector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
        stringToIndex1.put("foo", 0);
        dictionaryVector1.set(1, "bar".getBytes(StandardCharsets.UTF_8));
        stringToIndex1.put("bar", 1);
        dictionaryVector2.set(0, "bur".getBytes(StandardCharsets.UTF_8));
        stringToIndex2.put("bur", 0);

        vector1.set(0, stringToIndex1.get("foo"));
        vector1.set(1, stringToIndex1.get("bar"));
        vector1.set(2, stringToIndex1.get("bar"));
        vector1.set(3, stringToIndex1.get("foo"));

        vector2.set(2, stringToIndex2.get("bur"));

        VectorSchemaRoot root = VectorSchemaRoot.of(dictionaryVector1, vector1, dictionaryVector2, vector2);
        root.setRowCount(4);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file);
             ArrowFileWriter arrowWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());) {

          // batch 1
          arrowWriter.start();
          arrowWriter.writeBatch();
          System.out.println("WRITE vector: " + vector2);
          System.out.println("WRITE dict: " + dictionaryVector2);
          dictionaryVector1.reset();
          dictionaryVector2.reset();
          vector1.reset();
          vector2.reset();
          stringToIndex1.clear();
          stringToIndex2.clear();

          // batch 2
          // note the order is different for the strings
          dictionaryVector1.set(0, "bar".getBytes(StandardCharsets.UTF_8));
          stringToIndex1.put("bar", 0);
          dictionaryVector1.set(1, "foo".getBytes(StandardCharsets.UTF_8));
          stringToIndex1.put("foo", 1);
          dictionaryVector2.set(0, "arg".getBytes(StandardCharsets.UTF_8));
          stringToIndex2.put("arg", 0);

          vector1.set(0, stringToIndex1.get("bar"));
          vector1.set(1, stringToIndex1.get("bar"));
          vector1.set(2, stringToIndex1.get("foo"));
          vector1.set(3, stringToIndex1.get("foo"));

          vector2.set(0, stringToIndex2.get("arg"));

          root.setRowCount(4);

          arrowWriter.writeBatch();

          // batch 3
          dictionaryVector1.reset();
          dictionaryVector2.reset();
          vector1.reset();
          vector2.reset();
          stringToIndex1.clear();
          stringToIndex2.clear();

          dictionaryVector1.set(0, "baz".getBytes(StandardCharsets.UTF_8));
          stringToIndex1.put("baz", 0);

          vector1.set(0, stringToIndex1.get("baz"));
          vector1.set(1, stringToIndex1.get("baz"));

          // nothing for vector 2

          root.setRowCount(2);

          arrowWriter.writeBatch();
          arrowWriter.end();
        }
      }
    }
  }
}
