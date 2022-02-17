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

package org.apache.arrow.c;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Collections;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DictionaryTest {
  private RootAllocator allocator = null;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  void roundtrip(FieldVector vector, DictionaryProvider provider, Class<?> clazz) {
    // Consumer allocates empty structures
    try (ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray consumerArrowArray = ArrowArray.allocateNew(allocator)) {

      // Producer creates structures from existing memory pointers
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(consumerArrowSchema.memoryAddress());
          ArrowArray arrowArray = ArrowArray.wrap(consumerArrowArray.memoryAddress())) {
        // Producer exports vector into the C Data Interface structures
        Data.exportVector(allocator, vector, provider, arrowArray, arrowSchema);
      }

      // Consumer imports vector
      try (CDataDictionaryProvider cDictionaryProvider = new CDataDictionaryProvider();
          FieldVector imported = Data.importVector(allocator, consumerArrowArray, consumerArrowSchema,
              cDictionaryProvider);) {
        assertTrue(clazz.isInstance(imported), String.format("expected %s but was %s", clazz, imported.getClass()));
        assertTrue(VectorEqualsVisitor.vectorEquals(vector, imported), "vectors are not equivalent");
        for (long id : cDictionaryProvider.getDictionaryIds()) {
          ValueVector exportedDictionaryVector = provider.lookup(id).getVector();
          ValueVector importedDictionaryVector = cDictionaryProvider.lookup(id).getVector();
          assertTrue(VectorEqualsVisitor.vectorEquals(exportedDictionaryVector, importedDictionaryVector),
              String.format("Dictionary vectors for ID %d are not equivalent", id));
        }
      }
    }
  }

  @Test
  public void testWithDictionary() throws Exception {
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    // create dictionary and provider
    final VarCharVector dictVector = new VarCharVector("dict", allocator);
    dictVector.allocateNewSafe();
    dictVector.setSafe(0, "aa".getBytes());
    dictVector.setSafe(1, "bb".getBytes());
    dictVector.setSafe(2, "cc".getBytes());
    dictVector.setValueCount(3);

    Dictionary dictionary = new Dictionary(dictVector, new DictionaryEncoding(1L, false, /* indexType= */null));
    provider.put(dictionary);

    // create vector and encode it
    final VarCharVector vector = new VarCharVector("vector", allocator);
    vector.allocateNewSafe();
    vector.setSafe(0, "bb".getBytes());
    vector.setSafe(1, "bb".getBytes());
    vector.setSafe(2, "cc".getBytes());
    vector.setSafe(3, "aa".getBytes());
    vector.setValueCount(4);

    // get the encoded vector
    IntVector encodedVector = (IntVector) DictionaryEncoder.encode(vector, dictionary);

    // Perform roundtrip using C Data Interface
    roundtrip(encodedVector, provider, IntVector.class);

    // Close all
    AutoCloseables.close((AutoCloseable) vector, encodedVector, dictVector);
  }

  @Test
  public void testRoundtripMultipleBatches() throws IOException {
    try (ArrowStreamReader reader = createMultiBatchReader();
        ArrowSchema consumerArrowSchema = ArrowSchema.allocateNew(allocator)) {
      // Load first batch
      reader.loadNextBatch();
      // Producer fills consumer schema stucture
      Data.exportSchema(allocator, reader.getVectorSchemaRoot().getSchema(), reader, consumerArrowSchema);
      // Consumer loads it as an empty vector schema root
      try (CDataDictionaryProvider consumerDictionaryProvider = new CDataDictionaryProvider();
          VectorSchemaRoot consumerRoot = Data.importVectorSchemaRoot(allocator, consumerArrowSchema,
              consumerDictionaryProvider)) {
        do {
          try (ArrowArray consumerArray = ArrowArray.allocateNew(allocator)) {
            // Producer exports next data
            Data.exportVectorSchemaRoot(allocator, reader.getVectorSchemaRoot(), reader, consumerArray);
            // Consumer loads next data
            Data.importIntoVectorSchemaRoot(allocator, consumerArray, consumerRoot, consumerDictionaryProvider);

            // Roundtrip validation
            assertTrue(consumerRoot.equals(reader.getVectorSchemaRoot()), "vector schema roots are not equivalent");
            for (long id : consumerDictionaryProvider.getDictionaryIds()) {
              ValueVector exportedDictionaryVector = reader.lookup(id).getVector();
              ValueVector importedDictionaryVector = consumerDictionaryProvider.lookup(id).getVector();
              assertTrue(VectorEqualsVisitor.vectorEquals(exportedDictionaryVector, importedDictionaryVector),
                  String.format("Dictionary vectors for ID %d are not equivalent", id));
            }
          }
        }
        while (reader.loadNextBatch());
      }
    }
  }

  private ArrowStreamReader createMultiBatchReader() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (final VarCharVector dictVector = new VarCharVector("dict", allocator);
        IntVector vector = new IntVector("foo", allocator)) {
      // create dictionary and provider
      DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
      dictVector.allocateNewSafe();
      dictVector.setSafe(0, "aa".getBytes());
      dictVector.setSafe(1, "bb".getBytes());
      dictVector.setSafe(2, "cc".getBytes());
      dictVector.setSafe(3, "dd".getBytes());
      dictVector.setSafe(4, "ee".getBytes());
      dictVector.setValueCount(5);
      Dictionary dictionary = new Dictionary(dictVector, new DictionaryEncoding(1L, false, /* indexType= */null));
      provider.put(dictionary);

      Schema schema = new Schema(Collections.singletonList(vector.getField()));
      try (
          VectorSchemaRoot root = new VectorSchemaRoot(schema, Collections.singletonList(vector),
              vector.getValueCount());
          ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(os));) {

        writer.start();

        // Batch 1
        vector.setNull(0);
        vector.setSafe(1, 1);
        vector.setSafe(2, 2);
        vector.setNull(3);
        vector.setSafe(4, 1);
        vector.setValueCount(5);
        root.setRowCount(5);
        writer.writeBatch();

        // Batch 2
        vector.setNull(0);
        vector.setSafe(1, 1);
        vector.setSafe(2, 2);
        vector.setValueCount(3);
        root.setRowCount(3);
        writer.writeBatch();

        // Batch 3
        vector.setSafe(0, 0);
        vector.setSafe(1, 1);
        vector.setSafe(2, 2);
        vector.setSafe(3, 3);
        vector.setSafe(4, 4);
        vector.setValueCount(5);
        root.setRowCount(5);
        writer.writeBatch();

        writer.end();
      }
    }

    ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());
    return new ArrowStreamReader(in, allocator);
  }

}
