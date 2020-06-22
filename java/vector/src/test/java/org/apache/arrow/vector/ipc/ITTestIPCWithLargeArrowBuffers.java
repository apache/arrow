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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for reading/writing {@link org.apache.arrow.vector.VectorSchemaRoot} with
 * large (more than 2GB) buffers by {@link ArrowReader} and {@link ArrowWriter}..
 * To run this test, please make sure there is at least 8GB free memory, and 8GB
 * free.disk space in the system.
 */
public class ITTestIPCWithLargeArrowBuffers {

  private static final Logger logger = LoggerFactory.getLogger(ITTestIPCWithLargeArrowBuffers.class);

  // 4GB buffer size
  static final long BUFFER_SIZE = 4 * 1024 * 1024 * 1024L;

  static final int DICTIONARY_VECTOR_SIZE = (int) (BUFFER_SIZE / BigIntVector.TYPE_WIDTH);

  static final int ENCODED_VECTOR_SIZE = (int) (BUFFER_SIZE / IntVector.TYPE_WIDTH);

  static final String FILE_NAME = "largeArrowData.data";

  static final long DICTIONARY_ID = 123L;

  static final ArrowType.Int ENCODED_VECTOR_TYPE = new ArrowType.Int(32, true);

  static final DictionaryEncoding DICTIONARY_ENCODING =
      new DictionaryEncoding(DICTIONARY_ID, false, ENCODED_VECTOR_TYPE);

  static final FieldType ENCODED_FIELD_TYPE =
      new FieldType(true, ENCODED_VECTOR_TYPE, DICTIONARY_ENCODING, null);

  static final Field ENCODED_VECTOR_FIELD = new Field("encoded vector", ENCODED_FIELD_TYPE, null);

  private void testWriteLargeArrowData(boolean streamMode) throws IOException {
    // simulate encoding big int as int
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         BigIntVector dictVector = new BigIntVector("dic vector", allocator);
         FileOutputStream out = new FileOutputStream(FILE_NAME);
         IntVector encodedVector = (IntVector) ENCODED_VECTOR_FIELD.createVector(allocator)) {

      // prepare dictionary provider.
      DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
      Dictionary dictionary = new Dictionary(dictVector, DICTIONARY_ENCODING);
      provider.put(dictionary);

      // populate the dictionary vector
      dictVector.allocateNew(DICTIONARY_VECTOR_SIZE);
      for (int i = 0; i < DICTIONARY_VECTOR_SIZE; i++) {
        dictVector.set(i, i);
      }
      dictVector.setValueCount(DICTIONARY_VECTOR_SIZE);
      assertTrue(dictVector.getDataBuffer().capacity() > Integer.MAX_VALUE);
      logger.trace("Populating dictionary vector finished");

      // populate the encoded vector
      encodedVector.allocateNew(ENCODED_VECTOR_SIZE);
      for (int i = 0; i < ENCODED_VECTOR_SIZE; i++) {
        encodedVector.set(i, i % DICTIONARY_VECTOR_SIZE);
      }
      encodedVector.setValueCount(ENCODED_VECTOR_SIZE);
      assertTrue(encodedVector.getDataBuffer().capacity() > Integer.MAX_VALUE);
      logger.trace("Populating encoded vector finished");

      // build vector schema root and write data.
      try (VectorSchemaRoot root =
               new VectorSchemaRoot(
                   Arrays.asList(ENCODED_VECTOR_FIELD), Arrays.asList(encodedVector), ENCODED_VECTOR_SIZE);
           ArrowWriter writer = streamMode ?
               new ArrowStreamWriter(root, provider, out) :
               new ArrowFileWriter(root, provider, out.getChannel())) {
        writer.start();
        writer.writeBatch();
        writer.end();
        logger.trace("Writing data finished");
      }
    }

    assertTrue(new File(FILE_NAME).exists());
  }

  private void testReadLargeArrowData(boolean streamMode) throws IOException {
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         FileInputStream in = new FileInputStream(FILE_NAME);
         ArrowReader reader = streamMode ?
             new ArrowStreamReader(in, allocator) :
             new ArrowFileReader(in.getChannel(), allocator)) {

      // verify schema
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(1, readSchema.getFields().size());
      assertEquals(ENCODED_VECTOR_FIELD, readSchema.getFields().get(0));
      logger.trace("Verifying schema finished");

      // verify vector schema root
      assertTrue(reader.loadNextBatch());
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      assertEquals(ENCODED_VECTOR_SIZE, root.getRowCount());
      assertEquals(1, root.getFieldVectors().size());
      assertTrue(root.getFieldVectors().get(0) instanceof IntVector);

      IntVector encodedVector = (IntVector) root.getVector(0);
      for (int i = 0; i < ENCODED_VECTOR_SIZE; i++) {
        assertEquals(i % DICTIONARY_VECTOR_SIZE, encodedVector.get(i));
      }
      logger.trace("Verifying encoded vector finished");

      // verify dictionary
      Map<Long, Dictionary> dictVectors = reader.getDictionaryVectors();
      assertEquals(1, dictVectors.size());
      Dictionary dictionary = dictVectors.get(DICTIONARY_ID);
      assertNotNull(dictionary);

      assertTrue(dictionary.getVector() instanceof BigIntVector);
      BigIntVector dictVector = (BigIntVector) dictionary.getVector();
      assertEquals(DICTIONARY_VECTOR_SIZE, dictVector.getValueCount());
      for (int i = 0; i < DICTIONARY_VECTOR_SIZE; i++) {
        assertEquals(i, dictVector.get(i));
      }
      logger.trace("Verifying dictionary vector finished");

      // ensure no more data available
      assertFalse(reader.loadNextBatch());
    } finally {
      File dataFile = new File(FILE_NAME);
      dataFile.delete();
      assertFalse(dataFile.exists());
    }
  }

  @Test
  public void testIPC() throws IOException {
    logger.trace("Start testing reading/writing large arrow stream data");
    testWriteLargeArrowData(true);
    testReadLargeArrowData(true);
    logger.trace("Finish testing reading/writing large arrow stream data");

    logger.trace("Start testing reading/writing large arrow file data");
    testWriteLargeArrowData(false);
    testReadLargeArrowData(false);
    logger.trace("Finish testing reading/writing large arrow file data");
  }
}
