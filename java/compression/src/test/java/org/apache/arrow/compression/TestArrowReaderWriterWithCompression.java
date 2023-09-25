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

package org.apache.arrow.compression;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestArrowReaderWriterWithCompression {
  private BufferAllocator allocator;
  private VarCharVector dictionaryVector1;
  private Dictionary dictionary1;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);

    dictionaryVector1 = (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector("D1", allocator, null);

    setVector(dictionaryVector1,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8));

    dictionary1 = new Dictionary(dictionaryVector1,
        new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
  }

  @After
  public void terminate() throws Exception {
    dictionaryVector1.close();
    allocator.close();
  }



  @Test
  public void testArrowFileZstdRoundTrip() throws Exception {
    // Prepare sample data
    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("col", FieldType.notNullable(new ArrowType.Utf8()), new ArrayList<>()));
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
    final int rowCount = 10;
    GenerateSampleData.generateTestData(root.getVector(0), rowCount);
    root.setRowCount(rowCount);

    // Write an in-memory compressed arrow file
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final ArrowFileWriter writer =
           new ArrowFileWriter(root, null, Channels.newChannel(out), new HashMap<>(),
             IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD, Optional.of(7))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    // Read the in-memory compressed arrow file with CommonsCompressionFactory provided
    try (ArrowFileReader reader =
           new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
             allocator, CommonsCompressionFactory.INSTANCE)) {
      Assert.assertEquals(1, reader.getRecordBlocks().size());
      Assert.assertTrue(reader.loadNextBatch());
      Assert.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assert.assertFalse(reader.loadNextBatch());
    }

    // Read the in-memory compressed arrow file without CompressionFactory provided
    try (ArrowFileReader reader =
           new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
             allocator, NoCompressionCodec.Factory.INSTANCE)) {
      Assert.assertEquals(1, reader.getRecordBlocks().size());

      Exception exception = Assert.assertThrows(IllegalArgumentException.class, () -> reader.loadNextBatch());
      String expectedMessage = "Please add arrow-compression module to use CommonsCompressionFactory for ZSTD";
      Assert.assertEquals(expectedMessage, exception.getMessage());
    }
  }

  @Test
  public void testArrowFileZstdRoundTripWithDictionary() throws Exception {
    // Prepare sample data
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);

    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("col", FieldType.notNullable(new ArrowType.Utf8()), new ArrayList<>()));

    VarCharVector vector1 = (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector("D1", allocator, null);
    vector1.allocateNewSafe();
    vector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector1.setValueCount(6);
    FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);
    vector1.close();
    fields.add(encodedVector1.getField());

    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
    final int rowCount = 10;
    GenerateSampleData.generateTestData(root.getVector(0), rowCount);
    root.setRowCount(rowCount);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final ArrowFileWriter writer =
           new ArrowFileWriter(root, provider, Channels.newChannel(out), new HashMap<>(),
             IpcOption.DEFAULT, CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD, Optional.of(7))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    // Read the in-memory compressed arrow file with CommonsCompressionFactory provided
    try (ArrowFileReader reader =
           new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
             allocator, CommonsCompressionFactory.INSTANCE)) {
      Assert.assertEquals(1, reader.getRecordBlocks().size());
      Assert.assertTrue(reader.loadNextBatch());
      Assert.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assert.assertFalse(reader.loadNextBatch());
    }
    // Read the in-memory compressed arrow file without CompressionFactory provided
    try (ArrowFileReader reader =
           new ArrowFileReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
             allocator, NoCompressionCodec.Factory.INSTANCE)) {
      Assert.assertEquals(1, reader.getRecordBlocks().size());
      Exception exception = Assert.assertThrows(IllegalArgumentException.class, () -> reader.loadNextBatch());
      String expectedMessage = "Please add arrow-compression module to use CommonsCompressionFactory for ZSTD";
      Assert.assertEquals(expectedMessage, exception.getMessage());
    }
  }

  @Test
  public void testArrowStreamZstdRoundTrip() throws Exception {
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);

    final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("col", FieldType.notNullable(new ArrowType.Utf8()), new ArrayList<>()));

    VarCharVector vector1 = (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector("D1", allocator, null);
    vector1.allocateNewSafe();
    vector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector1.setValueCount(6);
    FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);
    vector1.close();
    fields.add(encodedVector1.getField());

    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
    final int rowCount = 10;
    GenerateSampleData.generateTestData(root.getVector(0), rowCount);
    root.setRowCount(rowCount);

    // Write an in-memory compressed arrow file
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (final ArrowStreamWriter writer =
             new ArrowStreamWriter(root, provider, Channels.newChannel(out), IpcOption.DEFAULT,
                 CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD, Optional.of(7))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }

    // Read the in-memory compressed arrow file with CommonsCompressionFactory provided
    try (ArrowStreamReader reader =
             new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
                 allocator, CommonsCompressionFactory.INSTANCE)) {
      Assert.assertTrue(reader.loadNextBatch());
      Assert.assertTrue(root.equals(reader.getVectorSchemaRoot()));
      Assert.assertFalse(reader.loadNextBatch());
    }

    // Read the in-memory compressed arrow file without CompressionFactory provided
    try (ArrowStreamReader reader =
             new ArrowStreamReader(new ByteArrayReadableSeekableByteChannel(out.toByteArray()),
                 allocator, NoCompressionCodec.Factory.INSTANCE)) {

      Exception exception = Assert.assertThrows(IllegalArgumentException.class, () -> reader.loadNextBatch());
      String expectedMessage = "Please add arrow-compression module to use CommonsCompressionFactory for ZSTD";
      Assert.assertEquals(expectedMessage, exception.getMessage());
    }
  }

  public static void setVector(VarCharVector vector, byte[]... values) {
    final int length = values.length;
    vector.allocateNewSafe();
    for (int i = 0; i < length; i++) {
      if (values[i] != null) {
        vector.set(i, values[i]);
      }
    }
    vector.setValueCount(length);
  }

}
