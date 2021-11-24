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
import static java.util.Arrays.asList;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.vector.TestUtils.newVarCharVector;
import static org.apache.arrow.vector.TestUtils.newVector;
import static org.apache.arrow.vector.testing.ValueVectorDataPopulator.setVector;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.TestUtils;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestArrowReaderWriter {

  private BufferAllocator allocator;

  private VarCharVector dictionaryVector1;
  private VarCharVector dictionaryVector2;
  private VarCharVector dictionaryVector3;
  private StructVector dictionaryVector4;

  private Dictionary dictionary1;
  private Dictionary dictionary2;
  private Dictionary dictionary3;
  private Dictionary dictionary4;

  private Schema schema;
  private Schema encodedSchema;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);

    dictionaryVector1 = newVarCharVector("D1", allocator);
    setVector(dictionaryVector1,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8));

    dictionaryVector2 = newVarCharVector("D2", allocator);
    setVector(dictionaryVector2,
        "aa".getBytes(StandardCharsets.UTF_8),
        "bb".getBytes(StandardCharsets.UTF_8),
        "cc".getBytes(StandardCharsets.UTF_8));

    dictionaryVector3 = newVarCharVector("D3", allocator);
    setVector(dictionaryVector3,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8),
        "aa".getBytes(StandardCharsets.UTF_8),
        "bb".getBytes(StandardCharsets.UTF_8),
        "cc".getBytes(StandardCharsets.UTF_8));
    
    dictionaryVector4 = newVector(StructVector.class, "D4", MinorType.STRUCT, allocator);
    final Map<String, List<Integer>> dictionaryValues4 = new HashMap<>();
    dictionaryValues4.put("a", Arrays.asList(1, 2, 3));
    dictionaryValues4.put("b", Arrays.asList(4, 5, 6));
    setVector(dictionaryVector4, dictionaryValues4);

    dictionary1 = new Dictionary(dictionaryVector1,
        new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
    dictionary2 = new Dictionary(dictionaryVector2,
        new DictionaryEncoding(/*id=*/2L, /*ordered=*/false, /*indexType=*/null));
    dictionary3 = new Dictionary(dictionaryVector3,
        new DictionaryEncoding(/*id=*/1L, /*ordered=*/false, /*indexType=*/null));
    dictionary4 = new Dictionary(dictionaryVector4,
        new DictionaryEncoding(/*id=*/3L, /*ordered=*/false, /*indexType=*/null));
  }

  @After
  public void terminate() throws Exception {
    dictionaryVector1.close();
    dictionaryVector2.close();
    dictionaryVector3.close();
    dictionaryVector4.close();
    allocator.close();
  }

  ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  byte[] array(ArrowBuf buf) {
    byte[] bytes = new byte[checkedCastToInt(buf.readableBytes())];
    buf.readBytes(bytes);
    return bytes;
  }

  @Test
  public void test() throws IOException {
    Schema schema = new Schema(asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)),
        Collections.<Field>emptyList())));
    ArrowType type = schema.getFields().get(0).getType();
    FieldVector vector = TestUtils.newVector(FieldVector.class, "testField", type, allocator);
    vector.initializeChildrenFromFields(schema.getFields().get(0).getChildren());

    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    byte[] values = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), asList(vector), 16);
         ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out))) {
      ArrowBuf validityb = buf(validity);
      ArrowBuf valuesb = buf(values);
      ArrowRecordBatch batch = new ArrowRecordBatch(16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb));
      VectorLoader loader = new VectorLoader(root);
      loader.load(batch);
      writer.writeBatch();

      validityb.close();
      valuesb.close();
      batch.close();
    }

    byte[] byteArray = out.toByteArray();

    try (SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(byteArray));
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(schema, readSchema);
      // TODO: dictionaries
      List<ArrowBlock> recordBatches = reader.getRecordBlocks();
      assertEquals(1, recordBatches.size());
      reader.loadNextBatch();
      VectorUnloader unloader = new VectorUnloader(reader.getVectorSchemaRoot());
      ArrowRecordBatch recordBatch = unloader.getRecordBatch();
      List<ArrowFieldNode> nodes = recordBatch.getNodes();
      assertEquals(1, nodes.size());
      ArrowFieldNode node = nodes.get(0);
      assertEquals(16, node.getLength());
      assertEquals(8, node.getNullCount());
      List<ArrowBuf> buffers = recordBatch.getBuffers();
      assertEquals(2, buffers.size());
      assertArrayEquals(validity, array(buffers.get(0)));
      assertArrayEquals(values, array(buffers.get(1)));

      // Read just the header. This demonstrates being able to read without need to
      // deserialize the buffer.
      ByteBuffer headerBuffer = ByteBuffer.allocate(recordBatches.get(0).getMetadataLength());
      headerBuffer.put(byteArray, (int) recordBatches.get(0).getOffset(), headerBuffer.capacity());
      // new format prefix_size ==8
      headerBuffer.position(8);
      Message messageFB = Message.getRootAsMessage(headerBuffer);
      RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());
      assertEquals(2, recordBatchFB.buffersLength());
      assertEquals(1, recordBatchFB.nodesLength());
      FieldNode nodeFB = recordBatchFB.nodes(0);
      assertEquals(16, nodeFB.length());
      assertEquals(8, nodeFB.nullCount());

      recordBatch.close();
    }
  }

  @Test
  public void testWriteReadNullVector() throws IOException {

    int valueCount = 3;

    NullVector nullVector = new NullVector("vector");
    nullVector.setValueCount(valueCount);

    Schema schema = new Schema(asList(nullVector.getField()));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), asList(nullVector), valueCount);
        ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out))) {
      ArrowRecordBatch batch = new ArrowRecordBatch(valueCount,
          asList(new ArrowFieldNode(valueCount, 0)),
          Collections.emptyList());
      VectorLoader loader = new VectorLoader(root);
      loader.load(batch);
      writer.writeBatch();
    }

    byte[] byteArray = out.toByteArray();

    try (SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(byteArray));
        ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(schema, readSchema);
      List<ArrowBlock> recordBatches = reader.getRecordBlocks();
      assertEquals(1, recordBatches.size());

      assertTrue(reader.loadNextBatch());
      assertEquals(1, reader.getVectorSchemaRoot().getFieldVectors().size());

      NullVector readNullVector = (NullVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      assertEquals(valueCount, readNullVector.getValueCount());
    }
  }

  @Test
  public void testWriteReadWithDictionaries() throws IOException {
    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);

    VarCharVector vector1 = newVarCharVector("varchar1", allocator);
    vector1.allocateNewSafe();
    vector1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector1.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    vector1.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    vector1.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    vector1.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    vector1.setValueCount(6);
    FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);
    vector1.close();

    VarCharVector vector2 = newVarCharVector("varchar2", allocator);
    vector2.allocateNewSafe();
    vector2.set(0, "bar".getBytes(StandardCharsets.UTF_8));
    vector2.set(1, "baz".getBytes(StandardCharsets.UTF_8));
    vector2.set(2, "foo".getBytes(StandardCharsets.UTF_8));
    vector2.set(3, "foo".getBytes(StandardCharsets.UTF_8));
    vector2.set(4, "foo".getBytes(StandardCharsets.UTF_8));
    vector2.set(5, "bar".getBytes(StandardCharsets.UTF_8));
    vector2.setValueCount(6);
    FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary1);
    vector2.close();

    List<Field> fields = Arrays.asList(encodedVector1.getField(), encodedVector2.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(encodedVector1, encodedVector2);
    try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVector1.getValueCount());
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowFileWriter writer = new ArrowFileWriter(root, provider, newChannel(out));) {

      writer.start();
      writer.writeBatch();
      writer.end();

      try (SeekableReadChannel channel = new SeekableReadChannel(
          new ByteArrayReadableSeekableByteChannel(out.toByteArray()));
          ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        Schema readSchema = reader.getVectorSchemaRoot().getSchema();
        assertEquals(root.getSchema(), readSchema);
        assertEquals(1, reader.getDictionaryBlocks().size());
        assertEquals(1, reader.getRecordBlocks().size());

        reader.loadNextBatch();
        assertEquals(2, reader.getVectorSchemaRoot().getFieldVectors().size());
      }
    }
  }

  @Test
  public void testWriteReadWithStructDictionaries() throws IOException {
    DictionaryProvider.MapDictionaryProvider provider =
        new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary4);

    try (final StructVector vector =
        newVector(StructVector.class, "D4", MinorType.STRUCT, allocator)) {
      final Map<String, List<Integer>> values = new HashMap<>();
      // Index: 0, 2, 1, 2, 1, 0, 0
      values.put("a", Arrays.asList(1, 3, 2, 3, 2, 1, 1));
      values.put("b", Arrays.asList(4, 6, 5, 6, 5, 4, 4));
      setVector(vector, values);
      FieldVector encodedVector = (FieldVector) DictionaryEncoder.encode(vector, dictionary4);

      List<Field> fields = Arrays.asList(encodedVector.getField());
      List<FieldVector> vectors = Collections2.asImmutableList(encodedVector);
      try (
          VectorSchemaRoot root =
              new VectorSchemaRoot(fields, vectors, encodedVector.getValueCount());
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ArrowFileWriter writer = new ArrowFileWriter(root, provider, newChannel(out));) {

        writer.start();
        writer.writeBatch();
        writer.end();

        try (
            SeekableReadChannel channel = new SeekableReadChannel(
                new ByteArrayReadableSeekableByteChannel(out.toByteArray()));
            ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
          final VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
          final Schema readSchema = readRoot.getSchema();
          assertEquals(root.getSchema(), readSchema);
          assertEquals(1, reader.getDictionaryBlocks().size());
          assertEquals(1, reader.getRecordBlocks().size());

          reader.loadNextBatch();
          assertEquals(1, readRoot.getFieldVectors().size());
          assertEquals(1, reader.getDictionaryVectors().size());

          // Read the encoded vector and check it
          final FieldVector readEncoded = readRoot.getVector(0);
          assertEquals(encodedVector.getValueCount(), readEncoded.getValueCount());
          assertTrue(new RangeEqualsVisitor(encodedVector, readEncoded)
              .rangeEquals(new Range(0, 0, encodedVector.getValueCount())));

          // Read the dictionary
          final Map<Long, Dictionary> readDictionaryMap = reader.getDictionaryVectors();
          final Dictionary readDictionary =
              readDictionaryMap.get(readEncoded.getField().getDictionary().getId());
          assertNotNull(readDictionary);

          // Assert the dictionary vector is correct
          final FieldVector readDictionaryVector = readDictionary.getVector();
          assertEquals(dictionaryVector4.getValueCount(), readDictionaryVector.getValueCount());
          final BiFunction<ValueVector, ValueVector, Boolean> typeComparatorIgnoreName =
              (v1, v2) -> new TypeEqualsVisitor(v1, false, true).equals(v2);
          assertTrue("Dictionary vectors are not equal",
              new RangeEqualsVisitor(dictionaryVector4, readDictionaryVector,
                  typeComparatorIgnoreName)
                      .rangeEquals(new Range(0, 0, dictionaryVector4.getValueCount())));

          // Assert the decoded vector is correct
          try (final ValueVector readVector =
              DictionaryEncoder.decode(readEncoded, readDictionary)) {
            assertEquals(vector.getValueCount(), readVector.getValueCount());
            assertTrue("Decoded vectors are not equal",
                new RangeEqualsVisitor(vector, readVector, typeComparatorIgnoreName)
                    .rangeEquals(new Range(0, 0, vector.getValueCount())));
          }
        }
      }
    }
  }

  @Test
  public void testEmptyStreamInFileIPC() throws IOException {

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);

    VarCharVector vector = newVarCharVector("varchar", allocator);
    vector.allocateNewSafe();
    vector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    vector.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    vector.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    vector.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    vector.setValueCount(6);

    FieldVector encodedVector1A = (FieldVector) DictionaryEncoder.encode(vector, dictionary1);
    vector.close();

    List<Field> fields = Arrays.asList(encodedVector1A.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(encodedVector1A);

    try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVector1A.getValueCount());
         ByteArrayOutputStream out = new ByteArrayOutputStream();
         ArrowFileWriter writer = new ArrowFileWriter(root, provider, newChannel(out))) {

      writer.start();
      writer.end();

      try (SeekableReadChannel channel = new SeekableReadChannel(
           new ByteArrayReadableSeekableByteChannel(out.toByteArray()));
           ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        Schema readSchema = reader.getVectorSchemaRoot().getSchema();
        assertEquals(root.getSchema(), readSchema);
        assertEquals(1, reader.getDictionaryVectors().size());
        assertEquals(0, reader.getDictionaryBlocks().size());
        assertEquals(0, reader.getRecordBlocks().size());
      }
    }

  }

  @Test
  public void testEmptyStreamInStreamingIPC() throws IOException {

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);

    VarCharVector vector = newVarCharVector("varchar", allocator);
    vector.allocateNewSafe();
    vector.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vector.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    vector.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    vector.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    vector.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    vector.setValueCount(6);

    FieldVector encodedVector = (FieldVector) DictionaryEncoder.encode(vector, dictionary1);
    vector.close();

    List<Field> fields = Arrays.asList(encodedVector.getField());
    try (VectorSchemaRoot root =
        new VectorSchemaRoot(fields, Arrays.asList(encodedVector), encodedVector.getValueCount());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, newChannel(out))) {

      writer.start();
      writer.end();


      try (ArrowStreamReader reader = new ArrowStreamReader(
          new ByteArrayReadableSeekableByteChannel(out.toByteArray()), allocator)) {
        Schema readSchema = reader.getVectorSchemaRoot().getSchema();
        assertEquals(root.getSchema(), readSchema);
        assertEquals(1, reader.getDictionaryVectors().size());
        assertFalse(reader.loadNextBatch());
      }
    }

  }

  @Test
  public void testDictionaryReplacement() throws Exception {
    VarCharVector vector1 = newVarCharVector("varchar1", allocator);
    setVector(vector1,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8));

    FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);

    VarCharVector vector2 = newVarCharVector("varchar2", allocator);
    setVector(vector2,
        "foo".getBytes(StandardCharsets.UTF_8),
        "foo".getBytes(StandardCharsets.UTF_8),
        "foo".getBytes(StandardCharsets.UTF_8),
        "foo".getBytes(StandardCharsets.UTF_8));

    FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary1);

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);
    List<Field> schemaFields = new ArrayList<>();
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVector1.getField(), provider, new HashSet<>()));
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVector2.getField(), provider, new HashSet<>()));
    Schema schema = new Schema(schemaFields);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(newChannel(outStream));

    // write schema
    MessageSerializer.serialize(out, schema);

    List<AutoCloseable> closeableList = new ArrayList<>();

    // write non-delta dictionary with id=1
    serializeDictionaryBatch(out, dictionary3, false, closeableList);

    // write non-delta dictionary with id=1
    serializeDictionaryBatch(out, dictionary1, false, closeableList);

    // write recordBatch2
    serializeRecordBatch(out, Arrays.asList(encodedVector1, encodedVector2), closeableList);

    // write eos
    out.writeIntLittleEndian(0);

    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayReadableSeekableByteChannel(outStream.toByteArray()), allocator)) {
      assertEquals(1, reader.getDictionaryVectors().size());
      assertTrue(reader.loadNextBatch());
      FieldVector dictionaryVector = reader.getDictionaryVectors().get(1L).getVector();
      // make sure the delta dictionary is concatenated.
      assertTrue(VectorEqualsVisitor.vectorEquals(dictionaryVector, dictionaryVector1, null));
      assertFalse(reader.loadNextBatch());
    }

    vector1.close();
    vector2.close();
    AutoCloseables.close(closeableList);
  }

  @Test
  public void testDeltaDictionary() throws Exception {
    VarCharVector vector1 = newVarCharVector("varchar1", allocator);
    setVector(vector1,
        "foo".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8),
        "baz".getBytes(StandardCharsets.UTF_8),
        "bar".getBytes(StandardCharsets.UTF_8));

    FieldVector encodedVector1 = (FieldVector) DictionaryEncoder.encode(vector1, dictionary1);

    VarCharVector vector2 = newVarCharVector("varchar2", allocator);
    setVector(vector2,
        "foo".getBytes(StandardCharsets.UTF_8),
        "aa".getBytes(StandardCharsets.UTF_8),
        "bb".getBytes(StandardCharsets.UTF_8),
        "cc".getBytes(StandardCharsets.UTF_8));

    FieldVector encodedVector2 = (FieldVector) DictionaryEncoder.encode(vector2, dictionary3);

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);
    provider.put(dictionary3);
    List<Field> schemaFields = new ArrayList<>();
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVector1.getField(), provider, new HashSet<>()));
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVector2.getField(), provider, new HashSet<>()));
    Schema schema = new Schema(schemaFields);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(newChannel(outStream));

    // write schema
    MessageSerializer.serialize(out, schema);

    List<AutoCloseable> closeableList = new ArrayList<>();

    // write non-delta dictionary with id=1
    serializeDictionaryBatch(out, dictionary1, false, closeableList);

    // write delta dictionary with id=1
    Dictionary deltaDictionary =
        new Dictionary(dictionaryVector2, new DictionaryEncoding(1L, false, null));
    serializeDictionaryBatch(out, deltaDictionary, true, closeableList);
    deltaDictionary.getVector().close();

    // write recordBatch2
    serializeRecordBatch(out, Arrays.asList(encodedVector1, encodedVector2), closeableList);

    // write eos
    out.writeIntLittleEndian(0);

    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayReadableSeekableByteChannel(outStream.toByteArray()), allocator)) {
      assertEquals(1, reader.getDictionaryVectors().size());
      assertTrue(reader.loadNextBatch());
      FieldVector dictionaryVector = reader.getDictionaryVectors().get(1L).getVector();
      // make sure the delta dictionary is concatenated.
      assertTrue(VectorEqualsVisitor.vectorEquals(dictionaryVector, dictionaryVector3, null));
      assertFalse(reader.loadNextBatch());
    }

    vector1.close();
    vector2.close();
    AutoCloseables.close(closeableList);

  }

  private void serializeDictionaryBatch(
      WriteChannel out,
      Dictionary dictionary,
      boolean isDelta,
      List<AutoCloseable> closeables) throws IOException {

    FieldVector dictVector = dictionary.getVector();
    VectorSchemaRoot root = new VectorSchemaRoot(
        Collections.singletonList(dictVector.getField()),
        Collections.singletonList(dictVector),
        dictVector.getValueCount());
    ArrowDictionaryBatch batch =
        new ArrowDictionaryBatch(dictionary.getEncoding().getId(), new VectorUnloader(root).getRecordBatch(), isDelta);
    MessageSerializer.serialize(out, batch);
    closeables.add(batch);
    closeables.add(root);
  }

  private void serializeRecordBatch(
      WriteChannel out,
      List<FieldVector> vectors,
      List<AutoCloseable> closeables) throws IOException {

    List<Field> fields = vectors.stream().map(v -> v.getField()).collect(Collectors.toList());
    VectorSchemaRoot root = new VectorSchemaRoot(
        fields,
        vectors,
        vectors.get(0).getValueCount());
    VectorUnloader unloader = new VectorUnloader(root);
    ArrowRecordBatch batch = unloader.getRecordBatch();
    MessageSerializer.serialize(out, batch);
    closeables.add(batch);
    closeables.add(root);
  }

  @Test
  public void testReadInterleavedData() throws IOException {
    List<ArrowRecordBatch> batches = createRecordBatches();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(newChannel(outStream));

    // write schema
    MessageSerializer.serialize(out, schema);

    // write dictionary1
    FieldVector dictVector1 = dictionary1.getVector();
    VectorSchemaRoot dictRoot1 = new VectorSchemaRoot(
        Collections.singletonList(dictVector1.getField()),
        Collections.singletonList(dictVector1),
        dictVector1.getValueCount());
    ArrowDictionaryBatch dictionaryBatch1 =
        new ArrowDictionaryBatch(1, new VectorUnloader(dictRoot1).getRecordBatch());
    MessageSerializer.serialize(out, dictionaryBatch1);
    dictionaryBatch1.close();
    dictRoot1.close();

    // write recordBatch1
    MessageSerializer.serialize(out, batches.get(0));

    // write dictionary2
    FieldVector dictVector2 = dictionary2.getVector();
    VectorSchemaRoot dictRoot2 = new VectorSchemaRoot(
        Collections.singletonList(dictVector2.getField()),
        Collections.singletonList(dictVector2),
        dictVector2.getValueCount());
    ArrowDictionaryBatch dictionaryBatch2 =
        new ArrowDictionaryBatch(2, new VectorUnloader(dictRoot2).getRecordBatch());
    MessageSerializer.serialize(out, dictionaryBatch2);
    dictionaryBatch2.close();
    dictRoot2.close();

    // write recordBatch1
    MessageSerializer.serialize(out, batches.get(1));

    // write eos
    out.writeIntLittleEndian(0);

    try (ArrowStreamReader reader = new ArrowStreamReader(
        new ByteArrayReadableSeekableByteChannel(outStream.toByteArray()), allocator)) {
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(encodedSchema, readSchema);
      assertEquals(2, reader.getDictionaryVectors().size());
      assertTrue(reader.loadNextBatch());
      assertTrue(reader.loadNextBatch());
      assertFalse(reader.loadNextBatch());
    }

    batches.forEach(batch -> batch.close());
  }

  private List<ArrowRecordBatch> createRecordBatches() {
    List<ArrowRecordBatch> batches = new ArrayList<>();

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary1);
    provider.put(dictionary2);

    VarCharVector vectorA1 = newVarCharVector("varcharA1", allocator);
    vectorA1.allocateNewSafe();
    vectorA1.set(0, "foo".getBytes(StandardCharsets.UTF_8));
    vectorA1.set(1, "bar".getBytes(StandardCharsets.UTF_8));
    vectorA1.set(3, "baz".getBytes(StandardCharsets.UTF_8));
    vectorA1.set(4, "bar".getBytes(StandardCharsets.UTF_8));
    vectorA1.set(5, "baz".getBytes(StandardCharsets.UTF_8));
    vectorA1.setValueCount(6);

    VarCharVector vectorA2 = newVarCharVector("varcharA2", allocator);
    vectorA2.setValueCount(6);
    FieldVector encodedVectorA1 = (FieldVector) DictionaryEncoder.encode(vectorA1, dictionary1);
    vectorA1.close();
    FieldVector encodedVectorA2 = (FieldVector) DictionaryEncoder.encode(vectorA1, dictionary2);
    vectorA2.close();

    List<Field> fields = Arrays.asList(encodedVectorA1.getField(), encodedVectorA2.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(encodedVectorA1, encodedVectorA2);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, encodedVectorA1.getValueCount());
    VectorUnloader unloader = new VectorUnloader(root);
    batches.add(unloader.getRecordBatch());
    root.close();

    VarCharVector vectorB1 = newVarCharVector("varcharB1", allocator);
    vectorB1.setValueCount(6);

    VarCharVector vectorB2 = newVarCharVector("varcharB2", allocator);
    vectorB2.allocateNew();
    vectorB2.setValueCount(6);
    vectorB2.set(0, "aa".getBytes(StandardCharsets.UTF_8));
    vectorB2.set(1, "aa".getBytes(StandardCharsets.UTF_8));
    vectorB2.set(3, "bb".getBytes(StandardCharsets.UTF_8));
    vectorB2.set(4, "bb".getBytes(StandardCharsets.UTF_8));
    vectorB2.set(5, "cc".getBytes(StandardCharsets.UTF_8));
    vectorB2.setValueCount(6);
    FieldVector encodedVectorB1 = (FieldVector) DictionaryEncoder.encode(vectorB1, dictionary1);
    vectorB1.close();
    FieldVector encodedVectorB2 = (FieldVector) DictionaryEncoder.encode(vectorB2, dictionary2);
    vectorB2.close();

    List<Field> fieldsB = Arrays.asList(encodedVectorB1.getField(), encodedVectorB2.getField());
    List<FieldVector> vectorsB = Collections2.asImmutableList(encodedVectorB1, encodedVectorB2);
    VectorSchemaRoot rootB = new VectorSchemaRoot(fieldsB, vectorsB, 6);
    VectorUnloader unloaderB = new VectorUnloader(rootB);
    batches.add(unloaderB.getRecordBatch());
    rootB.close();

    List<Field> schemaFields = new ArrayList<>();
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVectorA1.getField(), provider, new HashSet<>()));
    schemaFields.add(DictionaryUtility.toMessageFormat(encodedVectorA2.getField(), provider, new HashSet<>()));
    schema = new Schema(schemaFields);

    encodedSchema = new Schema(Arrays.asList(encodedVectorA1.getField(), encodedVectorA2.getField()));

    return batches;
  }

  @Test
  public void testLegacyIpcBackwardsCompatibility() throws Exception {
    Schema schema = new Schema(asList(Field.nullable("field", new ArrowType.Int(32, true))));
    IntVector vector = new IntVector("vector", allocator);
    final int valueCount = 2;
    vector.setValueCount(valueCount);
    vector.setSafe(0, 1);
    vector.setSafe(1, 2);
    ArrowRecordBatch batch = new ArrowRecordBatch(valueCount, asList(new ArrowFieldNode(valueCount, 0)),
        asList(vector.getValidityBuffer(), vector.getDataBuffer()));

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    WriteChannel out = new WriteChannel(newChannel(outStream));

    // write legacy ipc format
    IpcOption option = new IpcOption(true, MetadataVersion.DEFAULT);
    MessageSerializer.serialize(out, schema, option);
    MessageSerializer.serialize(out, batch);

    ReadChannel in = new ReadChannel(newChannel(new ByteArrayInputStream(outStream.toByteArray())));
    Schema readSchema = MessageSerializer.deserializeSchema(in);
    assertEquals(schema, readSchema);
    ArrowRecordBatch readBatch = MessageSerializer.deserializeRecordBatch(in, allocator);
    assertEquals(batch.getLength(), readBatch.getLength());
    assertEquals(batch.computeBodyLength(), readBatch.computeBodyLength());
    readBatch.close();

    // write ipc format with continuation
    option = IpcOption.DEFAULT;
    MessageSerializer.serialize(out, schema, option);
    MessageSerializer.serialize(out, batch);

    ReadChannel in2 = new ReadChannel(newChannel(new ByteArrayInputStream(outStream.toByteArray())));
    Schema readSchema2 = MessageSerializer.deserializeSchema(in2);
    assertEquals(schema, readSchema2);
    ArrowRecordBatch readBatch2 = MessageSerializer.deserializeRecordBatch(in2, allocator);
    assertEquals(batch.getLength(), readBatch2.getLength());
    assertEquals(batch.computeBodyLength(), readBatch2.computeBodyLength());
    readBatch2.close();

    batch.close();
    vector.close();
  }

  @Test
  public void testChannelReadFully() throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
    buf.putInt(200);
    buf.rewind();

    try (ReadChannel channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(buf.array())));
         ArrowBuf arrBuf = allocator.buffer(8)) {
      arrBuf.setInt(0, 100);
      arrBuf.writerIndex(4);
      assertEquals(4, arrBuf.writerIndex());

      long n = channel.readFully(arrBuf, 4);
      assertEquals(4, n);
      assertEquals(8, arrBuf.writerIndex());

      assertEquals(100, arrBuf.getInt(0));
      assertEquals(200, arrBuf.getInt(4));
    }
  }

  @Test
  public void testChannelReadFullyEos() throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
    buf.putInt(10);
    buf.rewind();

    try (ReadChannel channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(buf.array())));
         ArrowBuf arrBuf = allocator.buffer(8)) {
      int n = channel.readFully(arrBuf.nioBuffer(0, 8));
      assertEquals(4, n);

      // the input has only 4 bytes, so the number of bytes read should be 4
      assertEquals(4, channel.bytesRead());

      // the first 4 bytes have been read successfully.
      assertEquals(10, arrBuf.getInt(0));
    }
  }

  @Test
  public void testCustomMetaData() throws IOException {

    VarCharVector vector = newVarCharVector("varchar1", allocator);

    List<Field> fields = Arrays.asList(vector.getField());
    List<FieldVector> vectors = Collections2.asImmutableList(vector);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("key1", "value1");
    metadata.put("key2", "value2");
    try (VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors, vector.getValueCount());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out), metadata);) {

      writer.start();
      writer.end();

      try (SeekableReadChannel channel = new SeekableReadChannel(
          new ByteArrayReadableSeekableByteChannel(out.toByteArray()));
          ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
        reader.getVectorSchemaRoot();

        Map<String, String> readMeta = reader.getMetaData();
        assertEquals(2, readMeta.size());
        assertEquals("value1", readMeta.get("key1"));
        assertEquals("value2", readMeta.get("key2"));
      }
    }
  }
}
