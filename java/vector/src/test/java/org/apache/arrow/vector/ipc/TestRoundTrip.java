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

import static org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestRoundTrip extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestRoundTrip.class);
  private static BufferAllocator allocator;
  private final String name;
  private final IpcOption writeOption;

  public TestRoundTrip(String name, IpcOption writeOption) {
    this.name = name;
    this.writeOption = writeOption;
  }

  @Parameterized.Parameters(name = "options = {0}")
  public static Collection<Object[]> getWriteOption() {
    final IpcOption legacy = new IpcOption(true, MetadataVersion.V4);
    final IpcOption version4 = new IpcOption(false, MetadataVersion.V4);
    return Arrays.asList(
        new Object[] {"V4Legacy", legacy},
        new Object[] {"V4", version4},
        new Object[] {"V5", IpcOption.DEFAULT}
    );
  }

  @BeforeClass
  public static void setUpClass() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @AfterClass
  public static void tearDownClass() {
    allocator.close();
  }

  @Test
  public void testStruct() throws Exception {
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeData(COUNT, parent);
      roundTrip(
          new VectorSchemaRoot(parent.getChild("root")),
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {COUNT}, this::validateContent),
          validateStreamBatches(new int[] {COUNT}, this::validateContent));
    }
  }

  @Test
  public void testComplex() throws Exception {
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeComplexData(COUNT, parent);
      roundTrip(
          new VectorSchemaRoot(parent.getChild("root")),
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {COUNT}, this::validateComplexContent),
          validateStreamBatches(new int[] {COUNT}, this::validateComplexContent));
    }
  }

  @Test
  public void testMultipleRecordBatches() throws Exception {
    int[] counts = {10, 5};
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeData(counts[0], parent);
      roundTrip(
          new VectorSchemaRoot(parent.getChild("root")),
          /* dictionaryProvider */null,
          (root, writer) -> {
            writer.start();
            parent.allocateNew();
            writeData(counts[0], parent);
            root.setRowCount(counts[0]);
            writer.writeBatch();

            parent.allocateNew();
            // if we write the same data we don't catch that the metadata is stored in the wrong order.
            writeData(counts[1], parent);
            root.setRowCount(counts[1]);
            writer.writeBatch();

            writer.end();
          },
          validateFileBatches(counts, this::validateContent),
          validateStreamBatches(counts, this::validateContent));
    }
  }

  @Test
  public void testUnionV4() throws Exception {
    Assume.assumeTrue(writeOption.metadataVersion == MetadataVersion.V4);
    final File temp = File.createTempFile("arrow-test-" + name + "-", ".arrow");
    temp.deleteOnExit();
    final ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();

    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeUnionData(COUNT, parent);
      final VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> {
        try (final FileOutputStream fileStream = new FileOutputStream(temp)) {
          new ArrowFileWriter(root, null, fileStream.getChannel(), writeOption);
          new ArrowStreamWriter(root, null, Channels.newChannel(memoryStream), writeOption);
        }
      });
      assertTrue(e.getMessage(), e.getMessage().contains("Cannot write union with V4 metadata"));
      e = assertThrows(IllegalArgumentException.class, () -> {
        new ArrowStreamWriter(root, null, Channels.newChannel(memoryStream), writeOption);
      });
      assertTrue(e.getMessage(), e.getMessage().contains("Cannot write union with V4 metadata"));
    }
  }

  @Test
  public void testUnionV5() throws Exception {
    Assume.assumeTrue(writeOption.metadataVersion == MetadataVersion.V5);
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeUnionData(COUNT, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateUnionData(COUNT, root);
      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {COUNT}, this::validateUnionData),
          validateStreamBatches(new int[] {COUNT}, this::validateUnionData));
    }
  }

  @Test
  public void testTiny() throws Exception {
    try (final VectorSchemaRoot root = VectorSchemaRoot.create(MessageSerializerTest.testSchema(), allocator)) {
      root.getFieldVectors().get(0).allocateNew();
      int count = 16;
      TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
      for (int i = 0; i < count; i++) {
        vector.set(i, i < 8 ? 1 : 0, (byte) (i + 1));
      }
      vector.setValueCount(count);
      root.setRowCount(count);

      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {count}, this::validateTinyData),
          validateStreamBatches(new int[] {count}, this::validateTinyData));
    }
  }

  private void validateTinyData(int count, VectorSchemaRoot root) {
    assertEquals(count, root.getRowCount());
    TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
    for (int i = 0; i < count; i++) {
      if (i < 8) {
        assertEquals((byte) (i + 1), vector.get(i));
      } else {
        assertTrue(vector.isNull(i));
      }
    }
  }

  @Test
  public void testMetadata() throws Exception {
    List<Field> childFields = new ArrayList<>();
    childFields.add(new Field("varchar-child", new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata(1)), null));
    childFields.add(new Field("float-child",
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null, metadata(2)), null));
    childFields.add(new Field("int-child", new FieldType(false, new ArrowType.Int(32, true), null, metadata(3)), null));
    childFields.add(new Field("list-child", new FieldType(true, ArrowType.List.INSTANCE, null, metadata(4)),
        Collections2.asImmutableList(new Field("l1", FieldType.nullable(new ArrowType.Int(16, true)), null))));
    Field field = new Field("meta", new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata(0)), childFields);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("s1", "v1");
    metadata.put("s2", "v2");
    Schema originalSchema = new Schema(Collections2.asImmutableList(field), metadata);
    assertEquals(metadata, originalSchema.getCustomMetadata());

    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector vector = (StructVector) field.createVector(originalVectorAllocator)) {
      vector.allocateNewSafe();
      vector.setValueCount(0);

      List<FieldVector> vectors = Collections2.asImmutableList(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(originalSchema, vectors, 0);

      BiConsumer<Integer, VectorSchemaRoot> validate = (count, readRoot) -> {
        Schema schema = readRoot.getSchema();
        assertEquals(originalSchema, schema);
        assertEquals(originalSchema.getCustomMetadata(), schema.getCustomMetadata());
        Field top = schema.getFields().get(0);
        assertEquals(metadata(0), top.getMetadata());
        for (int i = 0; i < 4; i++) {
          assertEquals(metadata(i + 1), top.getChildren().get(i).getMetadata());
        }
      };
      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {0}, validate),
          validateStreamBatches(new int[] {0}, validate));
    }
  }

  private Map<String, String> metadata(int i) {
    Map<String, String> map = new HashMap<>();
    map.put("k_" + i, "v_" + i);
    map.put("k2_" + i, "v2_" + i);
    return Collections.unmodifiableMap(map);
  }

  @Test
  public void testFlatDictionary() throws Exception {
    AtomicInteger numDictionaryBlocksWritten = new AtomicInteger();
    MapDictionaryProvider provider = new MapDictionaryProvider();
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final VectorSchemaRoot root = writeFlatDictionaryData(originalVectorAllocator, provider)) {
      roundTrip(
          root,
          provider,
          (ignored, writer) -> {
            writer.start();
            writer.writeBatch();
            writer.end();
            if (writer instanceof ArrowFileWriter) {
              numDictionaryBlocksWritten.set(((ArrowFileWriter) writer).getDictionaryBlocks().size());
            }
          },
          (fileReader) -> {
            VectorSchemaRoot readRoot = fileReader.getVectorSchemaRoot();
            Schema schema = readRoot.getSchema();
            LOGGER.debug("reading schema: " + schema);
            assertTrue(fileReader.loadNextBatch());
            validateFlatDictionary(readRoot, fileReader);
            assertEquals(numDictionaryBlocksWritten.get(), fileReader.getDictionaryBlocks().size());
          },
          (streamReader) -> {
            VectorSchemaRoot readRoot = streamReader.getVectorSchemaRoot();
            Schema schema = readRoot.getSchema();
            LOGGER.debug("reading schema: " + schema);
            assertTrue(streamReader.loadNextBatch());
            validateFlatDictionary(readRoot, streamReader);
          });

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }
  }

  @Test
  public void testNestedDictionary() throws Exception {
    AtomicInteger numDictionaryBlocksWritten = new AtomicInteger();
    MapDictionaryProvider provider = new MapDictionaryProvider();
    // data being written:
    // [['foo', 'bar'], ['foo'], ['bar']] -> [[0, 1], [0], [1]]
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final VectorSchemaRoot root = writeNestedDictionaryData(originalVectorAllocator, provider)) {
      CheckedConsumer<ArrowReader> validateDictionary = (streamReader) -> {
        VectorSchemaRoot readRoot = streamReader.getVectorSchemaRoot();
        Schema schema = readRoot.getSchema();
        LOGGER.debug("reading schema: " + schema);
        assertTrue(streamReader.loadNextBatch());
        validateNestedDictionary(readRoot, streamReader);
      };
      roundTrip(
          root,
          provider,
          (ignored, writer) -> {
            writer.start();
            writer.writeBatch();
            writer.end();
            if (writer instanceof ArrowFileWriter) {
              numDictionaryBlocksWritten.set(((ArrowFileWriter) writer).getDictionaryBlocks().size());
            }
          },
          validateDictionary,
          validateDictionary);

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }
  }

  @Test
  public void testFixedSizeBinary() throws Exception {
    final int count = 10;
    final int typeWidth = 11;
    byte[][] byteValues = new byte[count][typeWidth];
    for (int i = 0; i < count; i++) {
      for (int j = 0; j < typeWidth; j++) {
        byteValues[i][j] = ((byte) i);
      }
    }

    BiConsumer<Integer, VectorSchemaRoot> validator = (expectedCount, root) -> {
      for (int i = 0; i < expectedCount; i++) {
        assertArrayEquals(byteValues[i], ((byte[]) root.getVector("fixed-binary").getObject(i)));
      }
    };

    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      FixedSizeBinaryVector fixedSizeBinaryVector = parent.addOrGet("fixed-binary",
          FieldType.nullable(new ArrowType.FixedSizeBinary(typeWidth)), FixedSizeBinaryVector.class);
      parent.allocateNew();
      for (int i = 0; i < count; i++) {
        fixedSizeBinaryVector.set(i, byteValues[i]);
      }
      parent.setValueCount(count);

      roundTrip(
          new VectorSchemaRoot(parent),
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {count}, validator),
          validateStreamBatches(new int[] {count}, validator));
    }
  }

  @Test
  public void testFixedSizeList() throws Exception {
    BiConsumer<Integer, VectorSchemaRoot> validator = (expectedCount, root) -> {
      for (int i = 0; i < expectedCount; i++) {
        assertEquals(Collections2.asImmutableList(i + 0.1f, i + 10.1f), root.getVector("float-pairs")
            .getObject(i));
        assertEquals(i, root.getVector("ints").getObject(i));
      }
    };

    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      FixedSizeListVector tuples = parent.addOrGet("float-pairs",
          FieldType.nullable(new ArrowType.FixedSizeList(2)), FixedSizeListVector.class);
      Float4Vector floats = (Float4Vector) tuples.addOrGetVector(FieldType.nullable(Types.MinorType.FLOAT4.getType()))
          .getVector();
      IntVector ints = parent.addOrGet("ints", FieldType.nullable(new ArrowType.Int(32, true)), IntVector.class);
      parent.allocateNew();
      for (int i = 0; i < COUNT; i++) {
        tuples.setNotNull(i);
        floats.set(i * 2, i + 0.1f);
        floats.set(i * 2 + 1, i + 10.1f);
        ints.set(i, i);
      }
      parent.setValueCount(COUNT);

      roundTrip(
          new VectorSchemaRoot(parent),
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[] {COUNT}, validator),
          validateStreamBatches(new int[] {COUNT}, validator));
    }
  }

  @Test
  public void testVarBinary() throws Exception {
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeVarBinaryData(COUNT, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateVarBinary(COUNT, root);

      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[]{COUNT}, this::validateVarBinary),
          validateStreamBatches(new int[]{COUNT}, this::validateVarBinary));
    }
  }

  @Test
  public void testReadWriteMultipleBatches() throws IOException {
    File file = new File("target/mytest_nulls_multibatch.arrow");
    int numBlocksWritten = 0;

    try (IntVector vector = new IntVector("foo", allocator);) {
      Schema schema = new Schema(Collections.singletonList(vector.getField()));
      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           VectorSchemaRoot root =
               new VectorSchemaRoot(schema, Collections.singletonList((FieldVector) vector), vector.getValueCount());
           ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel(), writeOption)) {
        writeBatchData(writer, vector, root);
        numBlocksWritten = writer.getRecordBlocks().size();
      }
    }

    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      validateBatchData(reader, vector);
      assertEquals(numBlocksWritten, reader.getRecordBlocks().size());
    }
  }

  @Test
  public void testMap() throws Exception {
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final VectorSchemaRoot root = writeMapData(originalVectorAllocator)) {
      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[]{root.getRowCount()}, (count, readRoot) -> validateMapData(readRoot)),
          validateStreamBatches(new int[]{root.getRowCount()}, (count, readRoot) -> validateMapData(readRoot)));
    }
  }

  @Test
  public void testListAsMap() throws Exception {
    try (final BufferAllocator originalVectorAllocator =
             allocator.newChildAllocator("original vectors", 0, allocator.getLimit());
         final VectorSchemaRoot root = writeListAsMapData(originalVectorAllocator)) {
      roundTrip(
          root,
          /* dictionaryProvider */null,
          TestRoundTrip::writeSingleBatch,
          validateFileBatches(new int[]{root.getRowCount()}, (count, readRoot) -> validateListAsMapData(readRoot)),
          validateStreamBatches(new int[]{root.getRowCount()}, (count, readRoot) -> validateListAsMapData(readRoot)));
    }
  }

  // Generic test helpers

  private static void writeSingleBatch(VectorSchemaRoot root, ArrowWriter writer) throws IOException {
    writer.start();
    writer.writeBatch();
    writer.end();
  }

  private CheckedConsumer<ArrowFileReader> validateFileBatches(
      int[] counts, BiConsumer<Integer, VectorSchemaRoot> validator) {
    return (arrowReader) -> {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      List<ArrowBlock> recordBatches = arrowReader.getRecordBlocks();
      assertEquals(counts.length, recordBatches.size());
      long previousOffset = 0;
      for (ArrowBlock rbBlock : recordBatches) {
        assertTrue(rbBlock.getOffset() + " > " + previousOffset, rbBlock.getOffset() > previousOffset);
        previousOffset = rbBlock.getOffset();
        arrowReader.loadRecordBatch(rbBlock);
        assertEquals("RB #" + i, counts[i], root.getRowCount());
        validator.accept(counts[i], root);
        try (final ArrowRecordBatch batch = unloader.getRecordBatch()) {
          List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
          for (ArrowBuffer arrowBuffer : buffersLayout) {
            assertEquals(0, arrowBuffer.getOffset() % 8);
          }
        }
        ++i;
      }
    };
  }

  private CheckedConsumer<ArrowStreamReader> validateStreamBatches(
      int[] counts, BiConsumer<Integer, VectorSchemaRoot> validator) {
    return (arrowReader) -> {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;

      for (int n = 0; n < counts.length; n++) {
        assertTrue(arrowReader.loadNextBatch());
        assertEquals("RB #" + i, counts[i], root.getRowCount());
        validator.accept(counts[i], root);
        try (final ArrowRecordBatch batch = unloader.getRecordBatch()) {
          final List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
          for (ArrowBuffer arrowBuffer : buffersLayout) {
            assertEquals(0, arrowBuffer.getOffset() % 8);
          }
        }
        ++i;
      }
      assertFalse(arrowReader.loadNextBatch());
    };
  }

  @FunctionalInterface
  interface CheckedConsumer<T> {
    void accept(T t) throws Exception;
  }

  @FunctionalInterface
  interface CheckedBiConsumer<T, U> {
    void accept(T t, U u) throws Exception;
  }

  private void roundTrip(VectorSchemaRoot root, DictionaryProvider provider,
                         CheckedBiConsumer<VectorSchemaRoot, ArrowWriter> writer,
                         CheckedConsumer<? super ArrowFileReader> fileValidator,
                         CheckedConsumer<? super ArrowStreamReader> streamValidator) throws Exception {
    final File temp = File.createTempFile("arrow-test-" + name + "-", ".arrow");
    temp.deleteOnExit();
    final ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
    final Map<String, String> metadata = new HashMap<>();
    metadata.put("foo", "bar");
    try (final FileOutputStream fileStream = new FileOutputStream(temp);
         final ArrowFileWriter fileWriter =
             new ArrowFileWriter(root, provider, fileStream.getChannel(), metadata, writeOption);
         final ArrowStreamWriter streamWriter =
             new ArrowStreamWriter(root, provider, Channels.newChannel(memoryStream), writeOption)) {
      writer.accept(root, fileWriter);
      writer.accept(root, streamWriter);
    }

    MessageMetadataResult metadataResult = MessageSerializer.readMessage(
        new ReadChannel(Channels.newChannel(new ByteArrayInputStream(memoryStream.toByteArray()))));
    assertNotNull(metadataResult);
    assertEquals(writeOption.metadataVersion.toFlatbufID(), metadataResult.getMessage().version());

    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, allocator.getLimit());
        FileInputStream fileInputStream = new FileInputStream(temp);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(memoryStream.toByteArray());
        ArrowFileReader fileReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator);
        ArrowStreamReader streamReader = new ArrowStreamReader(inputStream, readerAllocator)) {
      fileValidator.accept(fileReader);
      streamValidator.accept(streamReader);
      assertEquals(writeOption.metadataVersion, fileReader.getFooter().getMetadataVersion());
      assertEquals(metadata, fileReader.getMetaData());
    }
  }
}
