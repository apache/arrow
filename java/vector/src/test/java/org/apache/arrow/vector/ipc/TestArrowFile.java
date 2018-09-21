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

package org.apache.arrow.vector.ipc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowBuffer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeList;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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
  public void testWriteRead() throws IOException {
    File file = new File("target/mytest.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {

      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, root.getRowCount());
        ArrowRecordBatch batch = unloader.getRecordBatch();
        List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
        for (ArrowBuffer arrowBuffer : buffersLayout) {
          Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
        }
        validateContent(count, root);
        batch.close();
      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {

      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      ArrowRecordBatch batch = unloader.getRecordBatch();
      List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
      for (ArrowBuffer arrowBuffer : buffersLayout) {
        Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
      }
      batch.close();
      Assert.assertEquals(count, root.getRowCount());
      validateContent(count, root);
    }
  }

  @Test
  public void testWriteReadComplex() throws IOException {
    File file = new File("target/mytest_complex.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      writeComplexData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);

      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, root.getRowCount());
        validateComplexContent(count, root);
      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      Assert.assertEquals(count, root.getRowCount());
      validateComplexContent(count, root);
    }
  }

  @Test
  public void testWriteReadMultipleRBs() throws IOException {
    File file = new File("target/mytest_multiple.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int[] counts = {10, 5};

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator);
         FileOutputStream fileOutputStream = new FileOutputStream(file)) {
      writeData(counts[0], parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));

      try (ArrowFileWriter fileWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, stream)) {
        fileWriter.start();
        streamWriter.start();

        fileWriter.writeBatch();
        streamWriter.writeBatch();

        parent.allocateNew();
        // if we write the same data we don't catch that the metadata is stored in the wrong order.
        writeData(counts[1], parent);
        root.setRowCount(counts[1]);

        fileWriter.writeBatch();
        streamWriter.writeBatch();

        fileWriter.end();
        streamWriter.end();
      }
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      List<ArrowBlock> recordBatches = arrowReader.getRecordBlocks();
      Assert.assertEquals(2, recordBatches.size());
      long previousOffset = 0;
      for (ArrowBlock rbBlock : recordBatches) {
        Assert.assertTrue(rbBlock.getOffset() + " > " + previousOffset, rbBlock.getOffset() > previousOffset);
        previousOffset = rbBlock.getOffset();
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals("RB #" + i, counts[i], root.getRowCount());
        validateContent(counts[i], root);
        ++i;
      }
    }

    // read stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;

      for (int n = 0; n < 2; n++) {
        Assert.assertTrue(arrowReader.loadNextBatch());
        Assert.assertEquals("RB #" + i, counts[i], root.getRowCount());
        validateContent(counts[i], root);
        ++i;
      }
      Assert.assertFalse(arrowReader.loadNextBatch());
    }
  }

  @Test
  public void testWriteReadUnion() throws IOException {
    File file = new File("target/mytest_write_union.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeUnionData(count, parent);
      validateUnionData(count, new VectorSchemaRoot(parent.getChild("root")));
      write(parent.getChild("root"), file, stream);
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateUnionData(count, root);
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateUnionData(count, root);
    }
  }

  @Test
  public void testWriteReadTiny() throws IOException {
    File file = new File("target/mytest_write_tiny.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    try (VectorSchemaRoot root = VectorSchemaRoot.create(MessageSerializerTest.testSchema(), allocator)) {
      root.getFieldVectors().get(0).allocateNew();
      TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
      for (int i = 0; i < 16; i++) {
        vector.set(i, i < 8 ? 1 : 0, (byte) (i + 1));
      }
      vector.setValueCount(16);
      root.setRowCount(16);

      // write file
      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter arrowWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel())) {
        LOGGER.debug("writing schema: " + root.getSchema());
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
      // write stream
      try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(root, null, stream)) {
        arrowWriter.start();
        arrowWriter.writeBatch();
        arrowWriter.end();
      }
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("fileReader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateTinyData(root);
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("streamReader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateTinyData(root);
    }
  }

  private void validateTinyData(VectorSchemaRoot root) {
    Assert.assertEquals(16, root.getRowCount());
    TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
    for (int i = 0; i < 16; i++) {
      if (i < 8) {
        Assert.assertEquals((byte) (i + 1), vector.get(i));
      } else {
        Assert.assertTrue(vector.isNull(i));
      }
    }
  }

  @Test
  public void testWriteReadMetadata() throws IOException {
    File file = new File("target/mytest_metadata.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    List<Field> childFields = new ArrayList<Field>();
    childFields.add(new Field("varchar-child", new FieldType(true, ArrowType.Utf8.INSTANCE, null, metadata(1)), null));
    childFields.add(new Field("float-child",
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null, metadata(2)), null));
    childFields.add(new Field("int-child", new FieldType(false, new ArrowType.Int(32, true), null, metadata(3)), null));
    childFields.add(new Field("list-child", new FieldType(true, ArrowType.List.INSTANCE, null, metadata(4)),
        ImmutableList.of(new Field("l1", FieldType.nullable(new ArrowType.Int(16, true)), null))));
    Field field = new Field("meta", new FieldType(true, ArrowType.Struct.INSTANCE, null, metadata(0)), childFields);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("s1", "v1");
    metadata.put("s2", "v2");
    Schema originalSchema = new Schema(ImmutableList.of(field), metadata);
    Assert.assertEquals(metadata, originalSchema.getCustomMetadata());

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector vector = (StructVector) field.createVector(originalVectorAllocator)) {
      vector.allocateNewSafe();
      vector.setValueCount(0);

      List<FieldVector> vectors = ImmutableList.<FieldVector>of(vector);
      VectorSchemaRoot root = new VectorSchemaRoot(originalSchema, vectors, 0);

      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter fileWriter = new ArrowFileWriter(root, null, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, stream)) {
        LOGGER.debug("writing schema: " + root.getSchema());
        fileWriter.start();
        streamWriter.start();
        fileWriter.writeBatch();
        streamWriter.writeBatch();
        fileWriter.end();
        streamWriter.end();
      }
    }

    // read from file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertEquals(originalSchema, schema);
      Assert.assertEquals(originalSchema.getCustomMetadata(), schema.getCustomMetadata());
      Field top = schema.getFields().get(0);
      Assert.assertEquals(metadata(0), top.getMetadata());
      for (int i = 0; i < 4; i++) {
        Assert.assertEquals(metadata(i + 1), top.getChildren().get(i).getMetadata());
      }
    }

    // Read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertEquals(originalSchema, schema);
      Assert.assertEquals(originalSchema.getCustomMetadata(), schema.getCustomMetadata());
      Field top = schema.getFields().get(0);
      Assert.assertEquals(metadata(0), top.getMetadata());
      for (int i = 0; i < 4; i++) {
        Assert.assertEquals(metadata(i + 1), top.getChildren().get(i).getMetadata());
      }
    }
  }

  private Map<String, String> metadata(int i) {
    return ImmutableMap.of("k_" + i, "v_" + i, "k2_" + i, "v2_" + i);
  }

  @Test
  public void testWriteReadDictionary() throws IOException {
    File file = new File("target/mytest_dict.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int numDictionaryBlocksWritten = 0;

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)) {

      MapDictionaryProvider provider = new MapDictionaryProvider();

      try (VectorSchemaRoot root = writeFlatDictionaryData(originalVectorAllocator, provider);
           FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter fileWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, provider, stream)) {
        LOGGER.debug("writing schema: " + root.getSchema());
        fileWriter.start();
        streamWriter.start();
        fileWriter.writeBatch();
        streamWriter.writeBatch();
        fileWriter.end();
        streamWriter.end();
        numDictionaryBlocksWritten = fileWriter.getDictionaryBlocks().size();
      }

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }

    // read from file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateFlatDictionary(root, arrowReader);
      Assert.assertEquals(numDictionaryBlocksWritten, arrowReader.getDictionaryBlocks().size());
    }

    // Read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateFlatDictionary(root, arrowReader);
    }
  }

  @Test
  public void testWriteReadNestedDictionary() throws IOException {
    File file = new File("target/mytest_dict_nested.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    // data being written:
    // [['foo', 'bar'], ['foo'], ['bar']] -> [[0, 1], [0], [1]]

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE)
    ) {
      MapDictionaryProvider provider = new MapDictionaryProvider();

      try (VectorSchemaRoot root = writeNestedDictionaryData(vectorAllocator, provider);
           FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter fileWriter = new ArrowFileWriter(root, provider, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, provider, stream)) {

        validateNestedDictionary(root, provider);

        LOGGER.debug("writing schema: " + root.getSchema());
        fileWriter.start();
        streamWriter.start();
        fileWriter.writeBatch();
        streamWriter.writeBatch();
        fileWriter.end();
        streamWriter.end();
      }

      // Need to close dictionary vectors
      for (long id : provider.getDictionaryIds()) {
        provider.lookup(id).getVector().close();
      }
    }

    // read from file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateNestedDictionary(root, arrowReader);
    }

    // Read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateNestedDictionary(root, arrowReader);
    }
  }

  @Test
  public void testWriteReadFixedSizeBinary() throws IOException {
    File file = new File("target/mytest_fixed_size_binary.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    final int numValues = 10;
    final int typeWidth = 11;
    byte[][] byteValues = new byte[numValues][typeWidth];
    for (int i = 0; i < numValues; i++) {
      for (int j = 0; j < typeWidth; j++) {
        byteValues[i][j] = ((byte) i);
      }
    }

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      FixedSizeBinaryVector fixedSizeBinaryVector = parent.addOrGet("fixed-binary",
          FieldType.nullable(new FixedSizeBinary(typeWidth)), FixedSizeBinaryVector.class);
      parent.allocateNew();
      for (int i = 0; i < numValues; i++) {
        fixedSizeBinaryVector.set(i, byteValues[i]);
      }
      parent.setValueCount(numValues);
      write(parent, file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);

      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(numValues, root.getRowCount());
        for (int i = 0; i < numValues; i++) {
          Assert.assertArrayEquals(byteValues[i], ((byte[]) root.getVector("fixed-binary").getObject(i)));
        }
      }
    }

    // read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      Assert.assertEquals(numValues, root.getRowCount());
      for (int i = 0; i < numValues; i++) {
        Assert.assertArrayEquals(byteValues[i], ((byte[]) root.getVector("fixed-binary").getObject(i)));
      }
    }
  }

  @Test
  public void testWriteReadFixedSizeList() throws IOException {
    File file = new File("target/mytest_fixed_list.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator =
           allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         StructVector parent = StructVector.empty("parent", originalVectorAllocator)) {
      FixedSizeListVector tuples = parent.addOrGet("float-pairs",
          FieldType.nullable(new FixedSizeList(2)), FixedSizeListVector.class);
      Float4Vector floats = (Float4Vector) tuples.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType()))
          .getVector();
      IntVector ints = parent.addOrGet("ints", FieldType.nullable(new Int(32, true)), IntVector.class);
      parent.allocateNew();

      for (int i = 0; i < 10; i++) {
        tuples.setNotNull(i);
        floats.set(i * 2, i + 0.1f);
        floats.set(i * 2 + 1, i + 10.1f);
        ints.set(i, i);
      }

      parent.setValueCount(10);
      write(parent, file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);

      for (ArrowBlock rbBlock : arrowReader.getRecordBlocks()) {
        arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, root.getRowCount());
        for (int i = 0; i < 10; i++) {
          Assert.assertEquals(Lists.newArrayList(i + 0.1f, i + 10.1f), root.getVector("float-pairs").getObject(i));
          Assert.assertEquals(i, root.getVector("ints").getObject(i));
        }
      }
    }

    // read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      Assert.assertEquals(count, root.getRowCount());
      for (int i = 0; i < 10; i++) {
        Assert.assertEquals(Lists.newArrayList(i + 0.1f, i + 10.1f), root.getVector("float-pairs").getObject(i));
        Assert.assertEquals(i, root.getVector("ints").getObject(i));
      }
    }
  }

  @Test
  public void testWriteReadVarBin() throws IOException {
    File file = new File("target/mytest_varbin.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        StructVector parent = StructVector.empty("parent", vectorAllocator)) {
      writeVarBinaryData(count, parent);
      VectorSchemaRoot root = new VectorSchemaRoot(parent.getChild("root"));
      validateVarBinary(count, root);
      write(parent.getChild("root"), file, stream);
    }

    // read from file
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateVarBinary(count, root);
    }

    // read from stream
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
      Schema schema = root.getSchema();
      LOGGER.debug("reading schema: " + schema);
      Assert.assertTrue(arrowReader.loadNextBatch());
      validateVarBinary(count, root);
    }
  }

  @Test
  public void testReadWriteMultipleBatches() throws IOException {
    File file = new File("target/mytest_nulls_multibatch.arrow");
    int numBlocksWritten = 0;

    try (IntVector vector = new IntVector("foo", allocator);) {
      Schema schema = new Schema(Collections.singletonList(vector.getField()), null);
      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           VectorSchemaRoot root =
             new VectorSchemaRoot(schema, Collections.singletonList((FieldVector) vector), vector.getValueCount());
           ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel());) {
        writeBatchData(writer, vector, root);
        numBlocksWritten = writer.getRecordBlocks().size();
      }
    }

    try (FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);) {
      IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      validateBatchData(reader, vector);
      Assert.assertEquals(numBlocksWritten, reader.getRecordBlocks().size());
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
}
