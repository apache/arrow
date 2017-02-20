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
package org.apache.arrow.vector.file;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestArrowFile extends BaseFileTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestArrowFile.class);

  @Test
  public void testWrite() throws IOException {
    File file = new File("target/mytest_write.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)) {
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
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {
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
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      List<FieldVector> vectors = arrowReader.getVectors();

      for (ArrowBlock rbBlock : footer.getRecordBatches()) {
        int loaded = arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, loaded);
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), vectors);
        root.setRowCount(loaded);
        validateContent(count, root);
      }

      // TODO
//      List<ArrowBuffer> buffersLayout = batch.getBuffersLayout();
//      for (ArrowBuffer arrowBuffer : buffersLayout) {
//        Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
//      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {

      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);
      List<FieldVector> vectors = arrowReader.getVectors();

      int loaded = arrowReader.loadNextBatch();
      Assert.assertEquals(count, loaded);

//            List<ArrowBuffer> buffersLayout = dictionaryBatch.getDictionary().getBuffersLayout();
//            for (ArrowBuffer arrowBuffer : buffersLayout) {
//              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
//            }
//            vectorLoader.load(dictionaryBatch);
//
      VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), vectors);
      root.setRowCount(loaded);
      validateContent(count, root);
    }
  }

  @Test
  public void testWriteReadComplex() throws IOException {
    File file = new File("target/mytest_complex.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeComplexData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      List<FieldVector> vectors = arrowReader.getVectors();

      for (ArrowBlock rbBlock : footer.getRecordBatches()) {
        int loaded = arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals(count, loaded);
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), vectors);
        root.setRowCount(loaded);
        validateComplexContent(count, root);
      }
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {

      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);

      List<FieldVector> vectors = arrowReader.getVectors();

      int loaded = arrowReader.loadNextBatch();
      Assert.assertEquals(count, loaded);
      VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), vectors);
      root.setRowCount(loaded);
      validateComplexContent(count, root);
    }
  }

  @Test
  public void testWriteReadMultipleRBs() throws IOException {
    File file = new File("target/mytest_multiple.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int[] counts = { 10, 5 };

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         MapVector parent = new MapVector("parent", originalVectorAllocator, null);
         FileOutputStream fileOutputStream = new FileOutputStream(file);){
      writeData(counts[0], parent);

      FieldVector root = parent.getChild("root");
      List<Field> fields = root.getField().getChildren();
      List<FieldVector> vectors = root.getChildrenFromFields();
      try(ArrowFileWriter fileWriter = new ArrowFileWriter(fields, vectors, fileOutputStream.getChannel());
          ArrowStreamWriter streamWriter = new ArrowStreamWriter(fields, vectors, stream)) {
        fileWriter.start();
        streamWriter.start();

        int valueCount = root.getAccessor().getValueCount();
        fileWriter.writeBatch(valueCount);
        streamWriter.writeBatch(valueCount);

        parent.allocateNew();
        writeData(counts[1], parent); // if we write the same data we don't catch that the metadata is stored in the wrong order.
        valueCount = root.getAccessor().getValueCount();
        fileWriter.writeBatch(valueCount);
        streamWriter.writeBatch(valueCount);

        fileWriter.end();
        streamWriter.end();
      }
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      List<ArrowBlock> recordBatches = footer.getRecordBatches();
      Assert.assertEquals(2, recordBatches.size());
      long previousOffset = 0;
      for (ArrowBlock rbBlock : recordBatches) {
        Assert.assertTrue(rbBlock.getOffset() + " > " + previousOffset, rbBlock.getOffset() > previousOffset);
        previousOffset = rbBlock.getOffset();
        int loaded = arrowReader.loadRecordBatch(rbBlock);
        Assert.assertEquals("RB #" + i, counts[i], loaded);
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), arrowReader.getVectors());
        root.setRowCount(loaded);
        validateContent(counts[i], root);
        ++i;
      }
    }

    // read stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;

      for (int n = 0; n < 2; n++) {
        int loaded = arrowReader.loadNextBatch();
        Assert.assertEquals("RB #" + i, counts[i], loaded);
        VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), arrowReader.getVectors());
        root.setRowCount(loaded);
        validateContent(counts[i], root);
        ++i;
      }
      int loaded = arrowReader.loadNextBatch();
      Assert.assertEquals(0, loaded);
    }
  }

  @Test
  public void testWriteReadUnion() throws IOException {
    File file = new File("target/mytest_write_union.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {
      writeUnionData(count, parent);
      validateUnionData(count, new VectorSchemaRoot(parent.getChild("root")));
      write(parent.getChild("root"), file, stream);
    }

    // read file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      // initialize vectors
      VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), arrowReader.getVectors());
      validateUnionData(count, root);
    }

    // Read from stream.
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      // initialize vectors
      VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), arrowReader.getVectors());
      validateUnionData(count, root);
    }
  }

  @Test
  public void testWriteReadDictionary() throws IOException {
    File file = new File("target/mytest_dict.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    // write
    try (BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
         NullableVarCharVector vector = new NullableVarCharVector("varchar", originalVectorAllocator);
         NullableVarCharVector dictionary = new NullableVarCharVector("dict", originalVectorAllocator)) {
      vector.allocateNewSafe();
      NullableVarCharVector.Mutator mutator = vector.getMutator();
      mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(3, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.set(4, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(5, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.setValueCount(6);

      dictionary.allocateNewSafe();
      mutator = dictionary.getMutator();
      mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(2, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.setValueCount(3);

      DictionaryVector dictionaryVector = DictionaryVector.encode(vector, new Dictionary(dictionary, 1L, false));

      List<Field> fields = ImmutableList.of(dictionaryVector.getField());
      List<FieldVector> vectors = ImmutableList.of((FieldVector) dictionaryVector);

      try (FileOutputStream fileOutputStream = new FileOutputStream(file);
           ArrowFileWriter fileWriter = new ArrowFileWriter(fields, vectors, fileOutputStream.getChannel());
           ArrowStreamWriter streamWriter = new ArrowStreamWriter(fields, vectors, stream)) {
        LOGGER.debug("writing schema: " + fileWriter.getSchema());
        fileWriter.start();
        streamWriter.start();
        fileWriter.writeBatch(6);
        streamWriter.writeBatch(6);
        fileWriter.end();
        streamWriter.end();
      }

      dictionaryVector.close();
    }

    // read from file
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         FileInputStream fileInputStream = new FileInputStream(file);
         ArrowFileReader arrowReader = new ArrowFileReader(fileInputStream.getChannel(), readerAllocator)) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateDictionary(arrowReader.getVectors().get(0));
    }

    // Read from stream
    try (BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
         ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
         ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator)) {
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);
      arrowReader.loadNextBatch();
      validateDictionary(arrowReader.getVectors().get(0));
    }
  }

  private void validateDictionary(FieldVector vector) {
    Assert.assertNotNull(vector);
    Assert.assertEquals(DictionaryVector.class, vector.getClass());
    Dictionary dictionary = ((DictionaryVector) vector).getDictionary();
    try {
      Assert.assertNotNull(dictionary.getId());
      NullableVarCharVector.Accessor dictionaryAccessor = ((NullableVarCharVector) dictionary.getVector()).getAccessor();
      Assert.assertEquals(3, dictionaryAccessor.getValueCount());
      Assert.assertEquals(new Text("foo"), dictionaryAccessor.getObject(0));
      Assert.assertEquals(new Text("bar"), dictionaryAccessor.getObject(1));
      Assert.assertEquals(new Text("baz"), dictionaryAccessor.getObject(2));
      FieldVector.Accessor accessor = vector.getAccessor();
      Assert.assertEquals(6, accessor.getValueCount());
      Assert.assertEquals(0, accessor.getObject(0));
      Assert.assertEquals(1, accessor.getObject(1));
      Assert.assertEquals(null, accessor.getObject(2));
      Assert.assertEquals(2, accessor.getObject(3));
      Assert.assertEquals(1, accessor.getObject(4));
      Assert.assertEquals(2, accessor.getObject(5));
    } finally {
      dictionary.getVector().close();
    }
  }

  /**
   * Writes the contents of parents to file. If outStream is non-null, also writes it
   * to outStream in the streaming serialized format.
   */
  private void write(FieldVector parent, File file, OutputStream outStream) throws IOException {
    int valueCount = parent.getAccessor().getValueCount();
    List<Field> fields = parent.getField().getChildren();
    List<FieldVector> vectors = parent.getChildrenFromFields();

    try (FileOutputStream fileOutputStream = new FileOutputStream(file);
         ArrowFileWriter arrowWriter = new ArrowFileWriter(fields, vectors, fileOutputStream.getChannel());) {
      LOGGER.debug("writing schema: " + arrowWriter.getSchema());
      arrowWriter.start();
      arrowWriter.writeBatch(valueCount);
      arrowWriter.end();
    }

    // Also try serializing to the stream writer.
    if (outStream != null) {
      try (ArrowStreamWriter arrowWriter = new ArrowStreamWriter(fields, vectors, outStream)) {
        arrowWriter.start();
        arrowWriter.writeBatch(valueCount);
        arrowWriter.end();
      }
    }
  }
}
