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

import static org.apache.arrow.vector.TestVectorUnloadLoad.newVectorUnloader;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
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
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)
        ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();

        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
            for (ArrowBuffer arrowBuffer : buffersLayout) {
              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
            }
            vectorLoader.load(recordBatch);
          }

          validateContent(count, root);
        }
      }
    }

    // Read from stream.
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)
        ) {
      arrowReader.init();
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        Byte type = arrowReader.nextBatchType();
        while (type != null) {
          if (type == MessageHeader.DictionaryBatch) {
            try (ArrowDictionaryBatch dictionaryBatch = arrowReader.nextDictionaryBatch()) {
              List<ArrowBuffer> buffersLayout = dictionaryBatch.getDictionary().getBuffersLayout();
              for (ArrowBuffer arrowBuffer : buffersLayout) {
                Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
              }
              vectorLoader.load(dictionaryBatch);
            }
          } else if (type == MessageHeader.RecordBatch) {
            try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
              List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
              for (ArrowBuffer arrowBuffer : buffersLayout) {
                Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
              }
              vectorLoader.load(recordBatch);
            }
          } else {
            throw new IOException("Unexpected message header type " + type);
          }

          type = arrowReader.nextBatchType();
        }
        validateContent(count, root);
      }
    }
  }

  @Test
  public void testWriteReadComplex() throws IOException {
    File file = new File("target/mytest_complex.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeComplexData(count, parent);
      write(parent.getChild("root"), file, stream);
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)
        ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            vectorLoader.load(recordBatch);
          }
          validateComplexContent(count, root);
        }
      }
    }

    // Read from stream.
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)
        ) {
      arrowReader.init();
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        Byte type = arrowReader.nextBatchType();
        while (type != null) {
          if (type == MessageHeader.DictionaryBatch) {
            try (ArrowDictionaryBatch dictionaryBatch = arrowReader.nextDictionaryBatch()) {
              vectorLoader.load(dictionaryBatch);
            }
          } else if (type == MessageHeader.RecordBatch) {
            try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
              vectorLoader.load(recordBatch);
            }
          } else {
            throw new IOException("Unexpected message header type " + type);
          }

          type = arrowReader.nextBatchType();
        }
        validateComplexContent(count, root);
      }
    }
  }

  @Test
  public void testWriteReadMultipleRBs() throws IOException {
    File file = new File("target/mytest_multiple.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int[] counts = { 10, 5 };

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null);
        FileOutputStream fileOutputStream = new FileOutputStream(file);) {
      writeData(counts[0], parent);
      VectorUnloader vectorUnloader0 = newVectorUnloader(parent.getChild("root"));
      Schema schema = vectorUnloader0.getSchema();
      Assert.assertEquals(2, schema.getFields().size());
      try (ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);
          ArrowStreamWriter streamWriter = new ArrowStreamWriter(stream, schema)) {
        try (ArrowRecordBatch recordBatch = vectorUnloader0.getRecordBatch()) {
          Assert.assertEquals("RB #0", counts[0], recordBatch.getLength());
          arrowWriter.writeRecordBatch(recordBatch);
          streamWriter.writeRecordBatch(recordBatch);
        }
        parent.allocateNew();
        writeData(counts[1], parent); // if we write the same data we don't catch that the metadata is stored in the wrong order.
        VectorUnloader vectorUnloader1 = newVectorUnloader(parent.getChild("root"));
        try (ArrowRecordBatch recordBatch = vectorUnloader1.getRecordBatch()) {
          Assert.assertEquals("RB #1", counts[1], recordBatch.getLength());
          arrowWriter.writeRecordBatch(recordBatch);
          streamWriter.writeRecordBatch(recordBatch);
        }
      }
    }

    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null);
        ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        Assert.assertEquals(2, recordBatches.size());
        long previousOffset = 0;
        for (ArrowBlock rbBlock : recordBatches) {
          Assert.assertTrue(rbBlock.getOffset() + " > " + previousOffset, rbBlock.getOffset() > previousOffset);
          previousOffset = rbBlock.getOffset();
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            Assert.assertEquals("RB #" + i, counts[i], recordBatch.getLength());
            List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
            for (ArrowBuffer arrowBuffer : buffersLayout) {
              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
            }
            vectorLoader.load(recordBatch);
            validateContent(counts[i], root);
          }
          ++i;
        }
      }
    }

    // read stream
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)
        ) {
      arrowReader.init();
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);
      int i = 0;
      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        for (int n = 0; n < 2; n++) {
          Byte type = arrowReader.nextBatchType();
          Assert.assertEquals(new Byte(MessageHeader.RecordBatch), type);
          try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
            assertTrue(recordBatch != null);
            Assert.assertEquals("RB #" + i, counts[i], recordBatch.getLength());
            List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
            for (ArrowBuffer arrowBuffer : buffersLayout) {
              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
            }
            vectorLoader.load(recordBatch);
            validateContent(counts[i], root);
          }
          ++i;
        }
      }
    }
  }

  @Test
  public void testWriteReadUnion() throws IOException {
    File file = new File("target/mytest_write_union.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {

      writeUnionData(count, parent);

      printVectors(parent.getChildrenFromFields());

      validateUnionData(count, new VectorSchemaRoot(parent.getChild("root")));

      write(parent.getChild("root"), file, stream);
    }
    // read
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors
      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        for (ArrowBlock rbBlock : recordBatches) {
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            vectorLoader.load(recordBatch);
          }
          validateUnionData(count, root);
        }
      }
    }

    // Read from stream.
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", vectorAllocator, null)
        ) {
      arrowReader.init();
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        VectorSchemaRoot root = vectorLoader.getVectorSchemaRoot();
        Byte type = arrowReader.nextBatchType();
        while (type != null) {
          if (type == MessageHeader.DictionaryBatch) {
            try (ArrowDictionaryBatch dictionaryBatch = arrowReader.nextDictionaryBatch()) {
              vectorLoader.load(dictionaryBatch);
            }
          } else if (type == MessageHeader.RecordBatch) {
            try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
              vectorLoader.load(recordBatch);
            }
          } else {
            throw new IOException("Unexpected message header type " + type);
          }

          type = arrowReader.nextBatchType();
        }
        validateUnionData(count, root);
      }
    }
  }

  @Test
  public void testWriteReadDictionary() throws IOException {
    File file = new File("target/mytest_dict.arrow");
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableVarCharVector vector = new NullableVarCharVector("varchar", originalVectorAllocator);) {
      vector.allocateNewSafe();
      NullableVarCharVector.Mutator mutator = vector.getMutator();
      mutator.set(0, "foo".getBytes(StandardCharsets.UTF_8));
      mutator.set(1, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(3, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.set(4, "bar".getBytes(StandardCharsets.UTF_8));
      mutator.set(5, "baz".getBytes(StandardCharsets.UTF_8));
      mutator.setValueCount(6);
      DictionaryVector dictionaryVector = DictionaryVector.encode(vector);

      VectorUnloader vectorUnloader = new VectorUnloader(new Schema(ImmutableList.of(dictionaryVector.getField())), 6, ImmutableList.of((FieldVector)dictionaryVector));
      LOGGER.debug("writing schema: " + vectorUnloader.getSchema());
      try (
          FileOutputStream fileOutputStream = new FileOutputStream(file);
          ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), vectorUnloader.getSchema());
          ArrowStreamWriter streamWriter = new ArrowStreamWriter(stream, vectorUnloader.getSchema());
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();) {
        List<ArrowDictionaryBatch> dictionaryBatches = vectorUnloader.getDictionaryBatches();
        for (ArrowDictionaryBatch dictionaryBatch: dictionaryBatches) {
          arrowWriter.writeDictionaryBatch(dictionaryBatch);
          streamWriter.writeDictionaryBatch(dictionaryBatch);
          try { dictionaryBatch.close(); } catch (Exception e) { throw new IOException(e); }
        }
        arrowWriter.writeRecordBatch(recordBatch);
        streamWriter.writeRecordBatch(recordBatch);
      }

      dictionaryVector.getIndexVector().close();
      dictionaryVector.getDictionary().getVector().close();
    }

    // read from file
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        FileInputStream fileInputStream = new FileInputStream(file);
        ArrowReader arrowReader = new ArrowReader(fileInputStream.getChannel(), readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
    ) {
      ArrowFooter footer = arrowReader.readFooter();
      Schema schema = footer.getSchema();
      LOGGER.debug("reading schema: " + schema);

      // initialize vectors

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        for (ArrowBlock dictionaryBlock : footer.getDictionaries()) {
          try (ArrowDictionaryBatch dictionaryBatch = arrowReader.readDictionaryBatch(dictionaryBlock);) {
            vectorLoader.load(dictionaryBatch);
          }
        }
        for (ArrowBlock rbBlock : footer.getRecordBatches()) {
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            vectorLoader.load(recordBatch);
          }
        }
        validateDictionary(vectorLoader.getVectorSchemaRoot().getVector("varchar"));
      }
    }

    // Read from stream
    try (
        BufferAllocator readerAllocator = allocator.newChildAllocator("reader", 0, Integer.MAX_VALUE);
        ByteArrayInputStream input = new ByteArrayInputStream(stream.toByteArray());
        ArrowStreamReader arrowReader = new ArrowStreamReader(input, readerAllocator);
        BufferAllocator vectorAllocator = allocator.newChildAllocator("final vectors", 0, Integer.MAX_VALUE);
    ) {
      arrowReader.init();
      Schema schema = arrowReader.getSchema();
      LOGGER.debug("reading schema: " + schema);

      try (VectorLoader vectorLoader = new VectorLoader(schema, vectorAllocator);) {
        Byte type = arrowReader.nextBatchType();
        while (type != null) {
          if (type == MessageHeader.DictionaryBatch) {
            try (ArrowDictionaryBatch batch = arrowReader.nextDictionaryBatch()) {
              vectorLoader.load(batch);
            }
          } else if (type == MessageHeader.RecordBatch) {
            try (ArrowRecordBatch batch = arrowReader.nextRecordBatch()) {
              vectorLoader.load(batch);
            }
          } else {
            Assert.fail("Unexpected message type " + type);
          }
          type = arrowReader.nextBatchType();
        }
        validateDictionary(vectorLoader.getVectorSchemaRoot().getVector("varchar"));
      }
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
  private void write(FieldVector parent, File file, OutputStream outStream) throws FileNotFoundException, IOException {
    VectorUnloader vectorUnloader = newVectorUnloader(parent);
    Schema schema = vectorUnloader.getSchema();
    LOGGER.debug("writing schema: " + schema);
    try (
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);
        ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();) {
      List<ArrowDictionaryBatch> dictionaryBatches = vectorUnloader.getDictionaryBatches();
      for (ArrowDictionaryBatch dictionaryBatch: dictionaryBatches) {
        arrowWriter.writeDictionaryBatch(dictionaryBatch);
        try { dictionaryBatch.close(); } catch (Exception e) { throw new IOException(e); }
      }
      arrowWriter.writeRecordBatch(recordBatch);
    }

    // Also try serializing to the stream writer.
    if (outStream != null) {
      try (
          ArrowStreamWriter arrowWriter = new ArrowStreamWriter(outStream, schema);
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();) {
        List<ArrowDictionaryBatch> dictionaryBatches = vectorUnloader.getDictionaryBatches();
        for (ArrowDictionaryBatch dictionaryBatch: dictionaryBatches) {
          arrowWriter.writeDictionaryBatch(dictionaryBatch);
          dictionaryBatch.close();
        }
        arrowWriter.writeRecordBatch(recordBatch);
      }
    }
  }
}
