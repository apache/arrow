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
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.NullableMapVector;
import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
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

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);

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

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);
        while (true) {
          try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
            if (recordBatch == null) break;
            List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
            for (ArrowBuffer arrowBuffer : buffersLayout) {
              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
            }
            vectorLoader.load(recordBatch);
          }
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

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);
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

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);
        while (true) {
          try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
            if (recordBatch == null) break;
            vectorLoader.load(recordBatch);
          }
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
          ArrowStreamWriter streamWriter = new ArrowStreamWriter(stream, schema, 2)) {
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
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator);) {
        VectorLoader vectorLoader = new VectorLoader(root);
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
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator);) {
        VectorLoader vectorLoader = new VectorLoader(root);
        for (int n = 0; n < 2; n++) {
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
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator);) {
        VectorLoader vectorLoader = new VectorLoader(root);
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

      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator)) {
        VectorLoader vectorLoader = new VectorLoader(root);
        while (true) {
          try (ArrowRecordBatch recordBatch = arrowReader.nextRecordBatch()) {
            if (recordBatch == null) break;
            vectorLoader.load(recordBatch);
          }
        }
        validateUnionData(count, root);
      }
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
        ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
            ) {
      arrowWriter.writeRecordBatch(recordBatch);
    }

    // Also try serializing to the stream writer.
    if (outStream != null) {
      try (
          ArrowStreamWriter arrowWriter = new ArrowStreamWriter(outStream, schema, -1);
          ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch();
          ) {
        arrowWriter.writeRecordBatch(recordBatch);
      }
    }
  }
}
