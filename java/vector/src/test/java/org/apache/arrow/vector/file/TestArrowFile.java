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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
      write(parent.getChild("root"), file);
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
      write(root, file);
    }
  }

  @Test
  public void testWriteRead() throws IOException {
    File file = new File("target/mytest.arrow");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeData(count, parent);
      write(parent.getChild("root"), file);
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
          Assert.assertEquals(0, rbBlock.getOffset() % 8);
          Assert.assertEquals(0, rbBlock.getMetadataLength() % 8);
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
  }

  @Test
  public void testWriteReadComplex() throws IOException {
    File file = new File("target/mytest_complex.arrow");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null)) {
      writeComplexData(count, parent);
      write(parent.getChild("root"), file);
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
  }

  @Test
  public void testWriteReadMultipleRBs() throws IOException {
    File file = new File("target/mytest_multiple.arrow");
    int count = COUNT;

    // write
    try (
        BufferAllocator originalVectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        MapVector parent = new MapVector("parent", originalVectorAllocator, null);
        FileOutputStream fileOutputStream = new FileOutputStream(file);) {
      writeData(count, parent);
      VectorUnloader vectorUnloader = newVectorUnloader(parent.getChild("root"));
      Schema schema = vectorUnloader.getSchema();
      Assert.assertEquals(2, schema.getFields().size());
      try (ArrowWriter arrowWriter = new ArrowWriter(fileOutputStream.getChannel(), schema);) {
        try (ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch()) {
          arrowWriter.writeRecordBatch(recordBatch);
        }
        parent.allocateNew();
        writeData(count, parent);
        try (ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch()) {
          arrowWriter.writeRecordBatch(recordBatch);
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
      try (VectorSchemaRoot root = new VectorSchemaRoot(schema, vectorAllocator);) {
        VectorLoader vectorLoader = new VectorLoader(root);
        List<ArrowBlock> recordBatches = footer.getRecordBatches();
        Assert.assertEquals(2, recordBatches.size());
        for (ArrowBlock rbBlock : recordBatches) {
          Assert.assertEquals(0, rbBlock.getOffset() % 8);
          Assert.assertEquals(0, rbBlock.getMetadataLength() % 8);
          try (ArrowRecordBatch recordBatch = arrowReader.readRecordBatch(rbBlock)) {
            List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
            for (ArrowBuffer arrowBuffer : buffersLayout) {
              Assert.assertEquals(0, arrowBuffer.getOffset() % 8);
            }
            vectorLoader.load(recordBatch);
            validateContent(count, root);
          }
        }
      }
    }
  }

  @Test
  public void testWriteReadUnion() throws IOException {
    File file = new File("target/mytest_write_union.arrow");
    int count = COUNT;
    try (
        BufferAllocator vectorAllocator = allocator.newChildAllocator("original vectors", 0, Integer.MAX_VALUE);
        NullableMapVector parent = new NullableMapVector("parent", vectorAllocator, null)) {

      writeUnionData(count, parent);

      printVectors(parent.getChildrenFromFields());

      validateUnionData(count, new VectorSchemaRoot(parent.getChild("root")));

      write(parent.getChild("root"), file);
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
  }

  private void write(FieldVector parent, File file) throws FileNotFoundException, IOException {
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
  }
}
