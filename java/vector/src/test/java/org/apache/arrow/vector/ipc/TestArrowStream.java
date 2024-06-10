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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Collections;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class TestArrowStream extends BaseFileTest {
  @Test
  public void testEmptyStream() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    // Write the stream.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out);
    writer.close();
    assertTrue(out.size() > 0);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
      assertEquals(schema, reader.getVectorSchemaRoot().getSchema());
      // Empty should return false
      assertFalse(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      assertFalse(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  public void testStreamZeroLengthBatch() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    try (IntVector vector = new IntVector("foo", allocator);) {
      Schema schema = new Schema(Collections.singletonList(vector.getField()));
      try (VectorSchemaRoot root =
             new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getValueCount());
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));) {
        vector.setValueCount(0);
        root.setRowCount(0);
        writer.writeBatch();
        writer.end();
      }
    }

    ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());

    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator);) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      IntVector vector = (IntVector) root.getFieldVectors().get(0);
      reader.loadNextBatch();
      assertEquals(0, vector.getValueCount());
      assertEquals(0, root.getRowCount());
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      int numBatches = 1;

      root.getFieldVectors().get(0).allocateNew();
      TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
      for (int i = 0; i < 16; i++) {
        vector.set(i, i < 8 ? 1 : 0, (byte) (i + 1));
      }
      vector.setValueCount(16);
      root.setRowCount(16);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      long bytesWritten = 0;
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
        writer.start();
        for (int i = 0; i < numBatches; i++) {
          writer.writeBatch();
        }
        writer.end();
        bytesWritten = writer.bytesWritten();
      }

      ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
      try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
        Schema readSchema = reader.getVectorSchemaRoot().getSchema();
        assertEquals(schema, readSchema);
        for (int i = 0; i < numBatches; i++) {
          assertTrue(reader.loadNextBatch());
        }
        // TODO figure out why reader isn't getting padding bytes
        assertEquals(bytesWritten, reader.bytesRead() + 8);
        assertFalse(reader.loadNextBatch());
        assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      }
    }
  }

  @Test
  public void testReadWriteMultipleBatches() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    try (IntVector vector = new IntVector("foo", allocator);) {
      Schema schema = new Schema(Collections.singletonList(vector.getField()));
      try (VectorSchemaRoot root =
             new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getValueCount());
           ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));) {
        writeBatchData(writer, vector, root);
      }
    }

    ByteArrayInputStream in = new ByteArrayInputStream(os.toByteArray());

    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator);) {
      IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);
      validateBatchData(reader, vector);
    }
  }
}
