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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.BaseFileTest;
import org.apache.arrow.vector.ipc.MessageSerializerTest;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestArrowStream extends BaseFileTest {
  @Test
  public void testEmptyStream() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

    // Write the stream.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
    }

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
      assertEquals(schema, reader.getVectorSchemaRoot().getSchema());
      // Empty should return false
      Assert.assertFalse(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      Assert.assertFalse(reader.loadNextBatch());
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      int numBatches = 1;

      root.getFieldVectors().get(0).allocateNew();
      TinyIntVector vector = (TinyIntVector)root.getFieldVectors().get(0);
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
        assertEquals(bytesWritten, reader.bytesRead() + 4);
        assertFalse(reader.loadNextBatch());
        assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      }
    }
  }
}
