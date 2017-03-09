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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.stream.MessageSerializerTest;
import org.apache.arrow.vector.types.pojo.Schema;
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
      // Empty should return nothing. Can be called repeatedly.
      reader.loadNextBatch();
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      reader.loadNextBatch();
      assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      int numBatches = 1;

      root.getFieldVectors().get(0).allocateNew();
      NullableTinyIntVector.Mutator mutator = (NullableTinyIntVector.Mutator) root.getFieldVectors().get(0).getMutator();
      for (int i = 0; i < 16; i++) {
        mutator.set(i, i < 8 ? 1 : 0, (byte)(i + 1));
      }
      mutator.setValueCount(16);
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
          reader.loadNextBatch();
        }
        // TODO figure out why reader isn't getting padding bytes
        assertEquals(bytesWritten, reader.bytesRead() + 4);
        reader.loadNextBatch();
        assertEquals(0, reader.getVectorSchemaRoot().getRowCount());
      }
    }
  }
}
