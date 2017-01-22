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
package org.apache.arrow.vector.stream;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.BaseFileTest;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestArrowStream extends BaseFileTest {
  @Test
  public void testEmptyStream() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();

    // Write the stream.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(out, schema)) {
    }

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
      reader.init();
      assertEquals(schema, reader.getSchema());
      // Empty should return null. Can be called repeatedly.
      assertTrue(reader.nextRecordBatch() == null);
      assertTrue(reader.nextRecordBatch() == null);
    }
  }

  @Test
  public void testReadWrite() throws IOException {
    Schema schema = MessageSerializerTest.testSchema();
    byte[] validity = new byte[] { (byte)255, 0};
    // second half is "undefined"
    byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    int numBatches = 5;
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long bytesWritten = 0;
    try (ArrowStreamWriter writer = new ArrowStreamWriter(out, schema)) {
      ArrowBuf validityb = MessageSerializerTest.buf(alloc, validity);
      ArrowBuf valuesb =  MessageSerializerTest.buf(alloc, values);
      for (int i = 0; i < numBatches; i++) {
        writer.writeRecordBatch(new ArrowRecordBatch(
            16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb)));
      }
      bytesWritten = writer.bytesWritten();
    }

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ArrowStreamReader reader = new ArrowStreamReader(in, alloc)) {
      reader.init();
      Schema readSchema = reader.getSchema();
      for (int i = 0; i < numBatches; i++) {
        assertEquals(schema, readSchema);
        assertTrue(
            readSchema.getFields().get(0).getTypeLayout().getVectorTypes().toString(),
            readSchema.getFields().get(0).getTypeLayout().getVectors().size() > 0);
        ArrowRecordBatch recordBatch = reader.nextRecordBatch();
        MessageSerializerTest.verifyBatch(recordBatch, validity, values);
        assertTrue(recordBatch != null);
      }
      assertTrue(reader.nextRecordBatch() == null);
      assertEquals(bytesWritten, reader.bytesRead());
    }
  }
}
