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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flatbuf.StreamHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.BaseFileTest;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

public class TestArrowStream extends BaseFileTest {
  private ArrowStreamHeader roundTrip(ArrowStreamHeader header) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int i = header.writeTo(builder);
    builder.finish(i);
    ByteBuffer dataBuffer = builder.dataBuffer();
    return new ArrowStreamHeader(StreamHeader.getRootAsStreamHeader(dataBuffer));
  }

  @Test
  public void testStreamHeader() {
    Schema schema = new Schema(asList(
        new Field("a", true, new ArrowType.Int(8, true), Collections.<Field>emptyList())
        ));
    ArrowStreamHeader header = new ArrowStreamHeader(schema, -1);
    assertEquals(header, roundTrip(header));

    header = new ArrowStreamHeader(schema, 1000);
    assertEquals(header, roundTrip(header));
  }

  @Test
  public void testEmptyStream() throws IOException {
    Schema schema = new Schema(asList(
        new Field("a", true, new ArrowType.Int(8, true), Collections.<Field>emptyList())
        ));

    // Write the header.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (ArrowStreamWriter writer = new ArrowStreamWriter(out, schema, -1)) {
    }

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
      reader.init();
      assertEquals(schema, reader.getSchema());
      assertEquals(-1, reader.getTotalBatches());
      // Empty should return null. Can be called repeatedly.
      assertTrue(reader.nextRecordBatch() == null);
      assertTrue(reader.nextRecordBatch() == null);
    }
  }

  public static ArrowBuf buf(BufferAllocator alloc, byte[] bytes) {
    ArrowBuf buffer = alloc.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  public static byte[] array(ArrowBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  @Test
  public void testReadWrite() throws IOException {
    Schema schema = new Schema(asList(new Field(
        "testField", true, new ArrowType.Int(8, true), Collections.<Field>emptyList())));
    byte[] validity = new byte[] { (byte)255, 0};
    // second half is "undefined"
    byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    int numBatches = 5;
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long bytesWritten = 0;
    try (ArrowStreamWriter writer = new ArrowStreamWriter(out, schema, numBatches)) {
      ArrowBuf validityb = buf(alloc, validity);
      ArrowBuf valuesb =  buf(alloc, values);
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
      assertEquals(numBatches, reader.getTotalBatches());
      for (int i = 0; i < numBatches; i++) {
        assertEquals(schema, readSchema);
        assertTrue(
            readSchema.getFields().get(0).getTypeLayout().getVectorTypes().toString(),
            readSchema.getFields().get(0).getTypeLayout().getVectors().size() > 0);
        ArrowRecordBatch recordBatch = reader.nextRecordBatch();
        assertTrue(recordBatch != null);
        List<ArrowFieldNode> nodes = recordBatch.getNodes();
        assertEquals(1, nodes.size());
        ArrowFieldNode node = nodes.get(0);
        assertEquals(16, node.getLength());
        assertEquals(8, node.getNullCount());
        List<ArrowBuf> buffers = recordBatch.getBuffers();
        assertEquals(2, buffers.size());
        assertArrayEquals(validity, array(buffers.get(0)));
        assertArrayEquals(values, array(buffers.get(1)));
      }
      assertTrue(reader.nextRecordBatch() == null);
      assertEquals(bytesWritten, reader.bytesRead());
    }
  }
}
