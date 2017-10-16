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

import static java.nio.channels.Channels.newChannel;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TestUtils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestArrowReaderWriter {

  private BufferAllocator allocator;

  @Before
  public void init() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  ArrowBuf buf(byte[] bytes) {
    ArrowBuf buffer = allocator.buffer(bytes.length);
    buffer.writeBytes(bytes);
    return buffer;
  }

  byte[] array(ArrowBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  @Test
  public void test() throws IOException {
    Schema schema = new Schema(asList(new Field("testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.<Field>emptyList())));
    ArrowType type = schema.getFields().get(0).getType();
    FieldVector vector = TestUtils.newVector(FieldVector.class, "testField", type, allocator);
    vector.initializeChildrenFromFields(schema.getFields().get(0).getChildren());

    byte[] validity = new byte[] {(byte) 255, 0};
    // second half is "undefined"
    byte[] values = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = new VectorSchemaRoot(schema.getFields(), asList(vector), 16);
         ArrowFileWriter writer = new ArrowFileWriter(root, null, newChannel(out))) {
      ArrowBuf validityb = buf(validity);
      ArrowBuf valuesb = buf(values);
      writer.writeRecordBatch(new ArrowRecordBatch(16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb)));
    }

    byte[] byteArray = out.toByteArray();

    try (SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(byteArray));
         ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
      Schema readSchema = reader.getVectorSchemaRoot().getSchema();
      assertEquals(schema, readSchema);
      assertTrue(readSchema.getFields().get(0).getTypeLayout().getVectorTypes().toString(), readSchema.getFields().get(0).getTypeLayout().getVectors().size() > 0);
      // TODO: dictionaries
      List<ArrowBlock> recordBatches = reader.getRecordBlocks();
      assertEquals(1, recordBatches.size());
      ArrowRecordBatch recordBatch = (ArrowRecordBatch) reader.readMessage(channel, allocator);
      List<ArrowFieldNode> nodes = recordBatch.getNodes();
      assertEquals(1, nodes.size());
      ArrowFieldNode node = nodes.get(0);
      assertEquals(16, node.getLength());
      assertEquals(8, node.getNullCount());
      List<ArrowBuf> buffers = recordBatch.getBuffers();
      assertEquals(2, buffers.size());
      assertArrayEquals(validity, array(buffers.get(0)));
      assertArrayEquals(values, array(buffers.get(1)));

      // Read just the header. This demonstrates being able to read without need to
      // deserialize the buffer.
      ByteBuffer headerBuffer = ByteBuffer.allocate(recordBatches.get(0).getMetadataLength());
      headerBuffer.put(byteArray, (int) recordBatches.get(0).getOffset(), headerBuffer.capacity());
      headerBuffer.position(4);
      Message messageFB = Message.getRootAsMessage(headerBuffer);
      RecordBatch recordBatchFB = (RecordBatch) messageFB.header(new RecordBatch());
      assertEquals(2, recordBatchFB.buffersLength());
      assertEquals(1, recordBatchFB.nodesLength());
      FieldNode nodeFB = recordBatchFB.nodes(0);
      assertEquals(16, nodeFB.length());
      assertEquals(8, nodeFB.nullCount());
    }
  }

}
