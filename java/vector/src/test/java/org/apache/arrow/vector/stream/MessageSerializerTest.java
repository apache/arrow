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
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.file.WriteChannel;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class MessageSerializerTest {

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
  public void testSchemaMessageSerialization() throws IOException {
    Schema schema = testSchema();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long size = MessageSerializer.serialize(
        new WriteChannel(Channels.newChannel(out)), schema);
    assertEquals(size, out.toByteArray().length);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    Schema deserialized = MessageSerializer.deserializeSchema(
        new ReadChannel(Channels.newChannel(in)));
    assertEquals(schema, deserialized);
    assertEquals(1, deserialized.getFields().size());
  }

  @Test
  public void testSerializeRecordBatch() throws IOException {
    byte[] validity = new byte[] { (byte)255, 0};
    // second half is "undefined"
    byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    ArrowBuf validityb = buf(alloc, validity);
    ArrowBuf valuesb =  buf(alloc, values);

    ArrowRecordBatch batch = new ArrowRecordBatch(
        16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), batch);

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    ArrowRecordBatch deserialized = MessageSerializer.deserializeRecordBatch(
        new ReadChannel(Channels.newChannel(in)), alloc);
    verifyBatch(deserialized, validity, values);
  }

  public static Schema testSchema() {
    return new Schema(asList(new Field(
        "testField", true, new ArrowType.Int(8, true), Collections.<Field>emptyList())));
  }

  // Verifies batch contents matching test schema.
  public static void verifyBatch(ArrowRecordBatch batch, byte[] validity, byte[] values) {
    assertTrue(batch != null);
    List<ArrowFieldNode> nodes = batch.getNodes();
    assertEquals(1, nodes.size());
    ArrowFieldNode node = nodes.get(0);
    assertEquals(16, node.getLength());
    assertEquals(8, node.getNullCount());
    List<ArrowBuf> buffers = batch.getBuffers();
    assertEquals(2, buffers.size());
    assertArrayEquals(validity, MessageSerializerTest.array(buffers.get(0)));
    assertArrayEquals(values, MessageSerializerTest.array(buffers.get(1)));
  }

}
