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
package org.apache.arrow.tools;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class EchoServerTest {
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

  private void testEchoServer(int serverPort, Schema schema, List<ArrowRecordBatch> batches)
      throws UnknownHostException, IOException {
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    try (Socket socket = new Socket("localhost", serverPort);
        ArrowStreamWriter writer = new ArrowStreamWriter(socket.getOutputStream(), schema);
        ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), alloc)) {
      for (ArrowRecordBatch batch: batches) {
        writer.writeRecordBatch(batch);
      }
      writer.end();

      reader.init();
      assertEquals(schema, reader.getSchema());
      for (int i = 0; i < batches.size(); i++) {
        ArrowRecordBatch result = reader.nextRecordBatch();
        ArrowRecordBatch expected = batches.get(i);
        assertTrue(result != null);
        assertEquals(expected.getBuffers().size(), result.getBuffers().size());
        for (int j = 0; j < expected.getBuffers().size(); j++) {
          assertTrue(expected.getBuffers().get(j).compareTo(result.getBuffers().get(j)) == 0);
        }
      }
      ArrowRecordBatch result = reader.nextRecordBatch();
      assertTrue(result == null);
      assertEquals(reader.bytesRead(), writer.bytesWritten());
    }
  }

  @Test
  public void basicTest() throws InterruptedException, IOException {
    final EchoServer server = new EchoServer(0);
    int serverPort = server.port();
    Thread serverThread = new Thread() {
      @Override
      public void run() {
        try {
          server.run();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    serverThread.start();

    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    byte[] validity = new byte[] { (byte)255, 0};
    byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    ArrowBuf validityb = buf(alloc, validity);
    ArrowBuf valuesb =  buf(alloc, values);
    ArrowRecordBatch batch = new ArrowRecordBatch(
        16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb));

    Schema schema = new Schema(asList(new Field(
        "testField", true, new ArrowType.Int(8, true), Collections.<Field>emptyList())));

    // Try an empty stream, just the header.
    testEchoServer(serverPort, schema, new ArrayList<ArrowRecordBatch>());

    // Try with one batch.
    List<ArrowRecordBatch> batches = new ArrayList<>();
    batches.add(batch);
    testEchoServer(serverPort, schema, batches);

    // Try with a few
    for (int i = 0; i < 10; i++) {
      batches.add(batch);
    }
    testEchoServer(serverPort, schema, batches);

    server.close();
    serverThread.join();
  }
}
