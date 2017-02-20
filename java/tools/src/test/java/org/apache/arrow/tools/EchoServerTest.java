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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

  private void testEchoServer(int serverPort,
                              Field field,
                              NullableTinyIntVector vector,
                              int batches)
      throws UnknownHostException, IOException {
    BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    try (Socket socket = new Socket("localhost", serverPort);
        ArrowStreamWriter writer = new ArrowStreamWriter(asList(field), asList((FieldVector) vector), socket.getOutputStream());
        ArrowStreamReader reader = new ArrowStreamReader(socket.getInputStream(), alloc)) {
      writer.start();
      for (int i = 0; i < batches; i++) {
        vector.allocateNew(16);
        for (int j = 0; j < 8; j++) {
          vector.getMutator().set(j, j + i);
          vector.getMutator().set(j + 8, 0, (byte) (j + i));
        }
        vector.getMutator().setValueCount(16);
        writer.writeBatch(16);
      }
      writer.end();

      assertEquals(new Schema(asList(field)), reader.getSchema());

      NullableTinyIntVector readVector = (NullableTinyIntVector) reader.getVectors().get(0);
      for (int i = 0; i < batches; i++) {
        int loaded = reader.loadNextBatch();
        assertEquals(16, loaded);
        assertEquals(16, readVector.getAccessor().getValueCount());
        for (int j = 0; j < 8; j++) {
          assertEquals(j + i, readVector.getAccessor().get(j));
          assertTrue(readVector.getAccessor().isNull(j + 8));
        }
      }
      int loaded = reader.loadNextBatch();
      assertEquals(0, loaded);
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

    Field field = new Field("testField", true, new ArrowType.Int(8, true), Collections.<Field>emptyList());
    NullableTinyIntVector vector = new NullableTinyIntVector("testField", alloc);
    Schema schema = new Schema(asList(field));

    // Try an empty stream, just the header.
    testEchoServer(serverPort, field, vector, 0);

    // Try with one batch.
    testEchoServer(serverPort, field, vector, 1);

    // Try with a few
    testEchoServer(serverPort, field, vector, 10);

    server.close();
    serverThread.join();
  }
}
