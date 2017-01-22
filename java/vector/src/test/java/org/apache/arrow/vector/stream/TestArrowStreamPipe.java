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

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import io.netty.buffer.ArrowBuf;

public class TestArrowStreamPipe {
  Schema schema = MessageSerializerTest.testSchema();
  // second half is "undefined"
  byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

  private final class WriterThread extends Thread {
    private final int numBatches;
    private final ArrowStreamWriter writer;

    public WriterThread(int numBatches, WritableByteChannel sinkChannel)
        throws IOException {
      this.numBatches = numBatches;
      writer = new ArrowStreamWriter(sinkChannel, schema);
    }

    @Override
    public void run() {
      BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
      try {
        ArrowBuf valuesb =  MessageSerializerTest.buf(alloc, values);
        for (int i = 0; i < numBatches; i++) {
          // Send a changing byte id first.
          byte[] validity = new byte[] { (byte)i, 0};
          ArrowBuf validityb = MessageSerializerTest.buf(alloc, validity);
          writer.writeRecordBatch(new ArrowRecordBatch(
              16, asList(new ArrowFieldNode(16, 8)), asList(validityb, valuesb)));
        }
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
        assertTrue(false);
      }
    }

    public long bytesWritten() { return writer.bytesWritten(); }
  }

  private final class ReaderThread extends Thread {
    private int batchesRead = 0;
    private final ArrowStreamReader reader;
    private final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);

    public ReaderThread(ReadableByteChannel sourceChannel)
        throws IOException {
      reader = new ArrowStreamReader(sourceChannel, alloc);
    }

    @Override
    public void run() {
      try {
        reader.init();
        assertEquals(schema, reader.getSchema());
        assertTrue(
            reader.getSchema().getFields().get(0).getTypeLayout().getVectorTypes().toString(),
            reader.getSchema().getFields().get(0).getTypeLayout().getVectors().size() > 0);

        // Read all the batches. Each batch contains an incrementing id and then some
        // constant data. Verify both.
        while (true) {
          ArrowRecordBatch batch = reader.nextRecordBatch();
          if (batch == null) break;
          byte[] validity = new byte[] { (byte)batchesRead, 0};
          MessageSerializerTest.verifyBatch(batch, validity, values);
          batchesRead++;
        }
      } catch (IOException e) {
        e.printStackTrace();
        assertTrue(false);
      }
    }

    public int getBatchesRead() { return batchesRead; }
    public long bytesRead() { return reader.bytesRead(); }
  }

  // Starts up a producer and consumer thread to read/write batches.
  @Test
  public void pipeTest() throws IOException, InterruptedException {
    int NUM_BATCHES = 1000;
    Pipe pipe = Pipe.open();
    WriterThread writer = new WriterThread(NUM_BATCHES, pipe.sink());
    ReaderThread reader = new ReaderThread(pipe.source());

    writer.start();
    reader.start();
    reader.join();
    writer.join();

    assertEquals(NUM_BATCHES, reader.getBatchesRead());
    assertEquals(writer.bytesWritten(), reader.bytesRead());
  }
}
