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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.stream.MessageSerializerTest;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestArrowStreamPipe {
  Schema schema = MessageSerializerTest.testSchema();
  // second half is "undefined"
  byte[] values = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);

  private final class WriterThread extends Thread {

    private final int numBatches;
    private final ArrowStreamWriter writer;

    public WriterThread(int numBatches, WritableByteChannel sinkChannel)
        throws IOException {
      this.numBatches = numBatches;
      BufferAllocator allocator = alloc.newChildAllocator("writer thread", 0, Integer.MAX_VALUE);
      List<FieldVector> vectors = new ArrayList<>();
      for (Field field : schema.getFields()) {
        MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
        vector.initializeChildrenFromFields(field.getChildren());
        vectors.add(vector);
      }
      writer = new ArrowStreamWriter(schema.getFields(), vectors, sinkChannel);
    }

    @Override
    public void run() {
      try {
        writer.start();
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
        Assert.fail(e.toString()); // have to explicitly fail since we're in a separate thread
      }
    }

    public long bytesWritten() { return writer.bytesWritten(); }
  }

  private final class ReaderThread extends Thread {
    private int batchesRead = 0;
    private final ArrowStreamReader reader;
    private final BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    private boolean done = false;

    public ReaderThread(ReadableByteChannel sourceChannel)
        throws IOException {
      reader = new ArrowStreamReader(sourceChannel, alloc) {
        @Override
        protected ArrowMessage readMessage(ReadChannel in, BufferAllocator allocator) throws IOException {
          ArrowMessage message = super.readMessage(in, allocator);
          if (message == null) {
            done = true;
          } else {
            byte[] validity = new byte[] {(byte) batchesRead, 0};
            MessageSerializerTest.verifyBatch((ArrowRecordBatch) message, validity, values);
            batchesRead++;
          }
          return message;
        }
        @Override
        public int loadNextBatch() throws IOException {
          // the batches being sent aren't valid so the decoding fails... catch and suppress
          try {
            return super.loadNextBatch();
          } catch (Exception e) {
            return 0;
          }
        }
      };
    }

    @Override
    public void run() {
      try {
        assertEquals(schema, reader.getSchema());
        assertTrue(
            reader.getSchema().getFields().get(0).getTypeLayout().getVectorTypes().toString(),
            reader.getSchema().getFields().get(0).getTypeLayout().getVectors().size() > 0);

        // Read all the batches. Each batch contains an incrementing id and then some
        // constant data. Verify both.
        while (!done) {
          reader.loadNextBatch();
        }
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.toString()); // have to explicitly fail since we're in a separate thread
      }
    }

    public int getBatchesRead() { return batchesRead; }
    public long bytesRead() { return reader.bytesRead(); }
  }

  // Starts up a producer and consumer thread to read/write batches.
  @Test
  public void pipeTest() throws IOException, InterruptedException {
    int NUM_BATCHES = 10;
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
