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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.NullableTinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.vector.stream.ArrowStreamWriter;
import org.apache.arrow.vector.stream.MessageSerializerTest;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestArrowStreamPipe {
  Schema schema = MessageSerializerTest.testSchema();
  BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);

  private final class WriterThread extends Thread {

    private final int numBatches;
    private final ArrowStreamWriter writer;
    private final VectorSchemaRoot root;

    public WriterThread(int numBatches, WritableByteChannel sinkChannel)
        throws IOException {
      this.numBatches = numBatches;
      BufferAllocator allocator = alloc.newChildAllocator("writer thread", 0, Integer.MAX_VALUE);
      root = VectorSchemaRoot.create(schema, allocator);
      writer = new ArrowStreamWriter(root, null, sinkChannel);
    }

    @Override
    public void run() {
      try {
        writer.start();
        for (int j = 0; j < numBatches; j++) {
          root.getFieldVectors().get(0).allocateNew();
          NullableTinyIntVector vector = (NullableTinyIntVector) root.getFieldVectors().get(0);
          // Send a changing batch id first
          vector.set(0, j);
          for (int i = 1; i < 16; i++) {
            vector.set(i, i < 8 ? 1 : 0, (byte) (i + 1));
          }
          vector.setValueCount(16);
          root.setRowCount(16);

          writer.writeBatch();
        }
        writer.close();
        root.close();
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.toString()); // have to explicitly fail since we're in a separate thread
      }
    }

    public long bytesWritten() {
      return writer.bytesWritten();
    }
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
          // Read all the batches. Each batch contains an incrementing id and then some
          // constant data. Verify both.
          ArrowMessage message = super.readMessage(in, allocator);
          if (message == null) {
            done = true;
          } else {
            batchesRead++;
          }
          return message;
        }

        @Override
        public boolean loadNextBatch() throws IOException {
          if (!super.loadNextBatch()) {
            return false;
          }
          if (!done) {
            VectorSchemaRoot root = getVectorSchemaRoot();
            Assert.assertEquals(16, root.getRowCount());
            NullableTinyIntVector vector = (NullableTinyIntVector) root.getFieldVectors().get(0);
            Assert.assertEquals((byte) (batchesRead - 1), vector.get(0));
            for (int i = 1; i < 16; i++) {
              if (i < 8) {
                Assert.assertEquals((byte) (i + 1), vector.get(i));
              } else {
                Assert.assertTrue(vector.isNull(i));
              }
            }
          }
          return true;
        }
      };
    }

    @Override
    public void run() {
      try {
        assertEquals(schema, reader.getVectorSchemaRoot().getSchema());
        assertTrue(
            reader.getVectorSchemaRoot().getSchema().getFields().get(0).getTypeLayout().getVectorTypes().toString(),
            reader.getVectorSchemaRoot().getSchema().getFields().get(0).getTypeLayout().getVectors().size() > 0);
        while (!done) {
          assertTrue(reader.loadNextBatch());
        }
        reader.close();
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail(e.toString()); // have to explicitly fail since we're in a separate thread
      }
    }

    public int getBatchesRead() {
      return batchesRead;
    }

    public long bytesRead() {
      return reader.bytesRead();
    }
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
