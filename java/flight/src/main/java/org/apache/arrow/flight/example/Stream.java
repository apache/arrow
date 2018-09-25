/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class Stream implements AutoCloseable, Iterable<ArrowRecordBatch> {

  private final String uuid = UUID.randomUUID().toString();
  private final List<ArrowRecordBatch> batches;
  private final Schema schema;
  private final long recordCount;

  public Stream(
      final Schema schema,
      List<ArrowRecordBatch> batches,
      long recordCount) {
    this.schema = schema;
    this.batches = ImmutableList.copyOf(batches);
    this.recordCount = recordCount;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public Iterator<ArrowRecordBatch> iterator() {
    return batches.iterator();
  }

  public long getRecordCount() {
    return recordCount;
  }

  public String getUuid() {
    return uuid;
  }

  public void sendTo(BufferAllocator allocator, ServerStreamListener listener) {
    try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      listener.start(root);
      final VectorLoader loader = new VectorLoader(root);
      for (ArrowRecordBatch batch : batches) {
        loader.load(batch);
        listener.putNext();
      }
      listener.completed();
    } catch (Exception ex) {
      listener.error(ex);
    }
  }

  public void verify(ExampleTicket ticket) {
    if (!uuid.equals(ticket.getUuid())) {
      throw new IllegalStateException("Ticket doesn't match.");
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(batches);
  }

  public static class StreamCreator {

    private final Schema schema;
    private final BufferAllocator allocator;
    private final List<ArrowRecordBatch> batches = new ArrayList<>();
    private final Consumer<Stream> committer;
    private long recordCount = 0;

    public StreamCreator(Schema schema, BufferAllocator allocator, Consumer<Stream> committer) {
      this.allocator = allocator;
      this.committer = committer;
      this.schema = schema;
    }

    public void drop() {
      try {
        AutoCloseables.close(batches);
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }

    public void add(ArrowRecordBatch batch) {
      batches.add(batch.cloneWithTransfer(allocator));
      recordCount += batch.getLength();
    }

    public void complete() {
      Stream stream = new Stream(schema, batches, recordCount);
      committer.accept(stream);
    }

  }

}
