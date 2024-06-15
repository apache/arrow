/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

public class TestLargeMessage {
  /** Make sure a Flight client accepts large message payloads by default. */
  @Test
  public void getLargeMessage() throws Exception {
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final Producer producer = new Producer(a);
        final FlightServer s =
            FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), producer).build().start()) {

      try (FlightClient client = FlightClient.builder(a, s.getLocation()).build()) {
        try (FlightStream stream = client.getStream(new Ticket(new byte[] {}));
            VectorSchemaRoot root = stream.getRoot()) {
          while (stream.next()) {
            for (final Field field : root.getSchema().getFields()) {
              int value = 0;
              final IntVector iv = (IntVector) root.getVector(field.getName());
              for (int i = 0; i < root.getRowCount(); i++) {
                assertEquals(value, iv.get(i));
                value++;
              }
            }
          }
        }
      }
    }
  }

  /** Make sure a Flight server accepts large message payloads by default. */
  @Test
  public void putLargeMessage() throws Exception {
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final Producer producer = new Producer(a);
        final FlightServer s =
            FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), producer).build().start()) {
      try (FlightClient client = FlightClient.builder(a, s.getLocation()).build();
          BufferAllocator testAllocator = a.newChildAllocator("testcase", 0, Long.MAX_VALUE);
          VectorSchemaRoot root = generateData(testAllocator)) {
        final FlightClient.ClientStreamListener listener =
            client.startPut(FlightDescriptor.path("hello"), root, new AsyncPutListener());
        listener.putNext();
        listener.completed();
        listener.getResult();
      }
    }
  }

  private static VectorSchemaRoot generateData(BufferAllocator allocator) {
    final int size = 128 * 1024;
    final List<String> fieldNames =
        Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10");
    final Stream<Field> fields =
        fieldNames.stream()
            .map(
                fieldName ->
                    new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null));
    final Schema schema = new Schema(fields.collect(toImmutableList()), null);

    final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.allocateNew();
    for (final String fieldName : fieldNames) {
      final IntVector iv = (IntVector) root.getVector(fieldName);
      iv.setValueCount(size);
      for (int i = 0; i < size; i++) {
        iv.set(i, i);
      }
    }
    root.setRowCount(size);
    return root;
  }

  private static class Producer implements FlightProducer, AutoCloseable {
    private final BufferAllocator allocator;

    Producer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try (VectorSchemaRoot root = generateData(allocator)) {
        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void listFlights(
        CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {}

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      return null;
    }

    @Override
    public Runnable acceptPut(
        CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
      return () -> {
        try (VectorSchemaRoot root = flightStream.getRoot()) {
          while (flightStream.next()) {;
          }
        }
      };
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
      listener.onCompleted();
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {}

    @Override
    public void close() throws Exception {
      allocator.close();
    }
  }
}
