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

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

public class TestLargeMessage {
  /**
   * Make sure a Flight client accepts large message payloads by default.
   */
  @Test
  public void getLargeMessage() throws Exception {
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
         final Producer producer = new Producer(a);
         final FlightServer s =
             FlightTestUtil.getStartedServer((port) -> new FlightServer(a, port, producer, ServerAuthHandler.NO_OP))) {

      try (FlightClient client = new FlightClient(a, new Location(FlightTestUtil.LOCALHOST, s.getPort()))) {
        FlightStream stream = client.getStream(new Ticket(new byte[]{}));
        try (VectorSchemaRoot root = stream.getRoot()) {
          while (stream.next()) {
            for (final Field field : root.getSchema().getFields()) {
              int value = 0;
              final IntVector iv = (IntVector) root.getVector(field.getName());
              for (int i = 0; i < root.getRowCount(); i++) {
                Assert.assertEquals(value, iv.get(i));
                value++;
              }
            }
          }
        }
        stream.close();
      }
    }
  }

  /**
   * Make sure a Flight server accepts large message payloads by default.
   */
  @Test
  public void putLargeMessage() throws Exception {
    try (final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
         final Producer producer = new Producer(a);
         final FlightServer s =
             FlightTestUtil.getStartedServer((port) -> new FlightServer(a, port, producer, ServerAuthHandler.NO_OP))) {

      try (FlightClient client = new FlightClient(a, new Location(FlightTestUtil.LOCALHOST, s.getPort()));
           BufferAllocator testAllocator = a.newChildAllocator("testcase", 0, Long.MAX_VALUE);
           VectorSchemaRoot root = generateData(testAllocator)) {
        final FlightClient.ClientStreamListener listener = client.startPut(FlightDescriptor.path("hello"), root);
        listener.putNext();
        listener.completed();
        Assert.assertEquals(listener.getResult(), Flight.PutResult.getDefaultInstance());
      }
    }
  }

  private static VectorSchemaRoot generateData(BufferAllocator allocator) {
    final int size = 128 * 1024;
    final List<String> fieldNames = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10");
    final Stream<Field> fields = fieldNames
        .stream()
        .map(fieldName -> new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null));
    final Schema schema = new Schema(fields::iterator, null);

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
    public void getStream(Ticket ticket, ServerStreamListener listener) {
      try (VectorSchemaRoot root = generateData(allocator)) {
        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public void listFlights(Criteria criteria, StreamListener<FlightInfo> listener) {

    }

    @Override
    public FlightInfo getFlightInfo(FlightDescriptor descriptor) {
      return null;
    }

    @Override
    public Callable<Flight.PutResult> acceptPut(FlightStream flightStream) {
      return () -> {
        try (VectorSchemaRoot root = flightStream.getRoot()) {
          while (flightStream.next()) {
            ;
          }
          return Flight.PutResult.getDefaultInstance();
        }
      };
    }

    @Override
    public Result doAction(Action action) {
      return null;
    }

    @Override
    public void listActions(StreamListener<ActionType> listener) {

    }

    @Override
    public void close() throws Exception {
      allocator.close();
    }
  }
}
