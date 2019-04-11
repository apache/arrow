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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.flight.perf.PerformanceTestServer;
import org.apache.arrow.flight.perf.TestPerf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestBackPressure {

  private static final int BATCH_SIZE = 4095;

  /**
   * Make sure that failing to consume one stream doesn't block other streams.
   */
  @Ignore
  @Test
  public void ensureIndependentSteams() throws Exception {
    try (
        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final PerformanceTestServer server = FlightTestUtil.getStartedServer(
            (port) -> (new PerformanceTestServer(a, new Location(FlightTestUtil.LOCALHOST, port))));
        final FlightClient client = new FlightClient(a, server.getLocation())
    ) {
      FlightStream fs1 = client.getStream(client.getInfo(
          TestPerf.getPerfFlightDescriptor(110L * BATCH_SIZE, BATCH_SIZE, 1))
          .getEndpoints().get(0).getTicket());
      consume(fs1, 10);

      // stop consuming fs1 but make sure we can consume a large amount of fs2.
      FlightStream fs2 = client.getStream(client.getInfo(
          TestPerf.getPerfFlightDescriptor(200L * BATCH_SIZE, BATCH_SIZE, 1))
          .getEndpoints().get(0).getTicket());
      consume(fs2, 100);

      consume(fs1, 100);
      consume(fs2, 100);

      consume(fs1);
      consume(fs2);

      fs1.close();
      fs2.close();

    }
  }

  /**
   * Make sure that a stream doesn't go faster than the consumer is consuming.
   */
  @Ignore
  @Test
  public void ensureWaitUntilProceed() throws Exception {
    // request some values.
    final long wait = 3000;
    final long epsilon = 1000;

    AtomicLong sleepTime = new AtomicLong(0);
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      final FlightProducer producer = new NoOpFlightProducer() {

        @Override
        public void getStream(CallContext context, Ticket ticket,
            ServerStreamListener listener) {
          int batches = 0;
          final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a", MinorType.BIGINT.getType())));
          VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator);
          listener.start(root);
          while (true) {
            while (!listener.isReady()) {
              try {
                Thread.sleep(1);
                sleepTime.addAndGet(1L);
              } catch (InterruptedException ignore) {
              }
            }

            if (batches > 100) {
              root.clear();
              listener.completed();
              return;
            }

            root.allocateNew();
            root.setRowCount(4095);
            listener.putNext();
            batches++;
          }
        }
      };


      try (
          BufferAllocator serverAllocator = allocator.newChildAllocator("server", 0, Long.MAX_VALUE);
          FlightServer server =
              FlightTestUtil.getStartedServer(
                  (port) -> new FlightServer(serverAllocator, port, producer, ServerAuthHandler.NO_OP));
          BufferAllocator clientAllocator = allocator.newChildAllocator("client", 0, Long.MAX_VALUE);
          FlightClient client =
              new FlightClient(clientAllocator, new Location(FlightTestUtil.LOCALHOST, server.getPort()))
      ) {
        FlightStream stream = client.getStream(new Ticket(new byte[1]));
        VectorSchemaRoot root = stream.getRoot();
        root.clear();
        Thread.sleep(wait);
        while (stream.next()) {
          root.clear();
        }
        long expected = wait - epsilon;
        Assert.assertTrue(
            String.format("Expected a sleep of at least %dms but only slept for %d", expected, sleepTime.get()),
            sleepTime.get() > expected);

      }
    }
  }

  private static void consume(FlightStream stream) {
    VectorSchemaRoot root = stream.getRoot();
    while (stream.next()) {
      root.clear();
    }
  }

  private static void consume(FlightStream stream, int batches) {
    VectorSchemaRoot root = stream.getRoot();
    while (batches > 0 && stream.next()) {
      root.clear();
      batches--;
    }
  }
}
