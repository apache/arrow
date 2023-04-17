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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.arrow.flight.perf.PerformanceTestServer;
import org.apache.arrow.flight.perf.TestPerf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;

public class TestBackPressure {

  private static final int BATCH_SIZE = 4095;

  /**
   * Make sure that failing to consume one stream doesn't block other streams.
   */
  @Disabled
  @Test
  public void ensureIndependentSteams() throws Exception {
    ensureIndependentSteams((b) -> (location -> new PerformanceTestServer(b, location)));
  }

  /**
   * Make sure that failing to consume one stream doesn't block other streams.
   */
  @Disabled
  @Test
  public void ensureIndependentSteamsWithCallbacks() throws Exception {
    ensureIndependentSteams((b) -> (location -> new PerformanceTestServer(b, location,
        new BackpressureStrategy.CallbackBackpressureStrategy(), true)));
  }

  /**
   * Test to make sure stream doesn't go faster than the consumer is consuming.
   */
  @Disabled
  @Test
  public void ensureWaitUntilProceed() throws Exception {
    ensureWaitUntilProceed(new PollingBackpressureStrategy(), false);
  }

  /**
   * Test to make sure stream doesn't go faster than the consumer is consuming using a callback-based
   * backpressure strategy.
   */
  @Disabled
  @Test
  public void ensureWaitUntilProceedWithCallbacks() throws Exception {
    ensureWaitUntilProceed(new RecordingCallbackBackpressureStrategy(), true);
  }

  /**
   * Make sure that failing to consume one stream doesn't block other streams.
   */
  private static void ensureIndependentSteams(Function<BufferAllocator, Function<Location, PerformanceTestServer>>
                                                  serverConstructor) throws Exception {
    try (
        final BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        final PerformanceTestServer server = serverConstructor.apply(a).apply(forGrpcInsecure(LOCALHOST, 0)).start();
        final FlightClient client = FlightClient.builder(a, server.getLocation()).build()
    ) {
      try (FlightStream fs1 = client.getStream(client.getInfo(
          TestPerf.getPerfFlightDescriptor(110L * BATCH_SIZE, BATCH_SIZE, 1))
          .getEndpoints().get(0).getTicket())) {
        consume(fs1, 10);

        // stop consuming fs1 but make sure we can consume a large amount of fs2.
        try (FlightStream fs2 = client.getStream(client.getInfo(
            TestPerf.getPerfFlightDescriptor(200L * BATCH_SIZE, BATCH_SIZE, 1))
            .getEndpoints().get(0).getTicket())) {
          consume(fs2, 100);

          consume(fs1, 100);
          consume(fs2, 100);

          consume(fs1);
          consume(fs2);
        }
      }
    }
  }

  /**
   * Make sure that a stream doesn't go faster than the consumer is consuming.
   */
  private static void ensureWaitUntilProceed(SleepTimeRecordingBackpressureStrategy bpStrategy, boolean isNonBlocking)
      throws Exception {
    // request some values.
    final long wait = 3000;
    final long epsilon = 1000;

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {

      final FlightProducer producer = new NoOpFlightProducer() {

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
          bpStrategy.register(listener);
          final Runnable loadData = () -> {
            int batches = 0;
            final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a", MinorType.BIGINT.getType())));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator)) {
              listener.start(root);
              while (true) {
                bpStrategy.waitForListener(0);
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

          if (!isNonBlocking) {
            loadData.run();
          } else {
            final ExecutorService service = Executors.newSingleThreadExecutor();
            service.submit(loadData);
            service.shutdown();
          }
        }
      };


      try (
          BufferAllocator serverAllocator = allocator.newChildAllocator("server", 0, Long.MAX_VALUE);
          FlightServer server = FlightServer.builder(serverAllocator, forGrpcInsecure(LOCALHOST, 0), producer)
              .build().start();
          BufferAllocator clientAllocator = allocator.newChildAllocator("client", 0, Long.MAX_VALUE);
          FlightClient client =
              FlightClient
                  .builder(clientAllocator, server.getLocation())
                  .build();
          FlightStream stream = client.getStream(new Ticket(new byte[1]))
      ) {
        VectorSchemaRoot root = stream.getRoot();
        root.clear();
        Thread.sleep(wait);
        while (stream.next()) {
          root.clear();
        }
        long expected = wait - epsilon;
        Assertions.assertTrue(
            bpStrategy.getSleepTime() > expected,
            String.format(
                "Expected a sleep of at least %dms but only slept for %d",
                expected,
                bpStrategy.getSleepTime()
            )
        );

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

  private interface SleepTimeRecordingBackpressureStrategy extends BackpressureStrategy {
    /**
     * Returns the total time spent waiting on the listener to be ready.
     * @return the total time spent waiting on the listener to be ready.
     */
    long getSleepTime();
  }

  /**
   * Implementation of a backpressure strategy that polls on isReady and records amount of time spent in Thread.sleep().
   */
  private static class PollingBackpressureStrategy implements SleepTimeRecordingBackpressureStrategy {
    private final AtomicLong sleepTime = new AtomicLong(0);
    private FlightProducer.ServerStreamListener listener;

    @Override
    public long getSleepTime() {
      return sleepTime.get();
    }

    @Override
    public void register(FlightProducer.ServerStreamListener listener) {
      this.listener = listener;
    }

    @Override
    public WaitResult waitForListener(long timeout) {
      while (!listener.isReady()) {
        try {
          Thread.sleep(1);
          sleepTime.addAndGet(1L);
        } catch (InterruptedException ignore) {
        }
      }
      return WaitResult.READY;
    }
  }

  /**
   * Implementation of a backpressure strategy that uses callbacks to detect changes in client readiness state
   * and records spent time waiting.
   */
  private static class RecordingCallbackBackpressureStrategy extends BackpressureStrategy.CallbackBackpressureStrategy
      implements SleepTimeRecordingBackpressureStrategy {
    private final AtomicLong sleepTime = new AtomicLong(0);

    @Override
    public long getSleepTime() {
      return sleepTime.get();
    }

    @Override
    public WaitResult waitForListener(long timeout) {
      final long startTime = System.currentTimeMillis();
      final WaitResult result = super.waitForListener(timeout);
      sleepTime.addAndGet(System.currentTimeMillis() - startTime);
      return result;
    }
  }
}
