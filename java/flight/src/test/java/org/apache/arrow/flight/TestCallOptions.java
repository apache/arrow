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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.arrow.flight.auth.ServerAuthHandler;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;

public class TestCallOptions {

  @Test
  public void timeoutFires() {
    test((client) -> {
      Instant start = Instant.now();
      Iterator<Result> results = client.doAction(new Action("hang"), CallOptions.timeout(1, TimeUnit.SECONDS));
      try {
        results.next();
        Assert.fail("Call should have failed");
      } catch (RuntimeException e) {
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("deadline exceeded"));
      }
      Instant end = Instant.now();
      Assert.assertTrue("Call took over 1500 ms despite timeout", Duration.between(start, end).toMillis() < 1500);
    });
  }

  @Test
  public void underTimeout() {
    test((client) -> {
      Instant start = Instant.now();
      // This shouldn't fail and it should complete within the timeout
      Iterator<Result> results = client.doAction(new Action("fast"), CallOptions.timeout(2, TimeUnit.SECONDS));
      Assert.assertArrayEquals(new byte[]{42, 42}, results.next().getBody());
      Instant end = Instant.now();
      Assert.assertTrue("Call took over 2500 ms despite timeout", Duration.between(start, end).toMillis() < 2500);
    });
  }

  void test(Consumer<FlightClient> testFn) {
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s =
            FlightTestUtil.getStartedServer((port) -> new FlightServer(a, port, producer, ServerAuthHandler.NO_OP));
        FlightClient client = new FlightClient(a, new Location(FlightTestUtil.LOCALHOST, s.getPort()))) {
      testFn.accept(client);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class Producer extends NoOpFlightProducer implements AutoCloseable {

    private final BufferAllocator allocator;

    Producer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void close() {
    }

    @Override
    public Result doAction(CallContext context, Action action) {
      switch (action.getType()) {
        case "hang": {
          try {
            Thread.sleep(25000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return new Result(new byte[]{});
        }
        case "fast": {
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          return new Result(new byte[]{42, 42});
        }
        default: {
          throw new UnsupportedOperationException(action.getType());
        }
      }
    }
  }
}
