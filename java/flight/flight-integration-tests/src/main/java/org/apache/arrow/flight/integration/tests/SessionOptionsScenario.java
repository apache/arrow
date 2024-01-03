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

package org.apache.arrow.flight.integration.tests;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.GetSessionOptionsRequest;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.ServerSessionMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Scenario to exercise Session Options functionality.
 */
final class SessionOptionsScenario implements Scenario {
  private final FlightServerMiddleware.Key<ServerSessionMiddleware> key =
      FlightServerMiddleware.Key.of("sessionmiddleware");

  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new SessionOptionsProducer(key);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
    AtomicInteger counter = new AtomicInteger(1000);
    builder.middleware(key, new ServerSessionMiddleware.Factory(() -> String.valueOf(counter.getAndIncrement())));
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient ignored) throws Exception {
    // TODO PHOXME add more interesting cases``

    final ClientCookieMiddleware.Factory factory = new ClientCookieMiddleware.Factory();
    try (final FlightClient flightClient = FlightClient.builder(allocator, location).intercept(factory).build()) {
      final FlightSqlClient client = new FlightSqlClient(flightClient);

      // No existing session yet
      IntegrationAssertions.assertThrows(FlightRuntimeException.class,
          () -> client.getSessionOptions(new GetSessionOptionsRequest()));
    }
  }
}
