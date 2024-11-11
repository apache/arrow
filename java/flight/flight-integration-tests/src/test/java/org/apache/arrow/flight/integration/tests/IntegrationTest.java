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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/** Run the integration test scenarios in-process. */
class IntegrationTest {
  @Test
  void authBasicProto() throws Exception {
    testScenario("auth:basic_proto");
  }

  @Test
  void expirationTimeCancelFlightInfo() throws Exception {
    testScenario("expiration_time:cancel_flight_info");
  }

  @Test
  void expirationTimeDoGet() throws Exception {
    testScenario("expiration_time:do_get");
  }

  @Test
  void expirationTimeListActions() throws Exception {
    testScenario("expiration_time:list_actions");
  }

  @Test
  void expirationTimeRenewFlightEndpoint() throws Exception {
    testScenario("expiration_time:renew_flight_endpoint");
  }

  @Test
  void locationReuseConnection() throws Exception {
    testScenario("location:reuse_connection");
  }

  @Test
  void middleware() throws Exception {
    testScenario("middleware");
  }

  @Test
  void ordered() throws Exception {
    testScenario("ordered");
  }

  @Test
  void pollFlightInfo() throws Exception {
    testScenario("poll_flight_info");
  }

  @Test
  void flightSql() throws Exception {
    testScenario("flight_sql");
  }

  @Test
  void flightSqlExtension() throws Exception {
    testScenario("flight_sql:extension");
  }

  @Test
  void flightSqlIngestion() throws Exception {
    testScenario("flight_sql:ingestion");
  }

  @Test
  void appMetadataFlightInfoEndpoint() throws Exception {
    testScenario("app_metadata_flight_info_endpoint");
  }

  @Test
  void sessionOptions() throws Exception {
    testScenario("session_options");
  }

  @Test
  void doExchangeEcho() throws Exception {
    testScenario("do_exchange:echo");
  }

  void testScenario(String scenarioName) throws Exception {
    TestBufferAllocationListener listener = new TestBufferAllocationListener();
    try (final BufferAllocator allocator = new RootAllocator(listener, Long.MAX_VALUE)) {
      final ExecutorService exec =
          Executors.newCachedThreadPool(
              new ThreadFactoryBuilder()
                  .setNameFormat("integration-test-flight-server-executor-%d")
                  .build());
      final FlightServer.Builder builder =
          FlightServer.builder()
              .executor(exec)
              .allocator(allocator)
              .location(Location.forGrpcInsecure("0.0.0.0", 0));
      final Scenario scenario = Scenarios.getScenario(scenarioName);
      scenario.buildServer(builder);
      builder.producer(scenario.producer(allocator, Location.forGrpcInsecure("0.0.0.0", 0)));

      try (final FlightServer server = builder.build()) {
        server.start();

        final Location location = Location.forGrpcInsecure("localhost", server.getPort());
        try (final FlightClient client = FlightClient.builder(allocator, location).build()) {
          scenario.client(allocator, location, client);
        }
      }

      // Shutdown the executor while allowing existing tasks to finish.
      // Without this wait, allocator.close() may get invoked earlier than an executor thread may
      // have finished freeing up resources
      // In that case, allocator.close() can throw an IllegalStateException for memory leak, leading
      // to flaky tests
      exec.shutdown();
      final boolean unused = exec.awaitTermination(3, TimeUnit.SECONDS);
    } catch (IllegalStateException e) {
      // this could be due to Allocator detecting memory leak. Add allocation trail to help debug
      listener.reThrowWithAddedAllocatorInfo(e);
    }
  }
}
