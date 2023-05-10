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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

/**
 * Run the integration test scenarios in-process.
 */
class IntegrationTest {
  @Test
  void authBasicProto() throws Exception {
    testScenario("auth:basic_proto");
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
  void flightSql() throws Exception {
    testScenario("flight_sql");
  }

  @Test
  void flightSqlExtension() throws Exception {
    testScenario("flight_sql:extension");
  }

  void testScenario(String scenarioName) throws Exception {
    try (final BufferAllocator allocator = new RootAllocator()) {
      final FlightServer.Builder builder = FlightServer.builder()
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
    }
  }
}
