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

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Scenarios for integration testing.
 */
final class Scenarios {

  private static Scenarios INSTANCE;

  private final Map<String, Supplier<Scenario>> scenarios;

  private Scenarios() {
    scenarios = new TreeMap<>();
    scenarios.put("auth:basic_proto", AuthBasicProtoScenario::new);
    scenarios.put("middleware", MiddlewareScenario::new);
    scenarios.put("ordered", OrderedScenario::new);
    scenarios.put("flight_sql", FlightSqlScenario::new);
    scenarios.put("flight_sql:extension", FlightSqlExtensionScenario::new);
  }

  private static Scenarios getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new Scenarios();
    }
    return INSTANCE;
  }

  static Scenario getScenario(String scenario) {
    final Supplier<Scenario> ctor = getInstance().scenarios.get(scenario);
    if (ctor == null) {
      throw new IllegalArgumentException("Unknown integration test scenario: " + scenario);
    }
    return ctor.get();
  }

  // Utility methods for implementing tests.

  public static void main(String[] args) {
    // Run scenarios one after the other
    final Location location = Location.forGrpcInsecure("localhost", 31337);
    for (final Map.Entry<String, Supplier<Scenario>> entry : getInstance().scenarios.entrySet()) {
      System.out.println("Running test scenario: " + entry.getKey());
      final Scenario scenario = entry.getValue().get();
      try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
        final FlightServer.Builder builder = FlightServer
            .builder(allocator, location, scenario.producer(allocator, location));
        scenario.buildServer(builder);
        try (final FlightServer server = builder.build()) {
          server.start();

          try (final FlightClient client = FlightClient.builder(allocator, location).build()) {
            scenario.client(allocator, location, client);
          }

          server.shutdown();
          server.awaitTermination(1, TimeUnit.SECONDS);
          System.out.println("Ran scenario " + entry.getKey());
        }
      } catch (Exception e) {
        System.out.println("Exception while running scenario " + entry.getKey());
        e.printStackTrace();
      }
    }
  }
}
