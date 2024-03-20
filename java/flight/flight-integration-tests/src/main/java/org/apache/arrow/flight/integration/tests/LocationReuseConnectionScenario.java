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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/** Test the 'arrow-flight-reuse-connection' scheme. */
public class LocationReuseConnectionScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new ReuseConnectionProducer();
  }

  @Override
  public void buildServer(FlightServer.Builder builder) throws Exception {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    final FlightInfo info = client.getInfo(FlightDescriptor.command("reuse".getBytes(StandardCharsets.UTF_8)));
    IntegrationAssertions.assertEquals(1, info.getEndpoints().size());
    IntegrationAssertions.assertEquals(1, info.getEndpoints().get(0).getLocations().size());
    Location actual = info.getEndpoints().get(0).getLocations().get(0);
    IntegrationAssertions.assertEquals(Location.reuseConnection().getUri(), actual.getUri());
  }

  private static class ReuseConnectionProducer extends NoOpFlightProducer {
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      List<FlightEndpoint> endpoints = Collections.singletonList(
              new FlightEndpoint(new Ticket(new byte[0]), Location.reuseConnection()));
      return new FlightInfo(
          new Schema(Collections.emptyList()), descriptor, endpoints, /*bytes*/ -1, /*records*/ -1);
    }
  }
}
