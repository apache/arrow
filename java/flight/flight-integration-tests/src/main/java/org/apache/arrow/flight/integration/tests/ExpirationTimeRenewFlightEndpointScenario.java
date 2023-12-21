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
import java.time.Instant;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
import org.apache.arrow.memory.BufferAllocator;

/** Test RenewFlightEndpoint. */
final class ExpirationTimeRenewFlightEndpointScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new ExpirationTimeProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    FlightInfo info = client.getInfo(FlightDescriptor.command("expiration".getBytes(StandardCharsets.UTF_8)));

    // Renew all endpoints with expiration time
    for (FlightEndpoint endpoint : info.getEndpoints()) {
      if (!endpoint.getExpirationTime().isPresent()) {
        continue;
      }
      Instant expiration = endpoint.getExpirationTime().get();
      FlightEndpoint renewed = client.renewFlightEndpoint(new RenewFlightEndpointRequest(endpoint));

      IntegrationAssertions.assertTrue("Renewed FlightEndpoint must have expiration time",
          renewed.getExpirationTime().isPresent());
      IntegrationAssertions.assertTrue("Renewed FlightEndpoint must have newer expiration time",
          renewed.getExpirationTime().get().isAfter(expiration));

    }
  }
}
