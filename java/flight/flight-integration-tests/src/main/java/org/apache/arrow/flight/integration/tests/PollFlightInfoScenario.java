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
import java.util.Optional;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.memory.BufferAllocator;

/** Test PollFlightInfo. */
final class PollFlightInfoScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new PollFlightInfoProducer();
  }

  @Override
  public void buildServer(FlightServer.Builder builder) throws Exception {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    PollInfo info = client.pollInfo(FlightDescriptor.command("heavy query".getBytes(StandardCharsets.UTF_8)));
    IntegrationAssertions.assertNotNull(info.getFlightInfo());
    Optional<Double> progress = info.getProgress();
    IntegrationAssertions.assertTrue("progress is missing", progress.isPresent());
    IntegrationAssertions.assertTrue("progress is invalid", progress.get() >= 0.0 && progress.get() <= 1.0);
    IntegrationAssertions.assertTrue("expiration is missing", info.getExpirationTime().isPresent());
    IntegrationAssertions.assertTrue("descriptor is missing",
        info.getFlightDescriptor().isPresent());

    info = client.pollInfo(info.getFlightDescriptor().get());
    IntegrationAssertions.assertNotNull(info.getFlightInfo());
    progress = info.getProgress();
    IntegrationAssertions.assertTrue("progress is missing in finished query", progress.isPresent());
    IntegrationAssertions.assertTrue("progress isn't 1.0 in finished query",
        Math.abs(progress.get() - 1.0) < Math.ulp(1.0));
    IntegrationAssertions.assertFalse("expiration is set in finished query", info.getExpirationTime().isPresent());
    IntegrationAssertions.assertFalse("descriptor is set in finished query", info.getFlightDescriptor().isPresent());
  }
}
