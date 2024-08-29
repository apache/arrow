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
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Test app_metadata in FlightInfo and FlightEndpoint. */
final class AppMetadataFlightInfoEndpointScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new AppMetadataFlightInfoEndpointProducer();
  }

  @Override
  public void buildServer(FlightServer.Builder builder) throws Exception {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    byte[] cmd = "foobar".getBytes(StandardCharsets.UTF_8);
    FlightInfo info = client.getInfo(FlightDescriptor.command(cmd));
    IntegrationAssertions.assertEquals(info.getAppMetadata(), cmd);
    IntegrationAssertions.assertEquals(info.getEndpoints().size(), 1);
    IntegrationAssertions.assertEquals(info.getEndpoints().get(0).getAppMetadata(), cmd);
  }

  /** producer for app_metadata test. */
  static class AppMetadataFlightInfoEndpointProducer extends NoOpFlightProducer {
    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      byte[] cmd = descriptor.getCommand();
      
      Schema schema = new Schema(
              Collections.singletonList(Field.notNullable("number", Types.MinorType.UINT4.getType())));

      List<FlightEndpoint> endpoints = Collections.singletonList(
              FlightEndpoint.builder(
                      new Ticket("".getBytes(StandardCharsets.UTF_8))).setAppMetadata(cmd).build());

      return FlightInfo.builder(schema, descriptor, endpoints).setAppMetadata(cmd).build();
    }
  }
}


