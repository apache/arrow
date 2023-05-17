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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Test the 'ordered' flag in FlightInfo. */
public class OrderedScenario implements Scenario {
  private static final Schema SCHEMA =
      new Schema(
          Collections.singletonList(Field.notNullable("number", Types.MinorType.INT.getType())));
  private static final byte[] ORDERED_COMMAND = "ordered".getBytes(StandardCharsets.UTF_8);

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new OrderedProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) throws Exception {}

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    final FlightInfo info = client.getInfo(FlightDescriptor.command(ORDERED_COMMAND));
    IntegrationAssertions.assertTrue("ordered must be true", info.getOrdered());
    IntegrationAssertions.assertEquals(3, info.getEndpoints().size());

    int offset = 0;
    for (int multiplier : Arrays.asList(1, 10, 100)) {
      FlightEndpoint endpoint = info.getEndpoints().get(offset);

      IntegrationAssertions.assertTrue(
          "locations must be empty", endpoint.getLocations().isEmpty());

      try (final FlightStream stream = client.getStream(endpoint.getTicket())) {
        IntegrationAssertions.assertEquals(SCHEMA, stream.getSchema());
        IntegrationAssertions.assertTrue("stream must have a batch", stream.next());

        IntVector number = (IntVector) stream.getRoot().getVector(0);
        IntegrationAssertions.assertEquals(3, stream.getRoot().getRowCount());

        IntegrationAssertions.assertFalse("value must be non-null", number.isNull(0));
        IntegrationAssertions.assertFalse("value must be non-null", number.isNull(1));
        IntegrationAssertions.assertFalse("value must be non-null", number.isNull(2));
        IntegrationAssertions.assertEquals(multiplier, number.get(0));
        IntegrationAssertions.assertEquals(2 * multiplier, number.get(1));
        IntegrationAssertions.assertEquals(3 * multiplier, number.get(2));

        IntegrationAssertions.assertFalse("stream must have one batch", stream.next());
      }

      offset++;
    }
  }

  private static class OrderedProducer extends NoOpFlightProducer {
    private static final byte[] TICKET_1 = "1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TICKET_2 = "2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TICKET_3 = "3".getBytes(StandardCharsets.UTF_8);

    private final BufferAllocator allocator;

    OrderedProducer(BufferAllocator allocator) {
      this.allocator = Objects.requireNonNull(allocator);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
        IntVector number = (IntVector) root.getVector(0);

        if (Arrays.equals(ticket.getBytes(), TICKET_1)) {
          number.setSafe(0, 1);
          number.setSafe(1, 2);
          number.setSafe(2, 3);
        } else if (Arrays.equals(ticket.getBytes(), TICKET_2)) {
          number.setSafe(0, 10);
          number.setSafe(1, 20);
          number.setSafe(2, 30);
        } else if (Arrays.equals(ticket.getBytes(), TICKET_3)) {
          number.setSafe(0, 100);
          number.setSafe(1, 200);
          number.setSafe(2, 300);
        } else {
          listener.error(
              CallStatus.INVALID_ARGUMENT
                  .withDescription(
                      "Could not find flight: " + new String(ticket.getBytes(), StandardCharsets.UTF_8))
                  .toRuntimeException());
          return;
        }

        root.setRowCount(3);

        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
      final boolean ordered =
          descriptor.isCommand() && Arrays.equals(descriptor.getCommand(), ORDERED_COMMAND);
      List<FlightEndpoint> endpoints;
      if (ordered) {
        endpoints =
            Arrays.asList(
                new FlightEndpoint(new Ticket(TICKET_1)),
                new FlightEndpoint(new Ticket(TICKET_2)),
                new FlightEndpoint(new Ticket(TICKET_3)));
      } else {
        endpoints =
            Arrays.asList(
                new FlightEndpoint(new Ticket(TICKET_1)),
                new FlightEndpoint(new Ticket(TICKET_3)),
                new FlightEndpoint(new Ticket(TICKET_2)));
      }
      return new FlightInfo(
          SCHEMA, descriptor, endpoints, /*bytes*/ -1, /*records*/ -1, ordered, IpcOption.DEFAULT);
    }
  }
}
