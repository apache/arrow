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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.CancelFlightInfoRequest;
import org.apache.arrow.flight.CancelFlightInfoResult;
import org.apache.arrow.flight.CancelStatus;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RenewFlightEndpointRequest;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** The server used for testing FlightEndpoint.expiration_time.
 * <p>
 * GetFlightInfo() returns a FlightInfo that has the following
 * three FlightEndpoints:
 *
 * <ol>
 * <li>No expiration time</li>
 * <li>5 seconds expiration time</li>
 * <li>6 seconds expiration time</li>
 * </ol>
 *
 * The client can't read data from the first endpoint multiple times
 * but can read data from the second and third endpoints. The client
 * can't re-read data from the second endpoint 5 seconds later. The
 * client can't re-read data from the third endpoint 6 seconds
 * later.
 * <p>
 * The client can cancel a returned FlightInfo by pre-defined
 * CancelFlightInfo action. The client can't read data from endpoints
 * even within 6 seconds after the action.
 * <p>
 * The client can extend the expiration time of a FlightEndpoint in
 * a returned FlightInfo by pre-defined RenewFlightEndpoint
 * action. The client can read data from endpoints multiple times
 * within more 10 seconds after the action.
 */
final class ExpirationTimeProducer extends NoOpFlightProducer {
  public static final Schema SCHEMA = new Schema(
      Collections.singletonList(Field.notNullable("number", Types.MinorType.UINT4.getType())));

  private final BufferAllocator allocator;
  private final List<EndpointStatus> statuses;

  ExpirationTimeProducer(BufferAllocator allocator) {
    this.allocator = allocator;
    this.statuses = new ArrayList<>();
  }

  @Override
  public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
    statuses.clear();
    List<FlightEndpoint> endpoints = new ArrayList<>();
    Instant now = Instant.now();
    endpoints.add(addEndpoint("No expiration time", null));
    endpoints.add(addEndpoint("5 seconds", now.plus(5, ChronoUnit.SECONDS)));
    endpoints.add(addEndpoint("6 seconds", now.plus(6, ChronoUnit.SECONDS)));
    return new FlightInfo(SCHEMA, descriptor, endpoints, -1, -1);
  }

  @Override
  public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
    // Obviously, not safe (since we don't lock), but we assume calls are not concurrent
    int index = parseIndexFromTicket(ticket);
    EndpointStatus status = statuses.get(index);
    if (status.cancelled) {
      listener.error(CallStatus.NOT_FOUND
          .withDescription("Invalid flight: cancelled: " +
                           new String(ticket.getBytes(), StandardCharsets.UTF_8))
          .toRuntimeException());
      return;
    } else if (status.expirationTime != null && Instant.now().isAfter(status.expirationTime)) {
      listener.error(CallStatus.NOT_FOUND
          .withDescription("Invalid flight: expired: " +
                           new String(ticket.getBytes(), StandardCharsets.UTF_8))
          .toRuntimeException());
      return;
    } else if (status.expirationTime == null && status.numGets > 0) {
      listener.error(CallStatus.NOT_FOUND
          .withDescription("Invalid flight: can't read multiple times: " +
                           new String(ticket.getBytes(), StandardCharsets.UTF_8))
          .toRuntimeException());
      return;
    }
    status.numGets++;

    try (final VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
      listener.start(root);
      UInt4Vector vector = (UInt4Vector) root.getVector(0);
      vector.setSafe(0, index);
      root.setRowCount(1);
      listener.putNext();
    }
    listener.completed();
  }

  @Override
  public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
    try {
      if (action.getType().equals(FlightConstants.CANCEL_FLIGHT_INFO.getType())) {
        CancelFlightInfoRequest request = CancelFlightInfoRequest.deserialize(ByteBuffer.wrap(action.getBody()));
        CancelStatus cancelStatus = CancelStatus.UNSPECIFIED;
        for (FlightEndpoint endpoint : request.getInfo().getEndpoints()) {
          int index = parseIndexFromTicket(endpoint.getTicket());
          EndpointStatus status = statuses.get(index);
          if (status.cancelled) {
            cancelStatus = CancelStatus.NOT_CANCELLABLE;
          } else {
            status.cancelled = true;
            if (cancelStatus == CancelStatus.UNSPECIFIED) {
              cancelStatus = CancelStatus.CANCELLED;
            }
          }
        }
        listener.onNext(new Result(new CancelFlightInfoResult(cancelStatus).serialize().array()));
      } else if (action.getType().equals(FlightConstants.RENEW_FLIGHT_ENDPOINT.getType())) {
        RenewFlightEndpointRequest request = RenewFlightEndpointRequest.deserialize(ByteBuffer.wrap(action.getBody()));
        FlightEndpoint endpoint = request.getFlightEndpoint();
        int index = parseIndexFromTicket(endpoint.getTicket());
        EndpointStatus status = statuses.get(index);
        if (status.cancelled) {
          listener.onError(CallStatus.INVALID_ARGUMENT
              .withDescription("Invalid flight: cancelled: " + index)
              .toRuntimeException());
          return;
        }

        String ticketBody = new String(endpoint.getTicket().getBytes(), StandardCharsets.UTF_8);
        ticketBody += ": renewed (+ 10 seconds)";
        Ticket ticket = new Ticket(ticketBody.getBytes(StandardCharsets.UTF_8));
        Instant expiration = Instant.now().plus(10, ChronoUnit.SECONDS);
        status.expirationTime = expiration;
        FlightEndpoint newEndpoint = new FlightEndpoint(
            ticket, expiration, endpoint.getLocations().toArray(new Location[0]));
        listener.onNext(new Result(newEndpoint.serialize().array()));
      } else {
        listener.onError(CallStatus.INVALID_ARGUMENT
            .withDescription("Unknown action: " + action.getType())
            .toRuntimeException());
        return;
      }
    } catch (IOException | URISyntaxException e) {
      listener.onError(CallStatus.INTERNAL.withCause(e).withDescription(e.toString()).toRuntimeException());
      return;
    }
    listener.onCompleted();
  }

  @Override
  public void listActions(CallContext context, StreamListener<ActionType> listener) {
    listener.onNext(FlightConstants.CANCEL_FLIGHT_INFO);
    listener.onNext(FlightConstants.RENEW_FLIGHT_ENDPOINT);
    listener.onCompleted();
  }

  private FlightEndpoint addEndpoint(String ticket, Instant expirationTime) {
    Ticket flightTicket = new Ticket(String.format("%d: %s", statuses.size(), ticket).getBytes(StandardCharsets.UTF_8));
    statuses.add(new EndpointStatus(expirationTime));
    return new FlightEndpoint(flightTicket, expirationTime);
  }

  private int parseIndexFromTicket(Ticket ticket) {
    final String contents = new String(ticket.getBytes(), StandardCharsets.UTF_8);
    int index = contents.indexOf(':');
    if (index == -1) {
      throw CallStatus.INVALID_ARGUMENT
          .withDescription("Invalid ticket: " +
                           new String(ticket.getBytes(), StandardCharsets.UTF_8))
          .toRuntimeException();
    }
    int endpointIndex = Integer.parseInt(contents.substring(0, index));
    if (endpointIndex < 0 || endpointIndex >= statuses.size()) {
      throw CallStatus.NOT_FOUND.withDescription("Out of bounds").toRuntimeException();
    }
    return endpointIndex;
  }

  /** The status of a returned endpoint. */
  static final class EndpointStatus {
    Instant expirationTime;
    int numGets;
    boolean cancelled;

    EndpointStatus(Instant expirationTime) {
      this.expirationTime = expirationTime;
      this.numGets = 0;
      this.cancelled = false;
    }
  }
}
