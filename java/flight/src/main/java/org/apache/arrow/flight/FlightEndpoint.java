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

package org.apache.arrow.flight;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.impl.Flight;

import com.google.common.collect.ImmutableList;

/**
 * POJO to convert to/from the underlying protobuf FlighEndpoint.
 */
public class FlightEndpoint {
  private List<Location> locations;
  private Ticket ticket;

  /**
   * Constructs a new instance.
   *
   * @param ticket A ticket that describe the key of a data stream.
   * @param locations  The possible locations the stream can be retrieved from.
   */
  public FlightEndpoint(Ticket ticket, Location... locations) {
    super();
    this.locations = ImmutableList.copyOf(locations);
    this.ticket = ticket;
  }

  /**
   * Constructs from the protocol buffer representation.
   */
  public FlightEndpoint(Flight.FlightEndpoint flt) {
    locations = flt.getLocationList().stream()
        .map(t -> new Location(t)).collect(Collectors.toList());
    ticket = new Ticket(flt.getTicket());
  }

  public List<Location> getLocations() {
    return locations;
  }

  public Ticket getTicket() {
    return ticket;
  }

  /**
   * Converts to the protocol buffer representation.
   */
  Flight.FlightEndpoint toProtocol() {
    Flight.FlightEndpoint.Builder b = Flight.FlightEndpoint.newBuilder()
        .setTicket(ticket.toProtocol());

    for (Location l : locations) {
      b.addLocation(Flight.Location.newBuilder()
          .setHost(l.getHost())
          .setPort(l.getPort())
          .build());
    }
    return b.build();
  }
}
