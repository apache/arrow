/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

public class FlightEndpoint {
  private List<Location> locations;
  private Ticket ticket;

  public FlightEndpoint(Ticket ticket, Location... locations) {
    super();
    this.locations = ImmutableList.copyOf(locations);
    this.ticket = ticket;
  }

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
