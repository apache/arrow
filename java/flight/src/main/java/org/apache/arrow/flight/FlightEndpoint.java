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

import org.apache.arrow.flight.impl.Flight;

public class FlightEndpoint {
  private Location location;
  private Ticket ticket;

  public FlightEndpoint(Location location, Ticket ticket) {
    super();
    this.location = location;
    this.ticket = ticket;
  }

  public FlightEndpoint(Flight.FlightEndpoint flt) {
    location = new Location(flt.getLocation());
    ticket = new Ticket(flt.getTicket());
  }

  public Location getLocation() {
    return location;
  }

  public Ticket getTicket() {
    return ticket;
  }

  Flight.FlightEndpoint toProtocol(){
    return Flight.FlightEndpoint.newBuilder()
        .setLocation(location.toProtocol())
        .setTicket(ticket.toProtocol())
        .build();
  }
}
