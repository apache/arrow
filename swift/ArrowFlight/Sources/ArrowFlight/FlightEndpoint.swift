// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Foundation
public class FlightEndpoint {
    let ticket: FlightTicket
    let locations: [FlightLocation]
    init(_ endpoint: Arrow_Flight_Protocol_FlightEndpoint) {
        self.ticket = FlightTicket(endpoint.ticket.ticket)
        self.locations = endpoint.location.map {return FlightLocation($0)}
    }

    public init(_ ticket: FlightTicket, locations: [FlightLocation]) {
        self.ticket = ticket
        self.locations = locations
    }

    func toProtocol() -> Arrow_Flight_Protocol_FlightEndpoint {
        var endpoint = Arrow_Flight_Protocol_FlightEndpoint()
        endpoint.ticket = self.ticket.toProtocol()
        endpoint.location = self.locations.map { $0.toProtocol() }
        return endpoint
    }
}
