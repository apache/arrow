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
import Arrow

public class FlightInfo {
    let flight_info: Arrow_Flight_Protocol_FlightInfo
    public var flightDescriptor: FlightDescriptor? {
        get { return flight_info.hasFlightDescriptor ? FlightDescriptor(flight_info.flightDescriptor) : nil }
    }
    
    public var endpoints: [FlightEndpoint] {
        return self.flight_info.endpoint.map { FlightEndpoint($0) }
    }
    public var schema: Data { flight_info.schema }
    
    var endpoint: [Arrow_Flight_Protocol_FlightEndpoint] = []
    init(_ flight_info: Arrow_Flight_Protocol_FlightInfo) {
        self.flight_info = flight_info
    }
    
    public init(_ schema: Data, endpoints: [FlightEndpoint] = [FlightEndpoint](), descriptor: FlightDescriptor? = nil) {
        if let localDescriptor = descriptor {
            self.flight_info = Arrow_Flight_Protocol_FlightInfo.with {
                $0.schema = schema
                $0.flightDescriptor = localDescriptor.toProtocol()
                $0.endpoint = endpoints.map { $0.toProtocol() }
            }
        } else {
            self.flight_info = Arrow_Flight_Protocol_FlightInfo.with {
                $0.schema = schema
                $0.endpoint = endpoints.map { $0.toProtocol() }
            }
        }
    }
    
    func toProtocol() -> Arrow_Flight_Protocol_FlightInfo {
        return self.flight_info
    }
}
