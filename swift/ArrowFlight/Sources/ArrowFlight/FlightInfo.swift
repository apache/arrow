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
    let flightInfo: Arrow_Flight_Protocol_FlightInfo
    public var flightDescriptor: FlightDescriptor? {
        return flightInfo.hasFlightDescriptor ? FlightDescriptor(flightInfo.flightDescriptor) : nil
    }

    public var endpoints: [FlightEndpoint] {
        return self.flightInfo.endpoint.map { FlightEndpoint($0) }
    }
    public var schema: ArrowSchema? {
        return schemaFromMessage(self.flightInfo.schema)
    }

    var endpoint: [Arrow_Flight_Protocol_FlightEndpoint] = []
    init(_ flightInfo: Arrow_Flight_Protocol_FlightInfo) {
        self.flightInfo = flightInfo
    }

    public init(_ schema: Data, endpoints: [FlightEndpoint] = [FlightEndpoint](), descriptor: FlightDescriptor? = nil) {
        if let localDescriptor = descriptor {
            self.flightInfo = Arrow_Flight_Protocol_FlightInfo.with {
                $0.schema = schema
                $0.flightDescriptor = localDescriptor.toProtocol()
                $0.endpoint = endpoints.map { $0.toProtocol() }
            }
        } else {
            self.flightInfo = Arrow_Flight_Protocol_FlightInfo.with {
                $0.schema = schema
                $0.endpoint = endpoints.map { $0.toProtocol() }
            }
        }
    }

    func toProtocol() -> Arrow_Flight_Protocol_FlightInfo {
        return self.flightInfo
    }
}
