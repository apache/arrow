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

public class FlightData {
    let flightData: Arrow_Flight_Protocol_FlightData
    public var flightDescriptor: FlightDescriptor? {
        return flightData.hasFlightDescriptor ? FlightDescriptor(flightData.flightDescriptor) : nil
    }

    public var dataHeader: Data { flightData.dataHeader }

    public var dataBody: Data { flightData.dataBody }

    init(_ flightData: Arrow_Flight_Protocol_FlightData) {
        self.flightData = flightData
    }

    public init(_ dataHeader: Data, dataBody: Data, flightDescriptor: FlightDescriptor? = nil) {
        if flightDescriptor != nil {
            self.flightData = Arrow_Flight_Protocol_FlightData.with {
                $0.dataHeader = dataHeader
                $0.dataBody = dataBody
                $0.flightDescriptor = flightDescriptor!.toProtocol()
            }
        } else {
            self.flightData = Arrow_Flight_Protocol_FlightData.with {
                $0.dataBody = dataBody
            }
        }
    }

    func toProtocol() -> Arrow_Flight_Protocol_FlightData { self.flightData }
}
