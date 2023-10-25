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

public class FlightDescriptor {
    public enum DescriptorType {
        case unknown
        case path
        case cmd
    }

    public let type: FlightDescriptor.DescriptorType
    public let cmd: Data
    public let paths: [String]

    init(_ descriptor: Arrow_Flight_Protocol_FlightDescriptor) {
        self.type = descriptor.type == .cmd ? .cmd : .path
        self.cmd = descriptor.cmd
        self.paths = descriptor.path
    }

    public init(cmd: Data) {
        self.type = .cmd
        self.cmd = cmd
        self.paths = [String]()
    }

    public init(paths: [String]) {
        self.type = .path
        self.cmd = Data()
        self.paths = paths
    }

    func toProtocol() -> Arrow_Flight_Protocol_FlightDescriptor {
        var descriptor = Arrow_Flight_Protocol_FlightDescriptor()
        descriptor.type = self.type == .cmd ? .cmd : .path
        descriptor.cmd = self.cmd
        descriptor.path = self.paths
        return descriptor
    }
}
