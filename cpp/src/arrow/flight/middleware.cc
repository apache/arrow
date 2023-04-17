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

#include "arrow/flight/middleware.h"

namespace arrow {
namespace flight {

std::string ToString(FlightMethod method) {
  // Technically, we can get this via Protobuf reflection, but in
  // practice we'd have to hardcode the method names to look up the
  // method descriptor...
  switch (method) {
    case FlightMethod::Handshake:
      return "Handshake";
    case FlightMethod::ListFlights:
      return "ListFlights";
    case FlightMethod::GetFlightInfo:
      return "GetFlightInfo";
    case FlightMethod::GetSchema:
      return "GetSchema";
    case FlightMethod::DoGet:
      return "DoGet";
    case FlightMethod::DoPut:
      return "DoPut";
    case FlightMethod::DoAction:
      return "DoAction";
    case FlightMethod::ListActions:
      return "ListActions";
    case FlightMethod::DoExchange:
      return "DoExchange";
    case FlightMethod::Invalid:
    default:
      return "(unknown Flight method)";
  }
}

}  // namespace flight
}  // namespace arrow
