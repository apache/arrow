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

// Integration test scenarios for Arrow Flight.

#include "arrow/flight/visibility.h"

#include <memory>
#include <string>

#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {

/// \brief An integration test for Arrow Flight.
class ARROW_FLIGHT_EXPORT Scenario {
 public:
  virtual ~Scenario() = default;
  /// \brief Set up the server.
  virtual Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                            FlightServerOptions* options) = 0;
  /// \brief Set up the client.
  virtual Status MakeClient(FlightClientOptions* options) = 0;
  /// \brief Run the scenario as the client.
  virtual Status RunClient(std::unique_ptr<FlightClient> client) = 0;
};

/// \brief Get the implementation of an integration test scenario by name.
Status GetScenario(const std::string& scenario_name, std::shared_ptr<Scenario>* out);

}  // namespace flight
}  // namespace arrow
