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

// Run the integration test scenarios in-process.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/flight/integration_tests/test_integration.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace flight {
namespace integration_tests {

Status RunScenario(const std::string& scenario_name) {
  std::shared_ptr<Scenario> scenario;
  ARROW_RETURN_NOT_OK(GetScenario(scenario_name, &scenario));

  std::unique_ptr<FlightServerBase> server;
  ARROW_ASSIGN_OR_RAISE(Location bind_location,
                        arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0));
  FlightServerOptions server_options(bind_location);
  ARROW_RETURN_NOT_OK(scenario->MakeServer(&server, &server_options));
  ARROW_RETURN_NOT_OK(server->Init(server_options));

  ARROW_ASSIGN_OR_RAISE(Location location,
                        arrow::flight::Location::ForGrpcTcp("0.0.0.0", server->port()));
  auto client_options = arrow::flight::FlightClientOptions::Defaults();
  ARROW_RETURN_NOT_OK(scenario->MakeClient(&client_options));
  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<FlightClient> client,
                        FlightClient::Connect(location, client_options));
  ARROW_RETURN_NOT_OK(scenario->RunClient(std::move(client)));
  return Status::OK();
}

TEST(FlightIntegration, AuthBasicProto) { ASSERT_OK(RunScenario("auth:basic_proto")); }

TEST(FlightIntegration, Middleware) { ASSERT_OK(RunScenario("middleware")); }

TEST(FlightIntegration, Ordered) { ASSERT_OK(RunScenario("ordered")); }

TEST(FlightIntegration, FlightSql) { ASSERT_OK(RunScenario("flight_sql")); }

TEST(FlightIntegration, FlightSqlExtension) {
  ASSERT_OK(RunScenario("flight_sql:extension"));
}

}  // namespace integration_tests
}  // namespace flight
}  // namespace arrow
