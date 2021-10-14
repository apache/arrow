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

#include <gflags/gflags.h>

#include <iostream>
#include <memory>
#include <string>

#include "arrow/flight/flight-sql/example/sqlite_server.h"
#include "arrow/flight/server.h"
#include "arrow/flight/test_integration.h"
#include "arrow/flight/test_util.h"
#include "arrow/io/test_common.h"
#include "arrow/testing/json_integration.h"
#include "arrow/util/logging.h"

DEFINE_int32(port, 31337, "Server port to listen on");

namespace pb = arrow::flight::protocol;

std::unique_ptr<arrow::flight::FlightServerBase> g_server;

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing server for Flight SQL.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  arrow::flight::Location location;
  ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port, &location));
  arrow::flight::FlightServerOptions options(location);

  g_server.reset(new arrow::flight::sql::example::SQLiteFlightSqlServer());

  ARROW_CHECK_OK(g_server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(g_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Server listening on localhost:" << g_server->port() << std::endl;
  ARROW_CHECK_OK(g_server->Serve());
  return 0;
}
