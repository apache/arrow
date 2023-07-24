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

// Example Flight SQL server backed by Acero.

#include <signal.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/flight/sql/example/acero_server.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

namespace flight = arrow::flight;
namespace sql = arrow::flight::sql;

DEFINE_string(location, "grpc://localhost:12345", "Location to listen on");

arrow::Status RunMain(const std::string& location_str) {
  ARROW_ASSIGN_OR_RAISE(flight::Location location, flight::Location::Parse(location_str));
  flight::FlightServerOptions options(location);

  std::unique_ptr<flight::FlightServerBase> server;
  ARROW_ASSIGN_OR_RAISE(server, sql::acero_example::MakeAceroServer());
  ARROW_RETURN_NOT_OK(server->Init(options));

  ARROW_RETURN_NOT_OK(server->SetShutdownOnSignals({SIGTERM}));

  ARROW_LOG(INFO) << "Listening on " << location.ToString();

  ARROW_RETURN_NOT_OK(server->Serve());
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  arrow::util::ArrowLog::StartArrowLog("acero-flight-sql-server",
                                       arrow::util::ArrowLogLevel::ARROW_INFO);
  arrow::util::ArrowLog::InstallFailureSignalHandler();

  arrow::Status st = RunMain(FLAGS_location);

  arrow::util::ArrowLog::ShutDownArrowLog();

  if (!st.ok()) {
    std::cerr << st << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
