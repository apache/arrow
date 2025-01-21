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

#include "arrow/util/config.h"

#include <gflags/gflags.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>

#include "arrow/flight/server.h"
#include "arrow/flight/server_tracing_middleware.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/io/test_common.h"
#include "arrow/util/logging.h"

#ifdef ARROW_WITH_OPENTELEMETRY
#  include "arrow/flight/otel_logging.h"
#  include "arrow/util/tracing_internal.h"

#  include <opentelemetry/context/propagation/global_propagator.h>
#  include <opentelemetry/context/propagation/text_map_propagator.h>
#  include <opentelemetry/trace/propagation/http_trace_context.h>
#endif

DEFINE_int32(port, 31337, "Server port to listen on");

#ifdef ARROW_WITH_OPENTELEMETRY
arrow::Status SetupOTel() {
  auto tracer = arrow::internal::tracing::GetTracer();
  ARROW_UNUSED(tracer);

  opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      opentelemetry::nostd::shared_ptr<
          opentelemetry::context::propagation::TextMapPropagator>(
          new opentelemetry::trace::propagation::HttpTraceContext()));

  ARROW_RETURN_NOT_OK(arrow::telemetry::internal::InitializeOtelLoggerProvider());

  auto logging_options = arrow::telemetry::OtelLoggingOptions::Defaults();
  logging_options.severity_threshold = arrow::telemetry::LogLevel::ARROW_TRACE;
  // Flush after every log message
  logging_options.flush_severity = arrow::telemetry::LogLevel::ARROW_TRACE;
  ARROW_RETURN_NOT_OK(arrow::flight::RegisterFlightOtelLoggers(logging_options));

  return arrow::Status::OK();
}
#endif

arrow::Status RunMain() {
#ifdef ARROW_WITH_OPENTELEMETRY
  ARROW_RETURN_NOT_OK(SetupOTel());
#endif

  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port));
  arrow::flight::FlightServerOptions options(location);
  options.middleware.emplace_back("tracing",
                                  arrow::flight::MakeTracingServerMiddlewareFactory());

  std::shared_ptr<arrow::flight::sql::example::SQLiteFlightSqlServer> server;
  ARROW_ASSIGN_OR_RAISE(server,
                        arrow::flight::sql::example::SQLiteFlightSqlServer::Create())

  ARROW_CHECK_OK(server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Server listening on localhost:" << server->port() << std::endl;
  ARROW_CHECK_OK(server->Serve());

  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing server for Flight SQL.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
