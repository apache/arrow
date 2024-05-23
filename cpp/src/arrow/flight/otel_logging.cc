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

#include <string_view>

#include "arrow/flight/otel_logging.h"
#include "arrow/flight/otel_logging_internal.h"
#include "arrow/result.h"
#include "arrow/util/logger.h"
#include "arrow/util/logging.h"

namespace arrow::flight {

namespace {
constexpr std::string_view kGrpcClientName = "arrow-flight-grpc-client-otel";
constexpr std::string_view kGrpcServerName = "arrow-flight-grpc-server-otel";
constexpr std::string_view kSqlClientName = "arrow-flight-sql-client-otel";
constexpr std::string_view kSqlServerName = "arrow-flight-sql-server-otel";
}  // namespace

Status RegisterFlightOtelLoggers(const telemetry::OtelLoggingOptions& options) {
  for (auto name : {kGrpcClientName, kGrpcServerName, kSqlClientName, kSqlServerName}) {
    ARROW_ASSIGN_OR_RAISE(auto logger,
                          telemetry::OtelLoggerProvider::MakeLogger(name, options));
    DCHECK_NE(logger, nullptr);
    ARROW_RETURN_NOT_OK(util::LoggerRegistry::RegisterLogger(name, std::move(logger)));
  }
  return Status::OK();
}

namespace internal {

std::shared_ptr<util::Logger> GetOtelGrpcClientLogger() {
  return util::LoggerRegistry::GetLogger(kGrpcClientName);
}
std::shared_ptr<util::Logger> GetOtelGrpcServerLogger() {
  return util::LoggerRegistry::GetLogger(kGrpcServerName);
}
std::shared_ptr<util::Logger> GetOtelSqlClientLogger() {
  return util::LoggerRegistry::GetLogger(kSqlClientName);
}
std::shared_ptr<util::Logger> GetOtelSqlServerLogger() {
  return util::LoggerRegistry::GetLogger(kSqlServerName);
}

}  // namespace internal

}  // namespace arrow::flight
