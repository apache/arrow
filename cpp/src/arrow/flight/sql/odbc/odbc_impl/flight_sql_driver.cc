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

#include <absl/synchronization/mutex.h>

#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_driver.h"

#include "arrow/compute/api.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"
#include "arrow/flight/sql/odbc/odbc_impl/util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

using arrow::util::ArrowLog;
using arrow::util::ArrowLogLevel;

namespace arrow::flight::sql::odbc {

static constexpr const char* kODBCLogLevel = "ARROW_ODBC_LOG_LEVEL";

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3), version_("0.9.0.0") {
  RegisterComputeKernels();
  // Register log after compute kernels check to avoid segfaults
  RegisterLog();
  // GH-48637: Disable Absl Deadlock detection from upstream projects
  absl::SetMutexDeadlockDetectionMode(absl::OnDeadlockCycle::kIgnore);
}

FlightSqlDriver::~FlightSqlDriver() {
  // Unregister log if logging is enabled
  if (arrow::internal::GetEnvVar(kODBCLogLevel).ValueOr("").empty()) {
    return;
  }
  ArrowLog::ShutDownArrowLog();
}

std::shared_ptr<Connection> FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version, version_);
}

Diagnostics& FlightSqlDriver::GetDiagnostics() { return diagnostics_; }

void FlightSqlDriver::SetVersion(std::string version) { version_ = std::move(version); }

void FlightSqlDriver::RegisterComputeKernels() {
  auto registry = arrow::compute::GetFunctionRegistry();

  // strptime is one of the required compute functions
  auto strptime_func = registry->GetFunction("strptime");
  if (!strptime_func.ok()) {
    // Register Kernel functions to library
    util::ThrowIfNotOK(arrow::compute::Initialize());
  }
}

void FlightSqlDriver::RegisterLog() {
  std::string log_level_str = arrow::internal::GetEnvVar(kODBCLogLevel)
                                  .Map(arrow::internal::AsciiToLower)
                                  .Map(arrow::internal::TrimString)
                                  .ValueOr("");
  if (log_level_str.empty()) {
    return;
  }

  auto log_level = ArrowLogLevel::ARROW_DEBUG;

  if (log_level_str == "fatal") {
    log_level = ArrowLogLevel::ARROW_FATAL;
  } else if (log_level_str == "error") {
    log_level = ArrowLogLevel::ARROW_ERROR;
  } else if (log_level_str == "warning") {
    log_level = ArrowLogLevel::ARROW_WARNING;
  } else if (log_level_str == "info") {
    log_level = ArrowLogLevel::ARROW_INFO;
  } else if (log_level_str == "debug") {
    log_level = ArrowLogLevel::ARROW_DEBUG;
  } else if (log_level_str == "trace") {
    log_level = ArrowLogLevel::ARROW_TRACE;
  }

  // Enable driver logging. Log files are not supported on Windows yet, since GLOG is not
  // tested fully on Windows.
  // Info log level is enabled by default.
  if (log_level != ArrowLogLevel::ARROW_INFO) {
    ArrowLog::StartArrowLog("arrow-flight-sql-odbc", log_level);
  }
}

}  // namespace arrow::flight::sql::odbc
