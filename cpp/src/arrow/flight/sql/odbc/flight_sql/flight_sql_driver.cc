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

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h"
#include "arrow/compute/api.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

#define ODBC_LOG_LEVEL "ARROW_ODBC_LOG_LEVEL"

using arrow::util::ArrowLogLevel;

namespace {
/// Return the corresponding ArrowLogLevel. Debug level is returned by default.
ArrowLogLevel ToLogLevel(int64_t level) {
  switch (level) {
    case -2:
      return ArrowLogLevel::ARROW_TRACE;
    case -1:
      return ArrowLogLevel::ARROW_DEBUG;
    case 0:
      return ArrowLogLevel::ARROW_INFO;
    case 1:
      return ArrowLogLevel::ARROW_WARNING;
    case 2:
      return ArrowLogLevel::ARROW_ERROR;
    case 3:
      return ArrowLogLevel::ARROW_FATAL;
    default:
      return ArrowLogLevel::ARROW_DEBUG;
  }
}
}  // namespace

namespace driver {
namespace flight_sql {

using odbcabstraction::Connection;
using odbcabstraction::OdbcVersion;

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3), version_("0.9.0.0") {
  RegisterLog();
  RegisterComputeKernels();
}

std::shared_ptr<Connection> FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version, version_);
}

odbcabstraction::Diagnostics& FlightSqlDriver::GetDiagnostics() { return diagnostics_; }

void FlightSqlDriver::SetVersion(std::string version) { version_ = std::move(version); }

void FlightSqlDriver::RegisterComputeKernels() {
  auto registry = arrow::compute::GetFunctionRegistry();

  // strptime is one of the required compute functions
  auto strptime_func = registry->GetFunction("strptime");
  if (!strptime_func.ok()) {
    // Register Kernel functions to library
    ThrowIfNotOK(arrow::compute::Initialize());
  }
}

void FlightSqlDriver::RegisterLog() {
  std::string log_level_str = arrow::internal::GetEnvVar(ODBC_LOG_LEVEL).ValueOr("");
  if (log_level_str.empty()) {
    return;
  }
  auto log_level = ToLogLevel(std::stoi(log_level_str));

  // Enable driver logging. Log files are not supported on Windows yet, since GLOG is not
  // tested fully on Windows.
  arrow::util::ArrowLog::StartArrowLog("arrow-flight-sql-odbc", log_level);
}

}  // namespace flight_sql
}  // namespace driver
