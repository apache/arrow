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
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spd_logger.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/utils.h"

#define DEFAULT_MAXIMUM_FILE_SIZE 16777216
#define CONFIG_FILE_NAME "arrow-odbc.ini"

namespace driver {
namespace flight_sql {

using odbcabstraction::Connection;
using odbcabstraction::LogLevel;
using odbcabstraction::OdbcVersion;
using odbcabstraction::SPDLogger;

namespace {
LogLevel ToLogLevel(int64_t level) {
  switch (level) {
    case 0:
      return LogLevel::LogLevel_TRACE;
    case 1:
      return LogLevel::LogLevel_DEBUG;
    case 2:
      return LogLevel::LogLevel_INFO;
    case 3:
      return LogLevel::LogLevel_WARN;
    case 4:
      return LogLevel::LogLevel_ERROR;
    default:
      return LogLevel::LogLevel_OFF;
  }
}
}  // namespace

FlightSqlDriver::FlightSqlDriver()
    : diagnostics_("Apache Arrow", "Flight SQL", OdbcVersion::V_3), version_("0.9.0.0") {}

std::shared_ptr<Connection> FlightSqlDriver::CreateConnection(OdbcVersion odbc_version) {
  return std::make_shared<FlightSqlConnection>(odbc_version, version_);
}

odbcabstraction::Diagnostics& FlightSqlDriver::GetDiagnostics() { return diagnostics_; }

void FlightSqlDriver::SetVersion(std::string version) { version_ = std::move(version); }

void FlightSqlDriver::RegisterLog() {
  odbcabstraction::PropertyMap propertyMap;
  driver::odbcabstraction::ReadConfigFile(propertyMap, CONFIG_FILE_NAME);

  auto log_enable_iterator = propertyMap.find(SPDLogger::LOG_ENABLED);
  auto log_enabled = log_enable_iterator != propertyMap.end()
                         ? odbcabstraction::AsBool(log_enable_iterator->second)
                         : false;
  if (!log_enabled) {
    return;
  }

  auto log_path_iterator = propertyMap.find(SPDLogger::LOG_PATH);
  auto log_path = log_path_iterator != propertyMap.end() ? log_path_iterator->second : "";
  if (log_path.empty()) {
    return;
  }

  auto log_level_iterator = propertyMap.find(SPDLogger::LOG_LEVEL);
  auto log_level = ToLogLevel(log_level_iterator != propertyMap.end()
                                  ? std::stoi(log_level_iterator->second)
                                  : 1);
  if (log_level == odbcabstraction::LogLevel_OFF) {
    return;
  }

  auto maximum_file_size_iterator = propertyMap.find(SPDLogger::MAXIMUM_FILE_SIZE);
  auto maximum_file_size = maximum_file_size_iterator != propertyMap.end()
                               ? std::stoi(maximum_file_size_iterator->second)
                               : DEFAULT_MAXIMUM_FILE_SIZE;

  auto maximum_file_quantity_iterator = propertyMap.find(SPDLogger::FILE_QUANTITY);
  auto maximum_file_quantity = maximum_file_quantity_iterator != propertyMap.end()
                                   ? std::stoi(maximum_file_quantity_iterator->second)
                                   : 1;

  std::unique_ptr<odbcabstraction::SPDLogger> logger(new odbcabstraction::SPDLogger());

  logger->init(maximum_file_quantity, maximum_file_size, log_path, log_level);
  odbcabstraction::Logger::SetInstance(std::move(logger));
}

}  // namespace flight_sql
}  // namespace driver
