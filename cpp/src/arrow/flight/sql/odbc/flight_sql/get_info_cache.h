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

#pragma once

#include <arrow/flight/sql/client.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace driver {
namespace flight_sql {

class GetInfoCache {
 private:
  std::unordered_map<uint16_t, driver::odbcabstraction::Connection::Info> info_;
  arrow::flight::FlightCallOptions& call_options_;
  std::unique_ptr<arrow::flight::sql::FlightSqlClient>& sql_client_;
  std::mutex mutex_;
  std::atomic<bool> has_server_info_;

 public:
  GetInfoCache(arrow::flight::FlightCallOptions& call_options,
               std::unique_ptr<arrow::flight::sql::FlightSqlClient>& client,
               const std::string& driver_version);
  void SetProperty(uint16_t property, driver::odbcabstraction::Connection::Info value);
  driver::odbcabstraction::Connection::Info GetInfo(uint16_t info_type);

 private:
  bool LoadInfoFromServer();
  void LoadDefaultsForMissingEntries();
};
}  // namespace flight_sql
}  // namespace driver
