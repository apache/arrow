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

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/connection.h"

namespace arrow::flight::sql::odbc {

class GetInfoCache {
 private:
  std::unordered_map<uint16_t, Connection::Info> info_;
  FlightClientOptions& client_options_;
  FlightCallOptions& call_options_;
  std::unique_ptr<FlightSqlClient>& sql_client_;
  std::mutex mutex_;
  std::atomic<bool> has_server_info_;

 public:
  GetInfoCache(FlightClientOptions& client_options, FlightCallOptions& call_options,
               std::unique_ptr<FlightSqlClient>& client,
               const std::string& driver_version);
  void SetProperty(uint16_t property, Connection::Info value);
  Connection::Info GetInfo(uint16_t info_type);

 private:
  bool LoadInfoFromServer();
  void LoadDefaultsForMissingEntries();
};

}  // namespace arrow::flight::sql::odbc
