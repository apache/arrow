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

#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/blocking_queue.h>

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::sql::FlightSqlClient;
using driver::odbcabstraction::BlockingQueue;

class FlightStreamChunkBuffer {
  BlockingQueue<Result<FlightStreamChunk>> queue_;

 public:
  FlightStreamChunkBuffer(FlightSqlClient& flight_sql_client,
                          const arrow::flight::FlightCallOptions& call_options,
                          const std::shared_ptr<FlightInfo>& flight_info,
                          size_t queue_capacity = 5);

  ~FlightStreamChunkBuffer();

  void Close();

  bool GetNext(FlightStreamChunk* chunk);
};

}  // namespace flight_sql
}  // namespace driver
