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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_stream_chunk_buffer.h"
#include "arrow/flight/sql/odbc/flight_sql/utils.h"

namespace driver {
namespace flight_sql {

using arrow::flight::FlightEndpoint;

FlightStreamChunkBuffer::FlightStreamChunkBuffer(
    FlightSqlClient& flight_sql_client,
    const arrow::flight::FlightCallOptions& call_options,
    const std::shared_ptr<FlightInfo>& flight_info, size_t queue_capacity)
    : queue_(queue_capacity) {
  // FIXME: Endpoint iteration should consider endpoints may be at different hosts
  for (const auto& endpoint : flight_info->endpoints()) {
    const arrow::flight::Ticket& ticket = endpoint.ticket;

    auto result = flight_sql_client.DoGet(call_options, ticket);
    ThrowIfNotOK(result.status());
    std::shared_ptr<FlightStreamReader> stream_reader_ptr(std::move(result.ValueOrDie()));

    BlockingQueue<Result<FlightStreamChunk>>::Supplier supplier = [=] {
      auto result = stream_reader_ptr->Next();
      bool isNotOk = !result.ok();
      bool isNotEmpty = result.ok() && (result.ValueOrDie().data != nullptr);

      return boost::make_optional(isNotOk || isNotEmpty, std::move(result));
    };
    queue_.AddProducer(std::move(supplier));
  }
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk* chunk) {
  Result<FlightStreamChunk> result;
  if (!queue_.Pop(&result)) {
    return false;
  }

  if (!result.status().ok()) {
    Close();
    throw odbcabstraction::DriverException(result.status().message());
  }
  *chunk = std::move(result.ValueOrDie());
  return chunk->data != nullptr;
}

void FlightStreamChunkBuffer::Close() { queue_.Close(); }

FlightStreamChunkBuffer::~FlightStreamChunkBuffer() { Close(); }

}  // namespace flight_sql
}  // namespace driver
