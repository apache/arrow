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

using arrow::flight::FlightClient;
using arrow::flight::FlightEndpoint;

FlightStreamChunkBuffer::FlightStreamChunkBuffer(
    FlightSqlClient& flight_sql_client,
    const arrow::flight::FlightClientOptions& client_options,
    const arrow::flight::FlightCallOptions& call_options,
    const std::shared_ptr<FlightInfo>& flight_info, size_t queue_capacity)
    : queue_(queue_capacity) {
  for (const auto& endpoint : flight_info->endpoints()) {
    const arrow::flight::Ticket& ticket = endpoint.ticket;

    arrow::Result<std::unique_ptr<FlightStreamReader>> result;
    std::shared_ptr<FlightSqlClient> temp_flight_sql_client;
    auto endpoint_locations = endpoint.locations;
    if (endpoint_locations.empty()) {
      // list of Locations needs to be empty to proceed
      result = flight_sql_client.DoGet(call_options, ticket);
    } else {
      // If it is non-empty, the driver should create a FlightSqlClient to connect to one
      // of the specified Locations directly.

      // GH-47117: Currently a new FlightClient will be made for each partition that
      // returns a non-empty Location, which is then disposed of. It may be better to
      // cache clients because a server may report the same Locations. It would also be
      // good to identify when the reported Location is the same as the original
      // connection's Location and skip creating a FlightClient in that scenario.

      std::unique_ptr<FlightClient> temp_flight_client;
      ThrowIfNotOK(FlightClient::Connect(endpoint_locations[0], client_options)
                       .Value(&temp_flight_client));
      temp_flight_sql_client.reset(new FlightSqlClient(std::move(temp_flight_client)));

      result = temp_flight_sql_client->DoGet(call_options, ticket);
    }

    ThrowIfNotOK(result.status());
    std::shared_ptr<FlightStreamReader> stream_reader_ptr(std::move(result.ValueOrDie()));

    BlockingQueue<std::pair<Result<FlightStreamChunk>,
                            std::shared_ptr<FlightSqlClient>>>::Supplier supplier = [=] {
      auto result = stream_reader_ptr->Next();
      bool is_not_ok = !result.ok();
      bool is_not_empty = result.ok() && (result.ValueOrDie().data != nullptr);

      // If result is valid, save the temp Flight SQL Client for future stream reader
      // call. temp_flight_sql_client is intentionally null if the list of endpoint
      // Locations is empty.
      // After all data is fetched from reader, the temp client is closed.
      return boost::make_optional(
          is_not_ok || is_not_empty,
          std::make_pair(std::move(result), temp_flight_sql_client));
    };
    queue_.AddProducer(std::move(supplier));
  }
}

bool FlightStreamChunkBuffer::GetNext(FlightStreamChunk* chunk) {
  std::pair<Result<FlightStreamChunk>, std::shared_ptr<FlightSqlClient>>
      closeable_endpoint_stream_pair;
  if (!queue_.Pop(&closeable_endpoint_stream_pair)) {
    return false;
  }

  Result<FlightStreamChunk> result = closeable_endpoint_stream_pair.first;
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
