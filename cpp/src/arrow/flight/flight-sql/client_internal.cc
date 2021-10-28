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

#include <arrow/flight/client.h>
#include <arrow/flight/flight-sql/client_internal.h>

namespace arrow {
namespace flight {
namespace sql {
namespace internal {
class FlightClientImpl {
 public:
  explicit FlightClientImpl(std::unique_ptr<FlightClient> client)
      : client_(std::move(client)) {}

  ~FlightClientImpl() = default;

  Status GetFlightInfo(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* info) {
    return client_->GetFlightInfo(options, descriptor, info);
  }

  Status DoPut(const FlightCallOptions& options, const FlightDescriptor& descriptor,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>* stream,
               std::unique_ptr<FlightMetadataReader>* reader) {
    return client_->DoPut(options, descriptor, schema, stream, reader);
  }

  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream) {
    return client_->DoGet(options, ticket, stream);
  }

  Status DoAction(const FlightCallOptions& options, const Action& action,
                  std::unique_ptr<ResultStream>* results) {
    return client_->DoAction(options, action, results);
  }

 private:
  std::unique_ptr<FlightClient> client_;
};

Status FlightClientImpl_GetFlightInfo(FlightClientImpl& client,
                                      const FlightCallOptions& options,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info) {
  return client.GetFlightInfo(options, descriptor, info);
}

Status FlightClientImpl_DoPut(FlightClientImpl& client, const FlightCallOptions& options,
                              const FlightDescriptor& descriptor,
                              const std::shared_ptr<Schema>& schema,
                              std::unique_ptr<FlightStreamWriter>* stream,
                              std::unique_ptr<FlightMetadataReader>* reader) {
  return client.DoPut(options, descriptor, schema, stream, reader);
}

Status FlightClientImpl_DoGet(FlightClientImpl& client, const FlightCallOptions& options,
                              const Ticket& ticket,
                              std::unique_ptr<FlightStreamReader>* stream) {
  return client.DoGet(options, ticket, stream);
}

Status FlightClientImpl_DoAction(FlightClientImpl& client,
                                 const FlightCallOptions& options, const Action& action,
                                 std::unique_ptr<ResultStream>* results) {
  return client.DoAction(options, action, results);
}

std::shared_ptr<FlightClientImpl> FlightClientImpl_Create(
    std::unique_ptr<FlightClient> client) {
  return std::make_shared<FlightClientImpl>(std::move(client));
}

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
