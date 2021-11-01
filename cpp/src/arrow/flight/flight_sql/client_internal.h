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

#include <arrow/flight/types.h>

namespace arrow {
namespace flight {

class FlightClient;
class FlightCallOptions;
class FlightStreamWriter;
class FlightMetadataReader;
class FlightStreamReader;

namespace sql {
namespace internal {

class FlightClientImpl;

Status FlightClientImpl_GetFlightInfo(FlightClientImpl& client,
                                      const FlightCallOptions& options,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info);

Status FlightClientImpl_DoPut(FlightClientImpl& client, const FlightCallOptions& options,
                              const FlightDescriptor& descriptor,
                              const std::shared_ptr<Schema>& schema,
                              std::unique_ptr<FlightStreamWriter>* stream,
                              std::unique_ptr<FlightMetadataReader>* reader);

Status FlightClientImpl_DoGet(FlightClientImpl& client, const FlightCallOptions& options,
                              const Ticket& ticket,
                              std::unique_ptr<FlightStreamReader>* stream);

Status FlightClientImpl_DoAction(FlightClientImpl& client,
                                 const FlightCallOptions& options, const Action& action,
                                 std::unique_ptr<ResultStream>* results);

std::shared_ptr<FlightClientImpl> FlightClientImpl_Create(
    std::unique_ptr<FlightClient> client);

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
