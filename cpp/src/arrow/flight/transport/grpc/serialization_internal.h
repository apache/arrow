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

// (De)serialization utilities that hook into gRPC, efficiently
// handling Arrow-encoded data in a gRPC call.

#pragma once

#include <memory>

#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/transport/grpc/protocol_grpc_internal.h"
#include "arrow/flight/type_fwd.h"
#include "arrow/result.h"

namespace arrow {
namespace flight {
namespace transport {
namespace grpc {

namespace pb = arrow::flight::protocol;

/// Write Flight message on gRPC stream with zero-copy optimizations.
// Returns Invalid if the payload is ill-formed
// Returns true if the payload was written, false if it was not
// (likely due to disconnect or end-of-stream, e.g. via an
// asynchronous cancellation)
arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer);
arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* writer);
arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* writer);
arrow::Result<bool> WritePayload(const FlightPayload& payload,
                                 ::grpc::ServerWriter<pb::FlightData>* writer);

/// Read Flight message from gRPC stream with zero-copy optimizations.
/// True is returned on success, false if stream ended.
bool ReadPayload(::grpc::ClientReader<pb::FlightData>* reader,
                 flight::internal::FlightData* data);
bool ReadPayload(::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 flight::internal::FlightData* data);
bool ReadPayload(::grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 flight::internal::FlightData* data);
bool ReadPayload(::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 flight::internal::FlightData* data);
// Overload to make genericity easier in DoPutPayloadWriter
bool ReadPayload(::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data);

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
