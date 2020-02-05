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

#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/message.h"
#include "arrow/status.h"

namespace arrow {

class Buffer;

namespace flight {
namespace internal {

/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
struct FlightData {
  /// Used only for puts, may be null
  std::unique_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Application-defined metadata
  std::shared_ptr<Buffer> app_metadata;

  /// Message body
  std::shared_ptr<Buffer> body;

  /// Open IPC message from the metadata and body
  Status OpenMessage(std::unique_ptr<ipc::Message>* message);
};

/// Write Flight message on gRPC stream with zero-copy optimizations.
/// True is returned on success, false if some error occurred (connection closed?).
bool WritePayload(const FlightPayload& payload,
                  grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer);
bool WritePayload(const FlightPayload& payload,
                  grpc::ServerWriter<pb::FlightData>* writer);

/// Read Flight message from gRPC stream with zero-copy optimizations.
/// True is returned on success, false if stream ended.
bool ReadPayload(grpc::ClientReader<pb::FlightData>* reader, FlightData* data);
bool ReadPayload(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 FlightData* data);

}  // namespace internal
}  // namespace flight
}  // namespace arrow
