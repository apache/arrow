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

// Enable gRPC customizations
#include "arrow/flight/protocol-internal.h"  // IWYU pragma: keep

#include <cstdint>
#include <limits>
#include <memory>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#include "grpc/byte_buffer_reader.h"

#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"

namespace pb = arrow::flight::protocol;

constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();

namespace arrow {
namespace flight {

/// Internal, not user-visible type used for memory-efficient reads from gRPC
/// stream
struct FlightData {
  /// Used only for puts, may be null
  std::unique_ptr<FlightDescriptor> descriptor;

  /// Non-length-prefixed Message header as described in format/Message.fbs
  std::shared_ptr<Buffer> metadata;

  /// Message body
  std::shared_ptr<Buffer> body;
};

namespace internal {

using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

bool ReadBytesZeroCopy(const std::shared_ptr<arrow::Buffer>& source_data,
                       CodedInputStream* input, std::shared_ptr<arrow::Buffer>* out);

}  // namespace internal
}  // namespace flight
}  // namespace arrow

namespace grpc {

using arrow::flight::FlightData;

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

// Helper to log status code, as gRPC doesn't expose why
// (de)serialization fails
inline Status FailSerialization(Status status) {
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "Error deserializing Flight message: "
                       << status.error_message();
  }
  return status;
}

inline arrow::Status FailSerialization(arrow::Status status) {
  if (!status.ok()) {
    ARROW_LOG(WARNING) << "Error deserializing Flight message: " << status.ToString();
  }
  return status;
}

// Write FlightData to a grpc::ByteBuffer without extra copying
Status FlightDataSerialize(const FlightPayload& msg, ByteBuffer* out, bool* own_buffer);

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
Status FlightDataDeserialize(ByteBuffer* buffer, FlightData* out);

}  // namespace grpc
