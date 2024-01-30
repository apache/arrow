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

#include <limits>
#include <memory>

#include "arrow/flight/platform.h"
#include "arrow/flight/type_fwd.h"
#include "arrow/util/config.h"

// Silence protobuf warnings
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4244)
#pragma warning(disable : 4267)
#endif

#include <grpcpp/impl/codegen/config_protobuf.h>
#include <grpcpp/impl/codegen/proto_utils.h>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

namespace grpc {

class ByteBuffer;

}  // namespace grpc

namespace arrow {
namespace flight {

namespace protocol {

class FlightData;

}  // namespace protocol

namespace transport {
namespace grpc {
// Those two functions are defined in serialization_internal.cc

// Write FlightData to a grpc::ByteBuffer without extra copying
::grpc::Status FlightDataSerialize(const arrow::flight::FlightPayload& msg,
                                   ::grpc::ByteBuffer* out, bool* own_buffer);

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
::grpc::Status FlightDataDeserialize(::grpc::ByteBuffer* buffer,
                                     arrow::flight::internal::FlightData* out);
}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow

namespace grpc {

template <>
class SerializationTraits<arrow::flight::protocol::FlightData> {
#ifdef GRPC_CUSTOM_MESSAGELITE
  using MessageType = grpc::protobuf::MessageLite;
#else
  using MessageType = grpc::protobuf::Message;
#endif

 public:
  // In the functions below, we cast back the Message argument to its real
  // type (see ReadPayload() and WritePayload() for the initial cast).
  static Status Serialize(const MessageType& msg, ByteBuffer* bb, bool* own_buffer) {
    return arrow::flight::transport::grpc::FlightDataSerialize(
        *reinterpret_cast<const arrow::flight::FlightPayload*>(&msg), bb, own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, MessageType* msg) {
    return arrow::flight::transport::grpc::FlightDataDeserialize(
        buffer, reinterpret_cast<arrow::flight::internal::FlightData*>(msg));
  }
};

}  // namespace grpc
