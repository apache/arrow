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

#include "grpcpp/impl/codegen/config_protobuf.h"

// It is necessary to undefined this macro so that the protobuf
// SerializationTraits specialization is not declared in proto_utils.h. We've
// copied that specialization below and modified it to exclude
// protocol::FlightData from the default implementation so we can specialize
// for our faster serialization-deserialization path
#undef GRPC_OPEN_SOURCE_PROTO

#include "grpcpp/impl/codegen/proto_utils.h"

namespace arrow {
namespace ipc {
namespace internal {

struct IpcPayload;

}  // namespace internal
}  // namespace ipc

namespace flight {

struct FlightData;

namespace protocol {

class FlightData;

}  // namespace protocol
}  // namespace flight
}  // namespace arrow

namespace grpc {

using arrow::flight::FlightData;
using arrow::ipc::internal::IpcPayload;

class ByteBuffer;
class Status;

Status FlightDataSerialize(const IpcPayload& msg, ByteBuffer* out, bool* own_buffer);
Status FlightDataDeserialize(ByteBuffer* buffer, FlightData* out);

// This class provides a protobuf serializer. It translates between protobuf
// objects and grpc_byte_buffers. More information about SerializationTraits can
// be found in include/grpcpp/impl/codegen/serialization_traits.h.
template <class T>
class SerializationTraits<
    T, typename std::enable_if<
           std::is_base_of<grpc::protobuf::Message, T>::value &&
           !std::is_same<arrow::flight::protocol::FlightData, T>::value>::type> {
 public:
  static Status Serialize(const grpc::protobuf::Message& msg, ByteBuffer* bb,
                          bool* own_buffer) {
    return GenericSerialize<ProtoBufferWriter, T>(msg, bb, own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, grpc::protobuf::Message* msg) {
    return GenericDeserialize<ProtoBufferReader, T>(buffer, msg);
  }
};

template <class T>
class SerializationTraits<T, typename std::enable_if<std::is_same<
                                 arrow::flight::protocol::FlightData, T>::value>::type> {
 public:
  static Status Serialize(const grpc::protobuf::Message& msg, ByteBuffer* bb,
                          bool* own_buffer) {
    return FlightDataSerialize(*reinterpret_cast<const IpcPayload*>(&msg), bb,
                               own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, grpc::protobuf::Message* msg) {
    return FlightDataDeserialize(buffer, reinterpret_cast<FlightData*>(msg));
  }
};

}  // namespace grpc
