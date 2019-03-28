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

#include "arrow/util/config.h"
#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/impl/codegen/config_protobuf.h>
#else
#include <grpc++/impl/codegen/config_protobuf.h>
#endif

// It is necessary to undefined this macro so that the protobuf
// SerializationTraits specialization is not declared in proto_utils.h. We've
// copied that specialization below and modified it to exclude
// protocol::FlightData from the default implementation so we can specialize
// for our faster serialization-deserialization path
#undef GRPC_OPEN_SOURCE_PROTO

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/impl/codegen/proto_utils.h>
#else
#include <grpc++/impl/codegen/proto_utils.h>
#endif

namespace grpc {

class ByteBuffer;

}  // namespace grpc

namespace arrow {
namespace flight {

struct FlightPayload;

namespace internal {

struct FlightData;

// Those two functions are defined in serialization-internal.cc

// Write FlightData to a grpc::ByteBuffer without extra copying
grpc::Status FlightDataSerialize(const FlightPayload& msg, grpc::ByteBuffer* out,
                                 bool* own_buffer);

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
grpc::Status FlightDataDeserialize(grpc::ByteBuffer* buffer, FlightData* out);

}  // namespace internal

namespace protocol {

class FlightData;

}  // namespace protocol
}  // namespace flight
}  // namespace arrow

namespace grpc {

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
  // In the functions below, we cast back the Message argument to its real
  // type (see ReadPayload() and WritePayload() for the initial cast).
  static Status Serialize(const grpc::protobuf::Message& msg, ByteBuffer* bb,
                          bool* own_buffer) {
    return arrow::flight::internal::FlightDataSerialize(
        *reinterpret_cast<const arrow::flight::FlightPayload*>(&msg), bb, own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, grpc::protobuf::Message* msg) {
    return arrow::flight::internal::FlightDataDeserialize(
        buffer, reinterpret_cast<arrow::flight::internal::FlightData*>(msg));
  }
};

}  // namespace grpc
