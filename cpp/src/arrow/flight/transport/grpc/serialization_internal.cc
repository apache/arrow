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

#include "arrow/flight/transport/grpc/serialization_internal.h"

// todo cleanup includes

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "arrow/flight/platform.h"

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4267)
#endif

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include <grpc/byte_buffer_reader.h>
#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#else
#include <grpc++/grpc++.h>
#include <grpc++/impl/codegen/proto_utils.h>
#endif

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/server.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace flight {
namespace transport {
namespace grpc {

namespace pb = arrow::flight::protocol;

// The pointer bitcast hack below causes legitimate warnings, silence them.
#ifndef _WIN32
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

// Pointer bitcast explanation: grpc::*Writer<T>::Write() and grpc::*Reader<T>::Read()
// both take a T* argument (here pb::FlightData*).  But they don't do anything
// with that argument except pass it to SerializationTraits<T>::Serialize() and
// SerializationTraits<T>::Deserialize().
//
// Since we control SerializationTraits<pb::FlightData>, we can interpret the
// pointer argument whichever way we want, including cast it back to the original type.
// (see customize_protobuf.h).

arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                       ::grpc::WriteOptions());
}

arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                       ::grpc::WriteOptions());
}

arrow::Result<bool> WritePayload(
    const FlightPayload& payload,
    ::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                       ::grpc::WriteOptions());
}

arrow::Result<bool> WritePayload(const FlightPayload& payload,
                                 ::grpc::ServerWriter<pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                       ::grpc::WriteOptions());
}

bool ReadPayload(::grpc::ClientReader<pb::FlightData>* reader,
                 flight::internal::FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(::grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 flight::internal::FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(::grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 flight::internal::FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(::grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 flight::internal::FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(::grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data) {
  return reader->Read(data);
}

#ifndef _WIN32
#pragma GCC diagnostic pop
#endif

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
