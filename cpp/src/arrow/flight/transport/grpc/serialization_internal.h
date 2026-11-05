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
#include <string>
#include <string_view>

#include "arrow/flight/protocol_internal.h"
#include "arrow/flight/transport/grpc/protocol_grpc_internal.h"
#include "arrow/flight/type_fwd.h"
#include "arrow/result.h"

namespace arrow {

class Buffer;

namespace flight {
namespace transport {
namespace grpc {

namespace pb = arrow::flight::protocol;

/// Zero-copy conversion of an Arrow Buffer to a gRPC Slice.
///
/// The Buffer lifetime is tied to the Slice (a shared_ptr is attached to the
/// slice's destructor callback)
/// \param[in] buf The Arrow Buffer to convert.
/// \return A gRPC Slice that shares ownership of the Buffer.
arrow::Result<::grpc::Slice> SliceFromBuffer(const std::shared_ptr<Buffer>& buf);

/// Zero-copy conversion of a gRPC ByteBuffer into an Arrow Buffer.
///
/// The Arrow Buffer owns a reference to the gRPC slice, so it stays valid
/// after the ByteBuffer goes away.
/// \param[in] cpp_buf The gRPC ByteBuffer to wrap.
/// \param[out] out The resulting Arrow Buffer that shares ownership of the gRPC slices.
/// \return Status indicating success or failure.
Status WrapGrpcBuffer(::grpc::ByteBuffer* cpp_buf, std::shared_ptr<Buffer>* out);

/// Copy a gRPC ByteBuffer's slices into one contiguous string.
/// \param[in] buffer The gRPC ByteBuffer to copy from.
/// \param[in] error_message The error message to use if the buffer cannot be dumped.
/// \return The buffer's bytes, or an IOError carrying FlightStatusCode::Internal
/// on failure.
ARROW_FLIGHT_EXPORT arrow::Result<std::string> BytesFromBuffer(
    const ::grpc::ByteBuffer& buffer, std::string_view error_message);

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
