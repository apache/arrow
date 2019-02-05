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

#include <limits>
#include <memory>

#include "google/protobuf/io/coded_stream.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/wire_format_lite.h"
#include "grpc/byte_buffer_reader.h"
#include "grpcpp/grpcpp.h"

#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "arrow/flight/Flight.grpc.pb.h"
#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/types.h"

namespace pb = arrow::flight::protocol;

using arrow::ipc::internal::IpcPayload;

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

// More efficient writing of FlightData to gRPC output buffer
// Implementation of ZeroCopyOutputStream that writes to a fixed-size buffer
class FixedSizeProtoWriter : public ::google::protobuf::io::ZeroCopyOutputStream {
 public:
  explicit FixedSizeProtoWriter(grpc_slice slice)
      : slice_(slice),
        bytes_written_(0),
        total_size_(static_cast<int>(GRPC_SLICE_LENGTH(slice))) {}

  bool Next(void** data, int* size) override {
    // Consume the whole slice
    *data = GRPC_SLICE_START_PTR(slice_) + bytes_written_;
    *size = total_size_ - bytes_written_;
    bytes_written_ = total_size_;
    return true;
  }

  void BackUp(int count) override { bytes_written_ -= count; }

  int64_t ByteCount() const override { return bytes_written_; }

 private:
  grpc_slice slice_;
  int bytes_written_;
  int total_size_;
};

bool ReadBytesZeroCopy(const std::shared_ptr<arrow::Buffer>& source_data,
                       CodedInputStream* input, std::shared_ptr<arrow::Buffer>* out);

// Internal wrapper for gRPC ByteBuffer so its memory can be exposed to Arrow
// consumers with zero-copy
class GrpcBuffer : public arrow::MutableBuffer {
 public:
  GrpcBuffer(grpc_slice slice, bool incref)
      : MutableBuffer(GRPC_SLICE_START_PTR(slice),
                      static_cast<int64_t>(GRPC_SLICE_LENGTH(slice))),
        slice_(incref ? grpc_slice_ref(slice) : slice) {}

  ~GrpcBuffer() override {
    // Decref slice
    grpc_slice_unref(slice_);
  }

  static arrow::Status Wrap(grpc::ByteBuffer* cpp_buf,
                            std::shared_ptr<arrow::Buffer>* out) {
    // These types are guaranteed by static assertions in gRPC to have the same
    // in-memory representation

    auto buffer = *reinterpret_cast<grpc_byte_buffer**>(cpp_buf);

    // This part below is based on the Flatbuffers gRPC SerializationTraits in
    // flatbuffers/grpc.h

    // Check if this is a single uncompressed slice.
    if ((buffer->type == GRPC_BB_RAW) &&
        (buffer->data.raw.compression == GRPC_COMPRESS_NONE) &&
        (buffer->data.raw.slice_buffer.count == 1)) {
      // If it is, then we can reference the `grpc_slice` directly.
      grpc_slice slice = buffer->data.raw.slice_buffer.slices[0];

      // Increment reference count so this memory remains valid
      *out = std::make_shared<GrpcBuffer>(slice, true);
    } else {
      // Otherwise, we need to use `grpc_byte_buffer_reader_readall` to read
      // `buffer` into a single contiguous `grpc_slice`. The gRPC reader gives
      // us back a new slice with the refcount already incremented.
      grpc_byte_buffer_reader reader;
      if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return arrow::Status::IOError("Internal gRPC error reading from ByteBuffer");
      }
      grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
      grpc_byte_buffer_reader_destroy(&reader);

      // Steal the slice reference
      *out = std::make_shared<GrpcBuffer>(slice, false);
    }

    return arrow::Status::OK();
  }

 private:
  grpc_slice slice_;
};

}  // namespace internal

}  // namespace flight
}  // namespace arrow

namespace grpc {

using arrow::flight::FlightData;
using arrow::flight::internal::FixedSizeProtoWriter;
using arrow::flight::internal::GrpcBuffer;
using arrow::flight::internal::ReadBytesZeroCopy;

using google::protobuf::internal::WireFormatLite;
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

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
template <>
class SerializationTraits<FlightData> {
 public:
  static Status Serialize(const FlightData& msg, ByteBuffer** buffer, bool* own_buffer) {
    return FailSerialization(Status(
        StatusCode::UNIMPLEMENTED, "internal::FlightData serialization not implemented"));
  }

  static Status Deserialize(ByteBuffer* buffer, FlightData* out) {
    if (!buffer) {
      return FailSerialization(Status(StatusCode::INTERNAL, "No payload"));
    }

    std::shared_ptr<arrow::Buffer> wrapped_buffer;
    GRPC_RETURN_NOT_OK(FailSerialization(GrpcBuffer::Wrap(buffer, &wrapped_buffer)));

    auto buffer_length = static_cast<int>(wrapped_buffer->size());
    CodedInputStream pb_stream(wrapped_buffer->data(), buffer_length);

    // TODO(wesm): The 2-parameter version of this function is deprecated
    pb_stream.SetTotalBytesLimit(buffer_length, -1 /* no threshold */);

    // This is the bytes remaining when using CodedInputStream like this
    while (pb_stream.BytesUntilTotalBytesLimit()) {
      const uint32_t tag = pb_stream.ReadTag();
      const int field_number = WireFormatLite::GetTagFieldNumber(tag);
      switch (field_number) {
        case pb::FlightData::kFlightDescriptorFieldNumber: {
          pb::FlightDescriptor pb_descriptor;
          if (!pb_descriptor.ParseFromCodedStream(&pb_stream)) {
            return FailSerialization(
                Status(StatusCode::INTERNAL, "Unable to parse FlightDescriptor"));
          }
        } break;
        case pb::FlightData::kDataHeaderFieldNumber: {
          if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->metadata)) {
            return FailSerialization(
                Status(StatusCode::INTERNAL, "Unable to read FlightData metadata"));
          }
        } break;
        case pb::FlightData::kDataBodyFieldNumber: {
          if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->body)) {
            return FailSerialization(
                Status(StatusCode::INTERNAL, "Unable to read FlightData body"));
          }
        } break;
        default:
          DCHECK(false) << "cannot happen";
      }
    }
    buffer->Clear();

    // TODO(wesm): Where and when should we verify that the FlightData is not
    // malformed or missing components?

    return Status::OK;
  }
};

// Write FlightData to a grpc::ByteBuffer without extra copying
template <>
class SerializationTraits<IpcPayload> {
 public:
  static grpc::Status Deserialize(ByteBuffer* buffer, IpcPayload* out) {
    return FailSerialization(grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                                          "IpcPayload deserialization not implemented"));
  }

  static grpc::Status Serialize(const IpcPayload& msg, ByteBuffer* out,
                                bool* own_buffer) {
    size_t total_size = 0;

    DCHECK_LT(msg.metadata->size(), kInt32Max);
    const int32_t metadata_size = static_cast<int32_t>(msg.metadata->size());

    // 1 byte for metadata tag
    total_size += 1 + WireFormatLite::LengthDelimitedSize(metadata_size);

    int64_t body_size = 0;
    for (const auto& buffer : msg.body_buffers) {
      // Buffer may be null when the row length is zero, or when all
      // entries are invalid.
      if (!buffer) continue;

      body_size += buffer->size();

      const int64_t remainder = buffer->size() % 8;
      if (remainder) {
        body_size += 8 - remainder;
      }
    }

    // 2 bytes for body tag
    // Only written when there are body buffers
    if (msg.body_length > 0) {
      total_size +=
          2 + WireFormatLite::LengthDelimitedSize(static_cast<size_t>(body_size));
    }

    // TODO(wesm): messages over 2GB unlikely to be yet supported
    if (total_size > kInt32Max) {
      return FailSerialization(
          grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                       "Cannot send record batches exceeding 2GB yet"));
    }

    // Allocate slice, assign to output buffer
    grpc::Slice slice(total_size);

    // XXX(wesm): for debugging
    // std::cout << "Writing record batch with total size " << total_size << std::endl;

    FixedSizeProtoWriter writer(*reinterpret_cast<grpc_slice*>(&slice));
    CodedOutputStream pb_stream(&writer);

    // Write header
    WireFormatLite::WriteTag(pb::FlightData::kDataHeaderFieldNumber,
                             WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
    pb_stream.WriteVarint32(metadata_size);
    pb_stream.WriteRawMaybeAliased(msg.metadata->data(),
                                   static_cast<int>(msg.metadata->size()));

    // Don't write tag if there are no body buffers
    if (msg.body_length > 0) {
      // Write body
      WireFormatLite::WriteTag(pb::FlightData::kDataBodyFieldNumber,
                               WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
      pb_stream.WriteVarint32(static_cast<uint32_t>(body_size));

      constexpr uint8_t kPaddingBytes[8] = {0};

      for (const auto& buffer : msg.body_buffers) {
        // Buffer may be null when the row length is zero, or when all
        // entries are invalid.
        if (!buffer) continue;

        pb_stream.WriteRawMaybeAliased(buffer->data(), static_cast<int>(buffer->size()));

        // Write padding if not multiple of 8
        const int remainder = static_cast<int>(buffer->size() % 8);
        if (remainder) {
          pb_stream.WriteRawMaybeAliased(kPaddingBytes, 8 - remainder);
        }
      }
    }

    DCHECK_EQ(static_cast<int>(total_size), pb_stream.ByteCount());

    // Hand off the slice to the returned ByteBuffer
    grpc::ByteBuffer tmp(&slice, 1);
    out->Swap(&tmp);
    *own_buffer = true;
    return grpc::Status::OK;
  }
};

}  // namespace grpc
