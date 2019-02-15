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

#include "arrow/flight/serialization-internal.h"

#include <string>

#include "arrow/buffer.h"
#include "arrow/flight/server.h"
#include "arrow/ipc/writer.h"

namespace arrow {
namespace flight {
namespace internal {

bool ReadBytesZeroCopy(const std::shared_ptr<arrow::Buffer>& source_data,
                       CodedInputStream* input, std::shared_ptr<arrow::Buffer>* out) {
  uint32_t length;
  if (!input->ReadVarint32(&length)) {
    return false;
  }
  *out = arrow::SliceBuffer(source_data, input->CurrentPosition(),
                            static_cast<int64_t>(length));
  return input->Skip(static_cast<int>(length));
}

using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

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
using arrow::flight::internal::GrpcBuffer;
using arrow::flight::internal::ReadBytesZeroCopy;

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

Status FlightDataSerialize(const FlightPayload& msg, ByteBuffer* out, bool* own_buffer) {
  size_t total_size = 0;

  // Write the descriptor if present
  int32_t descriptor_size = 0;
  if (msg.descriptor != nullptr) {
    DCHECK_LT(msg.descriptor->size(), kInt32Max);
    descriptor_size = static_cast<int32_t>(msg.descriptor->size());
    total_size += 1 + WireFormatLite::LengthDelimitedSize(descriptor_size);
  }

  const arrow::ipc::internal::IpcPayload& ipc_msg = msg.ipc_message;

  DCHECK_LT(ipc_msg.metadata->size(), kInt32Max);
  const int32_t metadata_size = static_cast<int32_t>(ipc_msg.metadata->size());

  // 1 byte for metadata tag
  total_size += 1 + WireFormatLite::LengthDelimitedSize(metadata_size);

  int64_t body_size = 0;
  for (const auto& buffer : ipc_msg.body_buffers) {
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
  if (ipc_msg.body_length > 0) {
    total_size += 2 + WireFormatLite::LengthDelimitedSize(static_cast<size_t>(body_size));
  }

  // TODO(wesm): messages over 2GB unlikely to be yet supported
  if (total_size > kInt32Max) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                        "Cannot send record batches exceeding 2GB yet");
  }

  // Allocate slice, assign to output buffer
  grpc::Slice slice(total_size);

  // XXX(wesm): for debugging
  // std::cout << "Writing record batch with total size " << total_size << std::endl;

  ArrayOutputStream writer(const_cast<uint8_t*>(slice.begin()),
                           static_cast<int>(slice.size()));
  CodedOutputStream pb_stream(&writer);

  // Write descriptor
  if (msg.descriptor != nullptr) {
    WireFormatLite::WriteTag(pb::FlightData::kFlightDescriptorFieldNumber,
                             WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
    pb_stream.WriteVarint32(descriptor_size);
    pb_stream.WriteRawMaybeAliased(msg.descriptor->data(),
                                   static_cast<int>(msg.descriptor->size()));
  }

  // Write header
  WireFormatLite::WriteTag(pb::FlightData::kDataHeaderFieldNumber,
                           WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
  pb_stream.WriteVarint32(metadata_size);
  pb_stream.WriteRawMaybeAliased(ipc_msg.metadata->data(),
                                 static_cast<int>(ipc_msg.metadata->size()));

  // Don't write tag if there are no body buffers
  if (ipc_msg.body_length > 0) {
    // Write body
    WireFormatLite::WriteTag(pb::FlightData::kDataBodyFieldNumber,
                             WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &pb_stream);
    pb_stream.WriteVarint32(static_cast<uint32_t>(body_size));

    constexpr uint8_t kPaddingBytes[8] = {0};

    for (const auto& buffer : ipc_msg.body_buffers) {
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

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
Status FlightDataDeserialize(ByteBuffer* buffer, FlightData* out) {
  if (!buffer) {
    return Status(StatusCode::INTERNAL, "No payload");
  }

  std::shared_ptr<arrow::Buffer> wrapped_buffer;
  GRPC_RETURN_NOT_OK(GrpcBuffer::Wrap(buffer, &wrapped_buffer));

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
        uint32_t length;
        if (!pb_stream.ReadVarint32(&length)) {
          return Status(StatusCode::INTERNAL,
                        "Unable to parse length of FlightDescriptor");
        }
        // Can't use ParseFromCodedStream as this reads the entire
        // rest of the stream into the descriptor command field.
        std::string buffer;
        pb_stream.ReadString(&buffer, length);
        if (!pb_descriptor.ParseFromString(buffer)) {
          return Status(StatusCode::INTERNAL, "Unable to parse FlightDescriptor");
        }
        arrow::flight::FlightDescriptor descriptor;
        GRPC_RETURN_NOT_OK(
            arrow::flight::internal::FromProto(pb_descriptor, &descriptor));
        out->descriptor.reset(new arrow::flight::FlightDescriptor(descriptor));
      } break;
      case pb::FlightData::kDataHeaderFieldNumber: {
        if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->metadata)) {
          return Status(StatusCode::INTERNAL, "Unable to read FlightData metadata");
        }
      } break;
      case pb::FlightData::kDataBodyFieldNumber: {
        if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->body)) {
          return Status(StatusCode::INTERNAL, "Unable to read FlightData body");
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

}  // namespace grpc
