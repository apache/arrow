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

#include "arrow/flight/serialization_internal.h"

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
#include "arrow/flight/server.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();

namespace arrow {
namespace flight {
namespace internal {

namespace pb = arrow::flight::protocol;

using arrow::ipc::IpcPayload;

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::ArrayOutputStream;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

using grpc::ByteBuffer;

bool ReadBytesZeroCopy(const std::shared_ptr<Buffer>& source_data,
                       CodedInputStream* input, std::shared_ptr<Buffer>* out) {
  uint32_t length;
  if (!input->ReadVarint32(&length)) {
    return false;
  }
  auto buf =
      SliceBuffer(source_data, input->CurrentPosition(), static_cast<int64_t>(length));
  *out = buf;
  return input->Skip(static_cast<int>(length));
}

// Internal wrapper for gRPC ByteBuffer so its memory can be exposed to Arrow
// consumers with zero-copy
class GrpcBuffer : public MutableBuffer {
 public:
  GrpcBuffer(grpc_slice slice, bool incref)
      : MutableBuffer(GRPC_SLICE_START_PTR(slice),
                      static_cast<int64_t>(GRPC_SLICE_LENGTH(slice))),
        slice_(incref ? grpc_slice_ref(slice) : slice) {}

  ~GrpcBuffer() override {
    // Decref slice
    grpc_slice_unref(slice_);
  }

  static Status Wrap(ByteBuffer* cpp_buf, std::shared_ptr<Buffer>* out) {
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

      if (slice.refcount) {
        // Increment reference count so this memory remains valid
        *out = std::make_shared<GrpcBuffer>(slice, true);
      } else {
        // Small slices (less than GRPC_SLICE_INLINED_SIZE bytes) are
        // inlined into the structure and must be copied.
        const uint8_t length = slice.data.inlined.length;
        ARROW_ASSIGN_OR_RAISE(*out, arrow::AllocateBuffer(length));
        std::memcpy((*out)->mutable_data(), slice.data.inlined.bytes, length);
      }
    } else {
      // Otherwise, we need to use `grpc_byte_buffer_reader_readall` to read
      // `buffer` into a single contiguous `grpc_slice`. The gRPC reader gives
      // us back a new slice with the refcount already incremented.
      grpc_byte_buffer_reader reader;
      if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return Status::IOError("Internal gRPC error reading from ByteBuffer");
      }
      grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
      grpc_byte_buffer_reader_destroy(&reader);

      // Steal the slice reference
      *out = std::make_shared<GrpcBuffer>(slice, false);
    }

    return Status::OK();
  }

 private:
  grpc_slice slice_;
};

// Destructor callback for grpc::Slice
static void ReleaseBuffer(void* buf_ptr) {
  delete reinterpret_cast<std::shared_ptr<Buffer>*>(buf_ptr);
}

// Initialize gRPC Slice from arrow Buffer
grpc::Slice SliceFromBuffer(const std::shared_ptr<Buffer>& buf) {
  // Allocate persistent shared_ptr to control Buffer lifetime
  auto ptr = new std::shared_ptr<Buffer>(buf);
  grpc::Slice slice(const_cast<uint8_t*>(buf->data()), static_cast<size_t>(buf->size()),
                    &ReleaseBuffer, ptr);
  // Make sure no copy was done (some grpc::Slice() constructors do an implicit memcpy)
  DCHECK_EQ(slice.begin(), buf->data());
  return slice;
}

static const uint8_t kPaddingBytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};

// Update the sizes of our Protobuf fields based on the given IPC payload.
grpc::Status IpcMessageHeaderSize(const arrow::ipc::IpcPayload& ipc_msg, bool has_body,
                                  size_t* header_size, int32_t* metadata_size) {
  DCHECK_LE(ipc_msg.metadata->size(), kInt32Max);
  *metadata_size = static_cast<int32_t>(ipc_msg.metadata->size());

  // 1 byte for metadata tag
  *header_size += 1 + WireFormatLite::LengthDelimitedSize(*metadata_size);

  // 2 bytes for body tag
  if (has_body) {
    // We write the body tag in the header but not the actual body data
    *header_size += 2 + WireFormatLite::LengthDelimitedSize(ipc_msg.body_length) -
                    ipc_msg.body_length;
  }

  return grpc::Status::OK;
}

grpc::Status FlightDataSerialize(const FlightPayload& msg, ByteBuffer* out,
                                 bool* own_buffer) {
  // Size of the IPC body (protobuf: data_body)
  size_t body_size = 0;
  // Size of the Protobuf "header" (everything except for the body)
  size_t header_size = 0;
  // Size of IPC header metadata (protobuf: data_header)
  int32_t metadata_size = 0;

  // Write the descriptor if present
  int32_t descriptor_size = 0;
  if (msg.descriptor != nullptr) {
    DCHECK_LE(msg.descriptor->size(), kInt32Max);
    descriptor_size = static_cast<int32_t>(msg.descriptor->size());
    header_size += 1 + WireFormatLite::LengthDelimitedSize(descriptor_size);
  }

  // App metadata tag if appropriate
  int32_t app_metadata_size = 0;
  if (msg.app_metadata && msg.app_metadata->size() > 0) {
    DCHECK_LE(msg.app_metadata->size(), kInt32Max);
    app_metadata_size = static_cast<int32_t>(msg.app_metadata->size());
    header_size += 1 + WireFormatLite::LengthDelimitedSize(app_metadata_size);
  }

  const arrow::ipc::IpcPayload& ipc_msg = msg.ipc_message;
  // No data in this payload (metadata-only).
  bool has_ipc = ipc_msg.type != ipc::MessageType::NONE;
  bool has_body = has_ipc ? ipc::Message::HasBody(ipc_msg.type) : false;

  if (has_ipc) {
    DCHECK(has_body || ipc_msg.body_length == 0);
    GRPC_RETURN_NOT_GRPC_OK(
        IpcMessageHeaderSize(ipc_msg, has_body, &header_size, &metadata_size));
    body_size = static_cast<size_t>(ipc_msg.body_length);
  }

  // TODO(wesm): messages over 2GB unlikely to be yet supported
  // Validated in WritePayload since returning error here causes gRPC to fail an assertion
  DCHECK_LE(body_size, kInt32Max);

  // Allocate and initialize slices
  std::vector<grpc::Slice> slices;
  slices.emplace_back(header_size);

  // Force the header_stream to be destructed, which actually flushes
  // the data into the slice.
  {
    ArrayOutputStream header_writer(const_cast<uint8_t*>(slices[0].begin()),
                                    static_cast<int>(slices[0].size()));
    CodedOutputStream header_stream(&header_writer);

    // Write descriptor
    if (msg.descriptor != nullptr) {
      WireFormatLite::WriteTag(pb::FlightData::kFlightDescriptorFieldNumber,
                               WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &header_stream);
      header_stream.WriteVarint32(descriptor_size);
      header_stream.WriteRawMaybeAliased(msg.descriptor->data(),
                                         static_cast<int>(msg.descriptor->size()));
    }

    // Write header
    if (has_ipc) {
      WireFormatLite::WriteTag(pb::FlightData::kDataHeaderFieldNumber,
                               WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &header_stream);
      header_stream.WriteVarint32(metadata_size);
      header_stream.WriteRawMaybeAliased(ipc_msg.metadata->data(),
                                         static_cast<int>(ipc_msg.metadata->size()));
    }

    // Write app metadata
    if (app_metadata_size > 0) {
      WireFormatLite::WriteTag(pb::FlightData::kAppMetadataFieldNumber,
                               WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &header_stream);
      header_stream.WriteVarint32(app_metadata_size);
      header_stream.WriteRawMaybeAliased(msg.app_metadata->data(),
                                         static_cast<int>(msg.app_metadata->size()));
    }

    if (has_body) {
      // Write body tag
      WireFormatLite::WriteTag(pb::FlightData::kDataBodyFieldNumber,
                               WireFormatLite::WIRETYPE_LENGTH_DELIMITED, &header_stream);
      header_stream.WriteVarint32(static_cast<uint32_t>(body_size));

      // Enqueue body buffers for writing, without copying
      for (const auto& buffer : ipc_msg.body_buffers) {
        // Buffer may be null when the row length is zero, or when all
        // entries are invalid.
        if (!buffer) continue;

        slices.push_back(SliceFromBuffer(buffer));

        // Write padding if not multiple of 8
        const auto remainder = static_cast<int>(
            bit_util::RoundUpToMultipleOf8(buffer->size()) - buffer->size());
        if (remainder) {
          slices.push_back(grpc::Slice(kPaddingBytes, remainder));
        }
      }
    }

    DCHECK_EQ(static_cast<int>(header_size), header_stream.ByteCount());
  }

  // Hand off the slices to the returned ByteBuffer
  *out = grpc::ByteBuffer(slices.data(), slices.size());
  *own_buffer = true;
  return grpc::Status::OK;
}

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
grpc::Status FlightDataDeserialize(ByteBuffer* buffer, FlightData* out) {
  if (!buffer) {
    return grpc::Status(grpc::StatusCode::INTERNAL, "No payload");
  }

  // Reset fields in case the caller reuses a single allocation
  out->descriptor = nullptr;
  out->app_metadata = nullptr;
  out->metadata = nullptr;
  out->body = nullptr;

  std::shared_ptr<arrow::Buffer> wrapped_buffer;
  GRPC_RETURN_NOT_OK(GrpcBuffer::Wrap(buffer, &wrapped_buffer));

  auto buffer_length = static_cast<int>(wrapped_buffer->size());
  CodedInputStream pb_stream(wrapped_buffer->data(), buffer_length);

  pb_stream.SetTotalBytesLimit(buffer_length);

  // This is the bytes remaining when using CodedInputStream like this
  while (pb_stream.BytesUntilTotalBytesLimit()) {
    const uint32_t tag = pb_stream.ReadTag();
    const int field_number = WireFormatLite::GetTagFieldNumber(tag);
    switch (field_number) {
      case pb::FlightData::kFlightDescriptorFieldNumber: {
        pb::FlightDescriptor pb_descriptor;
        uint32_t length;
        if (!pb_stream.ReadVarint32(&length)) {
          return grpc::Status(grpc::StatusCode::INTERNAL,
                              "Unable to parse length of FlightDescriptor");
        }
        // Can't use ParseFromCodedStream as this reads the entire
        // rest of the stream into the descriptor command field.
        std::string buffer;
        pb_stream.ReadString(&buffer, length);
        if (!pb_descriptor.ParseFromString(buffer)) {
          return grpc::Status(grpc::StatusCode::INTERNAL,
                              "Unable to parse FlightDescriptor");
        }
        arrow::flight::FlightDescriptor descriptor;
        GRPC_RETURN_NOT_OK(
            arrow::flight::internal::FromProto(pb_descriptor, &descriptor));
        out->descriptor.reset(new arrow::flight::FlightDescriptor(descriptor));
      } break;
      case pb::FlightData::kDataHeaderFieldNumber: {
        if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->metadata)) {
          return grpc::Status(grpc::StatusCode::INTERNAL,
                              "Unable to read FlightData metadata");
        }
      } break;
      case pb::FlightData::kAppMetadataFieldNumber: {
        if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->app_metadata)) {
          return grpc::Status(grpc::StatusCode::INTERNAL,
                              "Unable to read FlightData application metadata");
        }
      } break;
      case pb::FlightData::kDataBodyFieldNumber: {
        if (!ReadBytesZeroCopy(wrapped_buffer, &pb_stream, &out->body)) {
          return grpc::Status(grpc::StatusCode::INTERNAL,
                              "Unable to read FlightData body");
        }
      } break;
      default:
        DCHECK(false) << "cannot happen";
    }
  }
  buffer->Clear();

  // TODO(wesm): Where and when should we verify that the FlightData is not
  // malformed?

  // Set the default value for an unspecified FlightData body. The other
  // fields can be null if they're unspecified.
  if (out->body == nullptr) {
    out->body = std::make_shared<Buffer>(nullptr, 0);
  }

  return grpc::Status::OK;
}

::arrow::Result<std::unique_ptr<ipc::Message>> FlightData::OpenMessage() {
  return ipc::Message::Open(metadata, body);
}

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

Status WritePayload(const FlightPayload& payload,
                    grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  if (!writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                     grpc::WriteOptions())) {
    return Status::IOError("Could not write payload to stream");
  }
  return Status::OK();
}

Status WritePayload(const FlightPayload& payload,
                    grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  if (!writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                     grpc::WriteOptions())) {
    return Status::IOError("Could not write payload to stream");
  }
  return Status::OK();
}

Status WritePayload(const FlightPayload& payload,
                    grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  if (!writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                     grpc::WriteOptions())) {
    return Status::IOError("Could not write payload to stream");
  }
  return Status::OK();
}

Status WritePayload(const FlightPayload& payload,
                    grpc::ServerWriter<pb::FlightData>* writer) {
  RETURN_NOT_OK(payload.Validate());
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  if (!writer->Write(*reinterpret_cast<const pb::FlightData*>(&payload),
                     grpc::WriteOptions())) {
    return Status::IOError("Could not write payload to stream");
  }
  return Status::OK();
}

bool ReadPayload(grpc::ClientReader<pb::FlightData>* reader, FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(grpc::ServerReaderWriter<pb::PutResult, pb::FlightData>* reader,
                 FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(grpc::ServerReaderWriter<pb::FlightData, pb::FlightData>* reader,
                 FlightData* data) {
  // Pretend to be pb::FlightData and intercept in SerializationTraits
  return reader->Read(reinterpret_cast<pb::FlightData*>(data));
}

bool ReadPayload(grpc::ClientReaderWriter<pb::FlightData, pb::PutResult>* reader,
                 pb::PutResult* data) {
  return reader->Read(data);
}

#ifndef _WIN32
#pragma GCC diagnostic pop
#endif

}  // namespace internal
}  // namespace flight
}  // namespace arrow
