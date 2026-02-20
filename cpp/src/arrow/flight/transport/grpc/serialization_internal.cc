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
#include <memory>
#include <string>
#include <vector>

#include "arrow/flight/platform.h"

#if defined(_MSC_VER)
#  pragma warning(push)
#  pragma warning(disable : 4267)
#endif

#include <grpc/byte_buffer_reader.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>

#if defined(_MSC_VER)
#  pragma warning(pop)
#endif

#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/transport.h"
#include "arrow/flight/transport/grpc/util_internal.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/logging_internal.h"

namespace arrow {
namespace flight {
namespace transport {
namespace grpc {

namespace pb = arrow::flight::protocol;

static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();

using ::grpc::ByteBuffer;

namespace {

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
      if (slice.refcount) {
        // Steal the slice reference
        *out = std::make_shared<GrpcBuffer>(slice, false);
      } else {
        // grpc_byte_buffer_reader_readall can give us an inlined slice,
        // copy the data as above
        const uint8_t length = slice.data.inlined.length;
        ARROW_ASSIGN_OR_RAISE(*out, arrow::AllocateBuffer(length));
        std::memcpy((*out)->mutable_data(), slice.data.inlined.bytes, length);
      }
      grpc_byte_buffer_reader_destroy(&reader);
    }

    return Status::OK();
  }

 private:
  grpc_slice slice_;
};

// Destructor callback for grpc::Slice
void ReleaseBuffer(void* buf_ptr) {
  delete reinterpret_cast<std::shared_ptr<Buffer>*>(buf_ptr);
}

}  // namespace

// Initialize gRPC Slice from arrow Buffer
arrow::Result<::grpc::Slice> SliceFromBuffer(const std::shared_ptr<Buffer>& buf) {
  // Allocate persistent shared_ptr to control Buffer lifetime
  std::shared_ptr<Buffer>* ptr = nullptr;
  if (ARROW_PREDICT_TRUE(buf->is_cpu())) {
    ptr = new std::shared_ptr<Buffer>(buf);
  } else {
    // Non-CPU buffer, must copy to CPU-accessible buffer first
    ARROW_ASSIGN_OR_RAISE(auto cpu_buf,
                          Buffer::ViewOrCopy(buf, default_cpu_memory_manager()));
    ptr = new std::shared_ptr<Buffer>(cpu_buf);
  }
  ::grpc::Slice slice(const_cast<uint8_t*>((*ptr)->data()),
                      static_cast<size_t>((*ptr)->size()), &ReleaseBuffer, ptr);
  // Make sure no copy was done (some grpc::Slice() constructors do an implicit memcpy)
  DCHECK_EQ(slice.begin(), (*ptr)->data());
  return slice;
}

::grpc::Status FlightDataSerialize(const FlightPayload& msg, ByteBuffer* out,
                                   bool* own_buffer) {
  // TODO(wesm): messages over 2GB unlikely to be yet supported
  // Validated in WritePayload since returning error here causes gRPC to fail an assertion
  DCHECK_LE(msg.ipc_message.body_length, kInt32Max);

  // Retrieve BufferVector from the FlightPayload's IPC message.
  auto buffers_result = msg.SerializeToBuffers();
  if (!buffers_result.ok()) {
    return ToGrpcStatus(buffers_result.status());
  }

  std::vector<::grpc::Slice> slices;
  slices.reserve(buffers_result->size());
  for (const auto& buffer : *buffers_result) {
    ::grpc::Slice slice;
    auto status = SliceFromBuffer(buffer).Value(&slice);
    if (ARROW_PREDICT_FALSE(!status.ok())) {
      // This will likely lead to abort as gRPC cannot recover from an error here
      return ToGrpcStatus(status);
    }
    slices.push_back(std::move(slice));
  }

  *out = ByteBuffer(slices.data(), slices.size());
  *own_buffer = true;
  return ::grpc::Status::OK;
}

arrow::Status WrapGrpcBuffer(::grpc::ByteBuffer* grpc_buf,
                             std::shared_ptr<arrow::Buffer>* out) {
  ARROW_RETURN_NOT_OK(GrpcBuffer::Wrap(grpc_buf, out));
  grpc_buf->Clear();
  return arrow::Status::OK();
}

// Read internal::FlightData from grpc::ByteBuffer containing FlightData
// protobuf without copying
::grpc::Status FlightDataDeserialize(ByteBuffer* buffer,
                                     arrow::flight::internal::FlightData* out) {
  if (!buffer) {
    return {::grpc::StatusCode::INTERNAL, "No payload"};
  }

  std::shared_ptr<arrow::Buffer> wrapped_buffer;
  GRPC_RETURN_NOT_OK(GrpcBuffer::Wrap(buffer, &wrapped_buffer));
  // Release gRPC memory now that Arrow Buffer holds its own reference.
  buffer->Clear();

  auto result = arrow::flight::internal::DeserializeFlightData(wrapped_buffer);
  if (!result.ok()) {
    return ToGrpcStatus(result.status());
  }
  *out = result.MoveValueUnsafe();
  return ::grpc::Status::OK;
}

// The pointer bitcast hack below causes legitimate warnings, silence them.
#ifndef _WIN32
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif

// Pointer bitcast explanation: grpc::*Writer<T>::Write() and grpc::*Reader<T>::Read()
// both take a T* argument (here pb::FlightData*).  But they don't do anything
// with that argument except pass it to SerializationTraits<T>::Serialize() and
// SerializationTraits<T>::Deserialize().
//
// Since we control SerializationTraits<pb::FlightData>, we can interpret the
// pointer argument whichever way we want, including cast it back to the original type.
// (see customize_grpc.h).

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
#  pragma GCC diagnostic pop
#endif

}  // namespace grpc
}  // namespace transport
}  // namespace flight
}  // namespace arrow
