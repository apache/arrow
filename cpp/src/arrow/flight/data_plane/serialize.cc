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

#include "arrow/flight/data_plane/serialize.h"
#include "arrow/flight/customize_protobuf.h"
#include "arrow/flight/internal.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"

#include <grpc/byte_buffer.h>
#include <grpc/slice.h>

#include <cstring>

namespace arrow {
namespace flight {
namespace internal {

namespace {

void ReleaseBuffer(void* buffer_ptr) {
  delete reinterpret_cast<std::shared_ptr<Buffer>*>(buffer_ptr);
}

}  // namespace

Status Deserialize(std::shared_ptr<Buffer> buffer, FlightData* data) {
  // hold the buffer
  std::shared_ptr<Buffer>* buffer_ptr = new std::shared_ptr<Buffer>(std::move(buffer));

  const grpc::Slice slice((*buffer_ptr)->mutable_data(),
                          static_cast<size_t>((*buffer_ptr)->size()), &ReleaseBuffer,
                          buffer_ptr);
  grpc::ByteBuffer bbuf = grpc::ByteBuffer(&slice, 1);

  {
    // make sure GrpcBuffer::Wrap goes the wanted path
    auto grpc_bbuf = *reinterpret_cast<grpc_byte_buffer**>(&bbuf);
    DCHECK_EQ(grpc_bbuf->type, GRPC_BB_RAW);
    DCHECK_EQ(grpc_bbuf->data.raw.compression, GRPC_COMPRESS_NONE);
    DCHECK_EQ(grpc_bbuf->data.raw.slice_buffer.count, 1);
    grpc_slice slice = grpc_bbuf->data.raw.slice_buffer.slices[0];
    DCHECK_NE(slice.refcount, 0);
  }

  // buffer ownership is transferred to "data" on success
  const Status st = FromGrpcStatus(FlightDataDeserialize(&bbuf, data));
  if (!st.ok()) {
    delete buffer_ptr;
  }
  return st;
}

SerializeSlice::SerializeSlice(grpc::Slice&& slice) {
  slice_ = arrow::internal::make_unique<grpc::Slice>(std::move(slice));
}
SerializeSlice::SerializeSlice(SerializeSlice&&) = default;
SerializeSlice::~SerializeSlice() = default;

const uint8_t* SerializeSlice::data() const { return slice_->begin(); }
int64_t SerializeSlice::size() const { return static_cast<int64_t>(slice_->size()); }

arrow::Result<std::vector<SerializeSlice>> Serialize(const FlightPayload& payload,
                                                     int64_t* total_size) {
  RETURN_NOT_OK(payload.Validate());

  grpc::ByteBuffer bbuf;
  bool owner;
  RETURN_NOT_OK(FromGrpcStatus(FlightDataSerialize(payload, &bbuf, &owner)));

  if (total_size) {
    *total_size = static_cast<int64_t>(bbuf.Length());
  }

  // ByteBuffer::Dump doesn't copy data buffer, IIUC
  std::vector<grpc::Slice> grpc_slices;
  RETURN_NOT_OK(FromGrpcStatus(bbuf.Dump(&grpc_slices)));

  // move grpc slice life cycle to returned serialize slice
  std::vector<SerializeSlice> slices;
  for (auto& grpc_slice : grpc_slices) {
    SerializeSlice slice(std::move(grpc_slice));
    slices.emplace_back(std::move(slice));
  }
  return slices;
}

}  // namespace internal
}  // namespace flight
}  // namespace arrow
