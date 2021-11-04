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

#include "arrow/buffer.h"
#include "arrow/result.h"

#include <memory>
#include <vector>

namespace grpc {

class Slice;

};  // namespace grpc

namespace arrow {
namespace flight {

struct FlightPayload;

namespace internal {

struct FlightData;

// Reader buffer management
// # data plane receives data and creates/stores to one reader buffer
// # Deserialize() creates a new shared_ptr to hold the buffer, creates a
//   gprc slice per that buffer, and installs destroyer callback, which
//   deletes the created shared_ptr when grpc slice is freed
// # Deserialize() calls FlightDataDeserialize() which transfers grpc slice
//   lifecyce to Buffer object (FlightData->body) managed by the end consumer
//   * see GrpcBuffer::slice_ in serialization_internal.cc
// # releasing the reader buffer
//   # end consumer frees FlightData
//   # frees Buffer(GrpcBuffer) object
//   # frees grpc slice GrpcBuffer::slice_
//   # invokes destroyer to release reader buffer

// Reader buffer must be a continuous memory block, see GrpcBuffer::Wrap
// - FlightDataDeserialize will copy and flatten non-continuous blocks anyway
// - it's necessary to get destroyer called

Status Deserialize(std::shared_ptr<Buffer> buffer, FlightData* data);

// Writer buffer management
// # FlightDataSerialize() holds buffer (FlightPayload.ipc_msg.body_buffer[i])
//   in returned grpc bbuf
//   * see SliceFromBuffer in serialization_internal.cc
// # dump grpc bbuf to a vector of grpc slice, then move to SerializeSlice[]
// # data plane sends data per returned SerializeSlice[]
// # releasing the writer buffer
//   # data plane frees vector<SerializeSlice>
//   # frees grpc slice SerializeSlice::slice_
//   # release writer buffer

// a simple wrapper of grpc::Slice, the only purpose is to hide grpc
// from data plane implementation
class SerializeSlice {
 public:
  explicit SerializeSlice(grpc::Slice&& slice);
  SerializeSlice(SerializeSlice&&);
  ~SerializeSlice();

  const uint8_t* data() const;
  int64_t size() const;

 private:
  std::unique_ptr<grpc::Slice> slice_;
};

arrow::Result<std::vector<SerializeSlice>> Serialize(const FlightPayload& payload,
                                                     int64_t* total_size = NULLPTR);

}  // namespace internal
}  // namespace flight
}  // namespace arrow
