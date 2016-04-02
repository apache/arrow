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

#ifndef ARROW_IPC_METADATA_INTERNAL_H
#define ARROW_IPC_METADATA_INTERNAL_H

#include <flatbuffers/flatbuffers.h>
#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/Message_generated.h"

namespace arrow {

namespace flatbuf = apache::arrow::flatbuf;

class Buffer;
struct Field;
class Schema;
class Status;

namespace ipc {

Status FieldFromFlatbuffer(const flatbuf::Field* field, std::shared_ptr<Field>* out);

class MessageBuilder {
 public:
  Status SetSchema(const Schema* schema);

  Status SetRecordBatch(int32_t length, int64_t body_length,
      const std::vector<flatbuf::FieldNode>& nodes,
      const std::vector<flatbuf::Buffer>& buffers);

  Status Finish();

  Status GetBuffer(std::shared_ptr<Buffer>* out);

 private:
  flatbuf::MessageHeader header_type_;
  flatbuffers::Offset<void> header_;
  int64_t body_length_;
  flatbuffers::FlatBufferBuilder fbb_;
};

Status WriteDataHeader(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_INTERNAL_H
