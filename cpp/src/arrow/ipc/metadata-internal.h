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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata.h"
#include "arrow/util/macros.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

class Array;
class Buffer;
struct Field;
class Schema;
class Status;

namespace ipc {

using FBB = flatbuffers::FlatBufferBuilder;
using FieldOffset = flatbuffers::Offset<arrow::flatbuf::Field>;
using VectorLayoutOffset = flatbuffers::Offset<arrow::flatbuf::VectorLayout>;
using Offset = flatbuffers::Offset<void>;

static constexpr flatbuf::MetadataVersion kMetadataVersion = flatbuf::MetadataVersion_V2;

Status FieldFromFlatbuffer(const flatbuf::Field* field, std::shared_ptr<Field>* out);

Status SchemaToFlatbuffer(
    FBB& fbb, const Schema& schema, flatbuffers::Offset<flatbuf::Schema>* out);

// Memoization data structure for handling shared dictionaries
class DictionaryMemo {
 public:
  DictionaryMemo();

  // Returns KeyError if dictionary not found
  Status GetDictionary(int32_t id, std::shared_ptr<Array>* dictionary) const;

  int32_t GetId(const std::shared_ptr<Array> dictionary);

  bool HasDictionary(const std::shared_ptr<Array> dictionary) const;
  bool HasDictionaryId(int32_t id) const;

  // Add a dictionary to the memo with a particular id. Returns KeyError if
  // that dictionary already exists
  Status AddDictionary(int32_t id, const std::shared_ptr<Array>& dictionary);

 private:
  // Dictionary memory addresses, to track whether a dictionary has been seen
  // before
  std::unordered_map<intptr_t, int32_t> dictionary_to_id_;

  // Map of dictionary id to dictionary array
  std::unordered_map<int32_t, std::shared_ptr<Array>> id_to_dictionary_;

  DISALLOW_COPY_AND_ASSIGN(DictionaryMemo);
};

class MessageBuilder {
 public:
  Status SetSchema(const Schema& schema);

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

Status WriteRecordBatchMetadata(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_INTERNAL_H
