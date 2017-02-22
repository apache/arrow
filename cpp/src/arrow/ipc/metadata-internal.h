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
#include <vector>

#include "flatbuffers/flatbuffers.h"

#include "arrow/ipc/File_generated.h"
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/metadata.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

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

// Construct a field with type for a dictionary-encoded field. None of its
// children or children's descendents can be dictionary encoded
Status FieldFromFlatbufferDictionary(
    const flatbuf::Field* field, std::shared_ptr<Field>* out);

// Construct a field for a non-dictionary-encoded field. Its children may be
// dictionary encoded
Status FieldFromFlatbuffer(const flatbuf::Field* field,
    const DictionaryMemo& dictionary_memo, std::shared_ptr<Field>* out);

Status SchemaToFlatbuffer(FBB& fbb, const Schema& schema, DictionaryMemo* dictionary_memo,
    flatbuffers::Offset<flatbuf::Schema>* out);

// Serialize arrow::Schema as a Flatbuffer
//
// \param[in] schema a Schema instance
// \param[inout] dictionary_memo class for tracking dictionaries and assigning
// dictionary ids
// \param[out] out the serialized arrow::Buffer
// \return Status outcome
Status WriteSchemaMessage(
    const Schema& schema, DictionaryMemo* dictionary_memo, std::shared_ptr<Buffer>* out);

Status WriteRecordBatchMessage(int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out);

Status WriteDictionaryMessage(int64_t id, int32_t length, int64_t body_length,
    const std::vector<flatbuf::FieldNode>& nodes,
    const std::vector<flatbuf::Buffer>& buffers, std::shared_ptr<Buffer>* out);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_INTERNAL_H
