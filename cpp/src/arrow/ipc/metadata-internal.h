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

// Internal metadata serialization matters

#ifndef ARROW_IPC_METADATA_INTERNAL_H
#define ARROW_IPC_METADATA_INTERNAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/ipc/Schema_generated.h"
#include "arrow/ipc/dictionary.h"

namespace arrow {

class Buffer;
class DataType;
class Schema;
class Status;
class Tensor;

namespace flatbuf = org::apache::arrow::flatbuf;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {
namespace internal {

static constexpr flatbuf::MetadataVersion kCurrentMetadataVersion =
    flatbuf::MetadataVersion_V3;

static constexpr flatbuf::MetadataVersion kMinMetadataVersion =
    flatbuf::MetadataVersion_V3;

static constexpr const char* kArrowMagicBytes = "ARROW1";

struct FieldMetadata {
  int64_t length;
  int64_t null_count;
  int64_t offset;
};

struct BufferMetadata {
  /// The shared memory page id where to find this. Set to -1 if unused
  int32_t page;

  /// The relative offset into the memory page to the starting byte of the buffer
  int64_t offset;

  /// Absolute length in bytes of the buffer
  int64_t length;
};

struct FileBlock {
  int64_t offset;
  int32_t metadata_length;
  int64_t body_length;
};

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

// Retrieve a list of all the dictionary ids and types required by the schema for
// reconstruction. The presumption is that these will be loaded either from
// the stream or file (or they may already be somewhere else in memory)
Status GetDictionaryTypes(const void* opaque_schema, DictionaryTypeMap* id_to_field);

// Construct a complete Schema from the message. May be expensive for very
// large schemas if you are only interested in a few fields
Status GetSchema(const void* opaque_schema, const DictionaryMemo& dictionary_memo,
                 std::shared_ptr<Schema>* out);

Status GetTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                         std::vector<int64_t>* shape, std::vector<int64_t>* strides,
                         std::vector<std::string>* dim_names);

/// Write a serialized message metadata with a length-prefix and padding to an
/// 8-byte offset
///
/// <message_size: int32><message: const void*><padding>
Status WriteMessage(const Buffer& message, io::OutputStream* file,
                    int32_t* message_length);

// Serialize arrow::Schema as a Flatbuffer
//
// \param[in] schema a Schema instance
// \param[inout] dictionary_memo class for tracking dictionaries and assigning
// dictionary ids
// \param[out] out the serialized arrow::Buffer
// \return Status outcome
Status WriteSchemaMessage(const Schema& schema, DictionaryMemo* dictionary_memo,
                          std::shared_ptr<Buffer>* out);

Status WriteRecordBatchMessage(const int64_t length, const int64_t body_length,
                               const std::vector<FieldMetadata>& nodes,
                               const std::vector<BufferMetadata>& buffers,
                               std::shared_ptr<Buffer>* out);

Status WriteTensorMessage(const Tensor& tensor, const int64_t buffer_start_offset,
                          std::shared_ptr<Buffer>* out);

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
                       const std::vector<FileBlock>& record_batches,
                       DictionaryMemo* dictionary_memo, io::OutputStream* out);

Status WriteDictionaryMessage(const int64_t id, const int64_t length,
                              const int64_t body_length,
                              const std::vector<FieldMetadata>& nodes,
                              const std::vector<BufferMetadata>& buffers,
                              std::shared_ptr<Buffer>* out);

}  // namespace internal
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_H
