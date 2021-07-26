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

#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <flatbuffers/flatbuffers.h>

#include "arrow/buffer.h"
#include "arrow/io/type_fwd.h"
#include "arrow/ipc/message.h"
#include "arrow/result.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

#include "generated/Message_generated.h"
#include "generated/Schema_generated.h"
#include "generated/SparseTensor_generated.h"  // IWYU pragma: keep

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;

namespace ipc {

class DictionaryFieldMapper;
class DictionaryMemo;

namespace internal {

using KeyValueOffset = flatbuffers::Offset<flatbuf::KeyValue>;
using KVVector = flatbuffers::Vector<KeyValueOffset>;

// This 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
constexpr int32_t kIpcContinuationToken = -1;

static constexpr flatbuf::MetadataVersion kCurrentMetadataVersion =
    flatbuf::MetadataVersion::V5;

static constexpr flatbuf::MetadataVersion kLatestMetadataVersion =
    flatbuf::MetadataVersion::V5;

static constexpr flatbuf::MetadataVersion kMinMetadataVersion =
    flatbuf::MetadataVersion::V4;

// These functions are used in unit tests
ARROW_EXPORT
MetadataVersion GetMetadataVersion(flatbuf::MetadataVersion version);

ARROW_EXPORT
flatbuf::MetadataVersion MetadataVersionToFlatbuffer(MetadataVersion version);

// Whether the type has a validity bitmap in the given IPC version
bool HasValidityBitmap(Type::type type_id, MetadataVersion version);

static constexpr const char* kArrowMagicBytes = "ARROW1";

struct FieldMetadata {
  int64_t length;
  int64_t null_count;
  int64_t offset;
};

struct BufferMetadata {
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

// Low-level utilities to help with reading Flatbuffers data.

#define CHECK_FLATBUFFERS_NOT_NULL(fb_value, name)             \
  if ((fb_value) == NULLPTR) {                                 \
    return Status::IOError("Unexpected null field ", name,     \
                           " in flatbuffer-encoded metadata"); \
  }

template <typename T>
inline uint32_t FlatBuffersVectorSize(const flatbuffers::Vector<T>* vec) {
  return (vec == NULLPTR) ? 0 : vec->size();
}

inline std::string StringFromFlatbuffers(const flatbuffers::String* s) {
  return (s == NULLPTR) ? "" : s->str();
}

// Read interface classes. We do not fully deserialize the flatbuffers so that
// individual fields metadata can be retrieved from very large schema without
//

// Construct a complete Schema from the message and add
// dictionary-encoded fields to a DictionaryMemo instance. May be
// expensive for very large schemas if you are only interested in a
// few fields
Status GetSchema(const void* opaque_schema, DictionaryMemo* dictionary_memo,
                 std::shared_ptr<Schema>* out);

Status GetTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                         std::vector<int64_t>* shape, std::vector<int64_t>* strides,
                         std::vector<std::string>* dim_names);

// EXPERIMENTAL: Extracting metadata of a SparseCOOIndex from the message
Status GetSparseCOOIndexMetadata(const flatbuf::SparseTensorIndexCOO* sparse_index,
                                 std::shared_ptr<DataType>* indices_type);

// EXPERIMENTAL: Extracting metadata of a SparseCSXIndex from the message
Status GetSparseCSXIndexMetadata(const flatbuf::SparseMatrixIndexCSX* sparse_index,
                                 std::shared_ptr<DataType>* indptr_type,
                                 std::shared_ptr<DataType>* indices_type);

// EXPERIMENTAL: Extracting metadata of a SparseCSFIndex from the message
Status GetSparseCSFIndexMetadata(const flatbuf::SparseTensorIndexCSF* sparse_index,
                                 std::vector<int64_t>* axis_order,
                                 std::vector<int64_t>* indices_size,
                                 std::shared_ptr<DataType>* indptr_type,
                                 std::shared_ptr<DataType>* indices_type);

// EXPERIMENTAL: Extracting metadata of a sparse tensor from the message
Status GetSparseTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                               std::vector<int64_t>* shape,
                               std::vector<std::string>* dim_names, int64_t* length,
                               SparseTensorFormat::type* sparse_tensor_format_id);

Status GetKeyValueMetadata(const KVVector* fb_metadata,
                           std::shared_ptr<KeyValueMetadata>* out);

template <typename RootType>
bool VerifyFlatbuffers(const uint8_t* data, int64_t size) {
  // Heuristic: tables in a Arrow flatbuffers buffer must take at least 1 bit
  // each in average (ARROW-11559).
  // Especially, the only recursive table (the `Field` table in Schema.fbs)
  // must have a non-empty `type` member.
  flatbuffers::Verifier verifier(
      data, static_cast<size_t>(size),
      /*max_depth=*/128,
      /*max_tables=*/static_cast<flatbuffers::uoffset_t>(8 * size));
  return verifier.VerifyBuffer<RootType>(nullptr);
}

static inline Status VerifyMessage(const uint8_t* data, int64_t size,
                                   const flatbuf::Message** out) {
  if (!VerifyFlatbuffers<flatbuf::Message>(data, size)) {
    return Status::IOError("Invalid flatbuffers message.");
  }
  *out = flatbuf::GetMessage(data);
  return Status::OK();
}

// Serialize arrow::Schema as a Flatbuffer
Status WriteSchemaMessage(const Schema& schema, const DictionaryFieldMapper& mapper,
                          const IpcWriteOptions& options, std::shared_ptr<Buffer>* out);

// This function is used in a unit test
ARROW_EXPORT
Status WriteRecordBatchMessage(
    const int64_t length, const int64_t body_length,
    const std::shared_ptr<const KeyValueMetadata>& custom_metadata,
    const std::vector<FieldMetadata>& nodes, const std::vector<BufferMetadata>& buffers,
    const IpcWriteOptions& options, std::shared_ptr<Buffer>* out);

Result<std::shared_ptr<Buffer>> WriteTensorMessage(const Tensor& tensor,
                                                   const int64_t buffer_start_offset,
                                                   const IpcWriteOptions& options);

Result<std::shared_ptr<Buffer>> WriteSparseTensorMessage(
    const SparseTensor& sparse_tensor, int64_t body_length,
    const std::vector<BufferMetadata>& buffers, const IpcWriteOptions& options);

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
                       const std::vector<FileBlock>& record_batches,
                       const std::shared_ptr<const KeyValueMetadata>& metadata,
                       io::OutputStream* out);

Status WriteDictionaryMessage(
    const int64_t id, const bool is_delta, const int64_t length,
    const int64_t body_length,
    const std::shared_ptr<const KeyValueMetadata>& custom_metadata,
    const std::vector<FieldMetadata>& nodes, const std::vector<BufferMetadata>& buffers,
    const IpcWriteOptions& options, std::shared_ptr<Buffer>* out);

static inline Result<std::shared_ptr<Buffer>> WriteFlatbufferBuilder(
    flatbuffers::FlatBufferBuilder& fbb,  // NOLINT non-const reference
    MemoryPool* pool = default_memory_pool()) {
  int32_t size = fbb.GetSize();

  ARROW_ASSIGN_OR_RAISE(auto result, AllocateBuffer(size, pool));

  uint8_t* dst = result->mutable_data();
  memcpy(dst, fbb.GetBufferPointer(), size);
  return std::move(result);
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
