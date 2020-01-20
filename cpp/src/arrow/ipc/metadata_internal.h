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
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <flatbuffers/flatbuffers.h>

#include "arrow/buffer.h"
#include "arrow/ipc/dictionary.h"  // IYWU pragma: keep
#include "arrow/ipc/message.h"
#include "arrow/memory_pool.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"

#include "generated/Message_generated.h"
#include "generated/Schema_generated.h"

namespace arrow {

class DataType;
class Schema;
class Tensor;
class SparseTensor;

namespace flatbuf = org::apache::arrow::flatbuf;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {

class DictionaryMemo;

namespace internal {

// This 0xFFFFFFFF value is the first 4 bytes of a valid IPC message
constexpr int32_t kIpcContinuationToken = -1;

static constexpr flatbuf::MetadataVersion kCurrentMetadataVersion =
    flatbuf::MetadataVersion::V4;

static constexpr flatbuf::MetadataVersion kMinMetadataVersion =
    flatbuf::MetadataVersion::V4;

MetadataVersion GetMetadataVersion(flatbuf::MetadataVersion version);

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

// EXPERIMENTAL: Extracting metadata of a sparse tensor from the message
Status GetSparseTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                               std::vector<int64_t>* shape,
                               std::vector<std::string>* dim_names, int64_t* length,
                               SparseTensorFormat::type* sparse_tensor_format_id);

static inline Status VerifyMessage(const uint8_t* data, int64_t size,
                                   const flatbuf::Message** out) {
  flatbuffers::Verifier verifier(data, size, /*max_depth=*/128);
  if (!flatbuf::VerifyMessageBuffer(verifier)) {
    return Status::IOError("Invalid flatbuffers message.");
  }
  *out = flatbuf::GetMessage(data);
  return Status::OK();
}

// Serialize arrow::Schema as a Flatbuffer
//
// \param[in] schema a Schema instance
// \param[in,out] dictionary_memo class for tracking dictionaries and assigning
// dictionary ids
// \param[out] out the serialized arrow::Buffer
// \return Status outcome
Status WriteSchemaMessage(const Schema& schema, DictionaryMemo* dictionary_memo,
                          std::shared_ptr<Buffer>* out);

Status WriteRecordBatchMessage(const int64_t length, const int64_t body_length,
                               const std::vector<FieldMetadata>& nodes,
                               const std::vector<BufferMetadata>& buffers,
                               std::shared_ptr<Buffer>* out);

Result<std::shared_ptr<Buffer>> WriteTensorMessage(const Tensor& tensor,
                                                   const int64_t buffer_start_offset);

Result<std::shared_ptr<Buffer>> WriteSparseTensorMessage(
    const SparseTensor& sparse_tensor, int64_t body_length,
    const std::vector<BufferMetadata>& buffers);

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
                       const std::vector<FileBlock>& record_batches,
                       io::OutputStream* out);

Status WriteDictionaryMessage(const int64_t id, const int64_t length,
                              const int64_t body_length,
                              const std::vector<FieldMetadata>& nodes,
                              const std::vector<BufferMetadata>& buffers,
                              std::shared_ptr<Buffer>* out);

static inline Result<std::shared_ptr<Buffer>> WriteFlatbufferBuilder(
    flatbuffers::FlatBufferBuilder& fbb) {
  int32_t size = fbb.GetSize();

  std::shared_ptr<Buffer> result;
  RETURN_NOT_OK(AllocateBuffer(default_memory_pool(), size, &result));

  uint8_t* dst = result->mutable_data();
  memcpy(dst, fbb.GetBufferPointer(), size);
  return result;
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_METADATA_H
