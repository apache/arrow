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

// Implement Arrow streaming binary format

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/dictionary.h"  // IWYU pragma: export
#include "arrow/ipc/message.h"
#include "arrow/ipc/options.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
class MemoryManager;
class MemoryPool;
class RecordBatch;
class Schema;
class Status;
class Table;
class Tensor;
class SparseTensor;

namespace io {

class OutputStream;

}  // namespace io

namespace ipc {

/// \brief Intermediate data structure with metadata header, and zero
/// or more buffers for the message body.
struct IpcPayload {
  MessageType type = MessageType::NONE;
  std::shared_ptr<Buffer> metadata;
  std::vector<std::shared_ptr<Buffer>> body_buffers;
  int64_t body_length = 0;
};

/// \class RecordBatchWriter
/// \brief Abstract interface for writing a stream of record batches
class ARROW_EXPORT RecordBatchWriter {
 public:
  virtual ~RecordBatchWriter();

  /// \brief Write a record batch to the stream
  ///
  /// \param[in] batch the record batch to write to the stream
  /// \return Status
  virtual Status WriteRecordBatch(const RecordBatch& batch) = 0;

  /// \brief Write possibly-chunked table by creating sequence of record batches
  /// \param[in] table table to write
  /// \return Status
  Status WriteTable(const Table& table);

  /// \brief Write Table with a particular chunksize
  /// \param[in] table table to write
  /// \param[in] max_chunksize maximum chunk size for table chunks
  /// \return Status
  Status WriteTable(const Table& table, int64_t max_chunksize);

  /// \brief Perform any logic necessary to finish the stream
  ///
  /// \return Status
  virtual Status Close() = 0;
};

/// Create a new IPC stream writer from stream sink and schema. User is
/// responsible for closing the actual OutputStream.
///
/// \param[in] sink output stream to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization
/// \return Result<std::shared_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::shared_ptr<RecordBatchWriter>> NewStreamWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options = IpcWriteOptions::Defaults());

/// Create a new IPC file writer from stream sink and schema
///
/// \param[in] sink output stream to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization, optional
/// \param[in] metadata custom metadata for File Footer, optional
/// \return Result<std::shared_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::shared_ptr<RecordBatchWriter>> NewFileWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options = IpcWriteOptions::Defaults(),
    const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

/// \brief Low-level API for writing a record batch (without schema)
/// to an OutputStream as encapsulated IPC message. See Arrow format
/// documentation for more detail.
///
/// \param[in] batch the record batch to write
/// \param[in] buffer_start_offset the start offset to use in the buffer metadata,
/// generally should be 0
/// \param[in] dst an OutputStream
/// \param[out] metadata_length the size of the length-prefixed flatbuffer
/// including padding to a 64-byte boundary
/// \param[out] body_length the size of the contiguous buffer block plus
/// \param[in] options options for serialization
/// \return Status
ARROW_EXPORT
Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
                        io::OutputStream* dst, int32_t* metadata_length,
                        int64_t* body_length, const IpcWriteOptions& options);

/// \brief Serialize record batch as encapsulated IPC message in a new buffer
///
/// \param[in] batch the record batch
/// \param[in] options the IpcWriteOptions to use for serialization
/// \return the serialized message
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                     const IpcWriteOptions& options);

/// \brief Serialize record batch as encapsulated IPC message in a new buffer
///
/// \param[in] batch the record batch
/// \param[in] mm a MemoryManager to allocate memory from
/// \return the serialized message
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> SerializeRecordBatch(const RecordBatch& batch,
                                                     std::shared_ptr<MemoryManager> mm);

/// \brief Write record batch to OutputStream
///
/// \param[in] batch the record batch to write
/// \param[in] options the IpcWriteOptions to use for serialization
/// \param[in] out the OutputStream to write the output to
/// \return Status
///
/// If writing to pre-allocated memory, you can use
/// arrow::ipc::GetRecordBatchSize to compute how much space is required
ARROW_EXPORT
Status SerializeRecordBatch(const RecordBatch& batch, const IpcWriteOptions& options,
                            io::OutputStream* out);

/// \brief Serialize schema as encapsulated IPC message
///
/// \param[in] schema the schema to write
/// \param[in] dictionary_memo a DictionaryMemo for recording dictionary ids
/// \param[in] pool a MemoryPool to allocate memory from
/// \return the serialized schema
ARROW_EXPORT
Result<std::shared_ptr<Buffer>> SerializeSchema(const Schema& schema,
                                                DictionaryMemo* dictionary_memo,
                                                MemoryPool* pool = default_memory_pool());

/// \brief Write multiple record batches to OutputStream, including schema
/// \param[in] batches a vector of batches. Must all have same schema
/// \param[in] options options for serialization
/// \param[out] dst an OutputStream
/// \return Status
ARROW_EXPORT
Status WriteRecordBatchStream(const std::vector<std::shared_ptr<RecordBatch>>& batches,
                              const IpcWriteOptions& options, io::OutputStream* dst);

/// \brief Compute the number of bytes needed to write an IPC payload
///     including metadata
///
/// \param[in] payload the IPC payload to write
/// \param[in] options write options
/// \return the size of the complete encapsulated message
ARROW_EXPORT
int64_t GetPayloadSize(const IpcPayload& payload,
                       const IpcWriteOptions& options = IpcWriteOptions::Defaults());

/// \brief Compute the number of bytes needed to write a record batch including metadata
///
/// \param[in] batch the record batch to write
/// \param[out] size the size of the complete encapsulated message
/// \return Status
ARROW_EXPORT
Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size);

/// \brief Compute the number of bytes needed to write a record batch including metadata
///
/// \param[in] batch the record batch to write
/// \param[in] options options for serialization
/// \param[out] size the size of the complete encapsulated message
/// \return Status
ARROW_EXPORT
Status GetRecordBatchSize(const RecordBatch& batch, const IpcWriteOptions& options,
                          int64_t* size);

/// \brief Compute the number of bytes needed to write a tensor including metadata
///
/// \param[in] tensor the tensor to write
/// \param[out] size the size of the complete encapsulated message
/// \return Status
ARROW_EXPORT
Status GetTensorSize(const Tensor& tensor, int64_t* size);

/// \brief EXPERIMENTAL: Convert arrow::Tensor to a Message with minimal memory
/// allocation
///
/// \param[in] tensor the Tensor to write
/// \param[in] pool MemoryPool to allocate space for metadata
/// \return the resulting Message
ARROW_EXPORT
Result<std::unique_ptr<Message>> GetTensorMessage(const Tensor& tensor, MemoryPool* pool);

/// \brief Write arrow::Tensor as a contiguous message.
///
/// The metadata and body are written assuming 64-byte alignment. It is the
/// user's responsibility to ensure that the OutputStream has been aligned
/// to a 64-byte multiple before writing the message.
///
/// The message is written out as followed:
/// \code
/// <metadata size> <metadata> <tensor data>
/// \endcode
///
/// \param[in] tensor the Tensor to write
/// \param[in] dst the OutputStream to write to
/// \param[out] metadata_length the actual metadata length, including padding
/// \param[out] body_length the actual message body length
/// \return Status
ARROW_EXPORT
Status WriteTensor(const Tensor& tensor, io::OutputStream* dst, int32_t* metadata_length,
                   int64_t* body_length);

/// \brief EXPERIMENTAL: Convert arrow::SparseTensor to a Message with minimal memory
/// allocation
///
/// The message is written out as followed:
/// \code
/// <metadata size> <metadata> <sparse index> <sparse tensor body>
/// \endcode
///
/// \param[in] sparse_tensor the SparseTensor to write
/// \param[in] pool MemoryPool to allocate space for metadata
/// \return the resulting Message
ARROW_EXPORT
Result<std::unique_ptr<Message>> GetSparseTensorMessage(const SparseTensor& sparse_tensor,
                                                        MemoryPool* pool);

/// \brief EXPERIMENTAL: Write arrow::SparseTensor as a contiguous message. The metadata,
/// sparse index, and body are written assuming 64-byte alignment. It is the
/// user's responsibility to ensure that the OutputStream has been aligned
/// to a 64-byte multiple before writing the message.
///
/// \param[in] sparse_tensor the SparseTensor to write
/// \param[in] dst the OutputStream to write to
/// \param[out] metadata_length the actual metadata length, including padding
/// \param[out] body_length the actual message body length
/// \return Status
ARROW_EXPORT
Status WriteSparseTensor(const SparseTensor& sparse_tensor, io::OutputStream* dst,
                         int32_t* metadata_length, int64_t* body_length);

/// \brief Compute IpcPayload for the given schema
/// \param[in] schema the Schema that is being serialized
/// \param[in] options options for serialization
/// \param[in,out] dictionary_memo class to populate with assigned dictionary ids
/// \param[out] out the returned vector of IpcPayloads
/// \return Status
ARROW_EXPORT
Status GetSchemaPayload(const Schema& schema, const IpcWriteOptions& options,
                        DictionaryMemo* dictionary_memo, IpcPayload* out);

/// \brief Compute IpcPayload for a dictionary
/// \param[in] id the dictionary id
/// \param[in] dictionary the dictionary values
/// \param[in] options options for serialization
/// \param[out] payload the output IpcPayload
/// \return Status
ARROW_EXPORT
Status GetDictionaryPayload(int64_t id, const std::shared_ptr<Array>& dictionary,
                            const IpcWriteOptions& options, IpcPayload* payload);

/// \brief Compute IpcPayload for a dictionary
/// \param[in] id the dictionary id
/// \param[in] is_delta whether the dictionary is a delta dictionary
/// \param[in] dictionary the dictionary values
/// \param[in] options options for serialization
/// \param[out] payload the output IpcPayload
/// \return Status
ARROW_EXPORT
Status GetDictionaryPayload(int64_t id, bool is_delta,
                            const std::shared_ptr<Array>& dictionary,
                            const IpcWriteOptions& options, IpcPayload* payload);

/// \brief Compute IpcPayload for the given record batch
/// \param[in] batch the RecordBatch that is being serialized
/// \param[in] options options for serialization
/// \param[out] out the returned IpcPayload
/// \return Status
ARROW_EXPORT
Status GetRecordBatchPayload(const RecordBatch& batch, const IpcWriteOptions& options,
                             IpcPayload* out);

/// \brief Write an IPC payload to the given stream.
/// \param[in] payload the payload to write
/// \param[in] options options for serialization
/// \param[in] dst The stream to write the payload to.
/// \param[out] metadata_length the length of the serialized metadata
/// \return Status
ARROW_EXPORT
Status WriteIpcPayload(const IpcPayload& payload, const IpcWriteOptions& options,
                       io::OutputStream* dst, int32_t* metadata_length);

/// \brief Compute IpcPayload for the given sparse tensor
/// \param[in] sparse_tensor the SparseTensor that is being serialized
/// \param[in,out] pool for any required temporary memory allocations
/// \param[out] out the returned IpcPayload
/// \return Status
ARROW_EXPORT
Status GetSparseTensorPayload(const SparseTensor& sparse_tensor, MemoryPool* pool,
                              IpcPayload* out);

namespace internal {

// These internal APIs may change without warning or deprecation

class ARROW_EXPORT IpcPayloadWriter {
 public:
  virtual ~IpcPayloadWriter();

  // Default implementation is a no-op
  virtual Status Start();

  virtual Status WritePayload(const IpcPayload& payload) = 0;

  virtual Status Close() = 0;
};

/// Create a new IPC payload stream writer from stream sink. User is
/// responsible for closing the actual OutputStream.
///
/// \param[in] sink output stream to write to
/// \param[in] options options for serialization
/// \return Result<std::shared_ptr<IpcPayloadWriter>>
ARROW_EXPORT
Result<std::unique_ptr<IpcPayloadWriter>> MakePayloadStreamWriter(
    io::OutputStream* sink, const IpcWriteOptions& options = IpcWriteOptions::Defaults());

/// Create a new IPC payload file writer from stream sink.
///
/// \param[in] sink output stream to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization, optional
/// \param[in] metadata custom metadata for File Footer, optional
/// \return Status
ARROW_EXPORT
Result<std::unique_ptr<IpcPayloadWriter>> MakePayloadFileWriter(
    io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options = IpcWriteOptions::Defaults(),
    const std::shared_ptr<const KeyValueMetadata>& metadata = NULLPTR);

/// Create a new RecordBatchWriter from IpcPayloadWriter and schema.
///
/// \param[in] sink the IpcPayloadWriter to write to
/// \param[in] schema the schema of the record batches to be written
/// \param[in] options options for serialization
/// \return Result<std::unique_ptr<RecordBatchWriter>>
ARROW_EXPORT
Result<std::unique_ptr<RecordBatchWriter>> OpenRecordBatchWriter(
    std::unique_ptr<IpcPayloadWriter> sink, const std::shared_ptr<Schema>& schema,
    const IpcWriteOptions& options = IpcWriteOptions::Defaults());

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
