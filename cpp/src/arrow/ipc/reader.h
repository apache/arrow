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

// Read Arrow files and streams

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "arrow/ipc/message.h"
#include "arrow/ipc/options.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class Schema;
class Status;
class Tensor;
class SparseTensor;

namespace io {

class InputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

class DictionaryMemo;

namespace internal {

struct IpcPayload;

}  // namespace internal

using RecordBatchReader = ::arrow::RecordBatchReader;

/// \class RecordBatchStreamReader
/// \brief Synchronous batch stream reader that reads from io::InputStream
///
/// This class reads the schema (plus any dictionaries) as the first messages
/// in the stream, followed by record batches. For more granular zero-copy
/// reads see the ReadRecordBatch functions
class ARROW_EXPORT RecordBatchStreamReader : public RecordBatchReader {
 public:
  /// Create batch reader from generic MessageReader.
  /// This will take ownership of the given MessageReader.
  ///
  /// \param[in] message_reader a MessageReader implementation
  /// \param[in] options any IPC reading options (optional)
  /// \return the created batch reader
  static Result<std::shared_ptr<RecordBatchReader>> Open(
      std::unique_ptr<MessageReader> message_reader,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  /// \brief Record batch stream reader from InputStream
  ///
  /// \param[in] stream an input stream instance. Must stay alive throughout
  /// lifetime of stream reader
  /// \param[in] options any IPC reading options (optional)
  /// \return the created batch reader
  static Result<std::shared_ptr<RecordBatchReader>> Open(
      io::InputStream* stream,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  /// \brief Open stream and retain ownership of stream object
  /// \param[in] stream the input stream
  /// \param[in] options any IPC reading options (optional)
  /// \return the created batch reader
  static Result<std::shared_ptr<RecordBatchReader>> Open(
      const std::shared_ptr<io::InputStream>& stream,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(std::unique_ptr<MessageReader> message_reader,
                     std::shared_ptr<RecordBatchReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(std::unique_ptr<MessageReader> message_reader,
                     std::unique_ptr<RecordBatchReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(io::InputStream* stream, std::shared_ptr<RecordBatchReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(const std::shared_ptr<io::InputStream>& stream,
                     std::shared_ptr<RecordBatchReader>* out);
};

/// \brief Reads the record batch file format
class ARROW_EXPORT RecordBatchFileReader {
 public:
  virtual ~RecordBatchFileReader() = default;

  /// \brief Open a RecordBatchFileReader
  ///
  /// Open a file-like object that is assumed to be self-contained; i.e., the
  /// end of the file interface is the end of the Arrow file. Note that there
  /// can be any amount of data preceding the Arrow-formatted data, because we
  /// need only locate the end of the Arrow file stream to discover the metadata
  /// and then proceed to read the data into memory.
  static Result<std::shared_ptr<RecordBatchFileReader>> Open(
      io::RandomAccessFile* file,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  /// \brief Open a RecordBatchFileReader
  /// If the file is embedded within some larger file or memory region, you can
  /// pass the absolute memory offset to the end of the file (which contains the
  /// metadata footer). The metadata must have been written with memory offsets
  /// relative to the start of the containing file
  ///
  /// \param[in] file the data source
  /// \param[in] footer_offset the position of the end of the Arrow file
  /// \param[in] options options for IPC reading
  /// \return the returned reader
  static Result<std::shared_ptr<RecordBatchFileReader>> Open(
      io::RandomAccessFile* file, int64_t footer_offset,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  /// \brief Version of Open that retains ownership of file
  ///
  /// \param[in] file the data source
  /// \param[in] options options for IPC reading
  /// \return the returned reader
  static Result<std::shared_ptr<RecordBatchFileReader>> Open(
      const std::shared_ptr<io::RandomAccessFile>& file,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  /// \brief Version of Open that retains ownership of file
  ///
  /// \param[in] file the data source
  /// \param[in] footer_offset the position of the end of the Arrow file
  /// \param[in] options options for IPC reading
  /// \return the returned reader
  static Result<std::shared_ptr<RecordBatchFileReader>> Open(
      const std::shared_ptr<io::RandomAccessFile>& file, int64_t footer_offset,
      const IpcReadOptions& options = IpcReadOptions::Defaults());

  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
                     int64_t footer_offset, std::shared_ptr<RecordBatchFileReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
                     std::shared_ptr<RecordBatchFileReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(io::RandomAccessFile* file, int64_t footer_offset,
                     std::shared_ptr<RecordBatchFileReader>* out);
  ARROW_DEPRECATED("Deprecated in 0.17.0. Use Result-returning version")
  static Status Open(io::RandomAccessFile* file,
                     std::shared_ptr<RecordBatchFileReader>* out);

  /// \brief The schema read from the file
  virtual std::shared_ptr<Schema> schema() const = 0;

  /// \brief Returns the number of record batches in the file
  virtual int num_record_batches() const = 0;

  /// \brief Return the metadata version from the file metadata
  virtual MetadataVersion version() const = 0;

  /// \brief Return the contents of the custom_metadata field from the file's
  /// Footer
  virtual std::shared_ptr<const KeyValueMetadata> metadata() const = 0;

  /// \brief Read a particular record batch from the file. Does not copy memory
  /// if the input source supports zero-copy.
  ///
  /// \param[in] i the index of the record batch to return
  /// \return the read batch
  virtual Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(int i) = 0;

  ARROW_DEPRECATED("Use version with Result return value")
  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch) {
    return ReadRecordBatch(i).Value(batch);
  }
};

// Generic read functions; does not copy data if the input supports zero copy reads

/// \brief Read Schema from stream serialized as a single IPC message
/// and populate any dictionary-encoded fields into a DictionaryMemo
///
/// \param[in] stream an InputStream
/// \param[in] dictionary_memo for recording dictionary-encoded fields
/// \return the output Schema
///
/// If record batches follow the schema, it is better to use
/// RecordBatchStreamReader
ARROW_EXPORT
Result<std::shared_ptr<Schema>> ReadSchema(io::InputStream* stream,
                                           DictionaryMemo* dictionary_memo);

/// \brief Read Schema from encapsulated Message
///
/// \param[in] message the message containing the Schema IPC metadata
/// \param[in] dictionary_memo DictionaryMemo for recording dictionary-encoded
/// fields. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \return the resulting Schema
ARROW_EXPORT
Result<std::shared_ptr<Schema>> ReadSchema(const Message& message,
                                           DictionaryMemo* dictionary_memo);

/// Read record batch as encapsulated IPC message with metadata size prefix and
/// header
///
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] options IPC options for reading
/// \param[in] stream the file where the batch is located
/// \return the read record batch
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const std::shared_ptr<Schema>& schema, const DictionaryMemo* dictionary_memo,
    const IpcReadOptions& options, io::InputStream* stream);

/// \brief Read record batch from message
///
/// \param[in] message a Message containing the record batch metadata
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] options IPC options for reading
/// \return the read record batch
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Message& message, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options);

/// Read record batch from file given metadata and schema
///
/// \param[in] metadata a Message containing the record batch metadata
/// \param[in] schema the record batch schema
/// \param[in] dictionary_memo DictionaryMemo which has any
/// dictionaries. Can be nullptr if you are sure there are no
/// dictionary-encoded fields
/// \param[in] file a random access file
/// \param[in] options options for deserialization
/// \return the read record batch
ARROW_EXPORT
Result<std::shared_ptr<RecordBatch>> ReadRecordBatch(
    const Buffer& metadata, const std::shared_ptr<Schema>& schema,
    const DictionaryMemo* dictionary_memo, const IpcReadOptions& options,
    io::RandomAccessFile* file);

/// \brief Read arrow::Tensor as encapsulated IPC message in file
///
/// \param[in] file an InputStream pointed at the start of the message
/// \return the read tensor
ARROW_EXPORT
Result<std::shared_ptr<Tensor>> ReadTensor(io::InputStream* file);

/// \brief EXPERIMENTAL: Read arrow::Tensor from IPC message
///
/// \param[in] message a Message containing the tensor metadata and body
/// \return the read tensor
ARROW_EXPORT
Result<std::shared_ptr<Tensor>> ReadTensor(const Message& message);

/// \brief EXPERIMENTAL: Read arrow::SparseTensor as encapsulated IPC message in file
///
/// \param[in] file an InputStream pointed at the start of the message
/// \return the read sparse tensor
ARROW_EXPORT
Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(io::InputStream* file);

/// \brief EXPERIMENTAL: Read arrow::SparseTensor from IPC message
///
/// \param[in] message a Message containing the tensor metadata and body
/// \return the read sparse tensor
ARROW_EXPORT
Result<std::shared_ptr<SparseTensor>> ReadSparseTensor(const Message& message);

namespace internal {

// These internal APIs may change without warning or deprecation

/// \brief EXPERIMENTAL: Read arrow::SparseTensorFormat::type from a metadata
/// \param[in] metadata a Buffer containing the sparse tensor metadata
/// \return the count of the body buffers
ARROW_EXPORT
Result<size_t> ReadSparseTensorBodyBufferCount(const Buffer& metadata);

/// \brief EXPERIMENTAL: Read arrow::SparseTensor from an IpcPayload
/// \param[in] payload a IpcPayload contains a serialized SparseTensor
/// \return the read sparse tensor
ARROW_EXPORT
Result<std::shared_ptr<SparseTensor>> ReadSparseTensorPayload(const IpcPayload& payload);

// For fuzzing targets
ARROW_EXPORT
Status FuzzIpcStream(const uint8_t* data, int64_t size);
ARROW_EXPORT
Status FuzzIpcFile(const uint8_t* data, int64_t size);

}  // namespace internal

ARROW_DEPRECATED("Deprecated in 0.17.0. Use version with Result return value")
ARROW_EXPORT
Status ReadSchema(io::InputStream* stream, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out);

ARROW_DEPRECATED("Deprecated in 0.17.0. Use version with Result return value")
ARROW_EXPORT
Status ReadSchema(const Message& message, DictionaryMemo* dictionary_memo,
                  std::shared_ptr<Schema>* out);

ARROW_DEPRECATED("Deprecated in 0.17.0. Use version with Result return value")
ARROW_EXPORT
Status ReadRecordBatch(const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::InputStream* stream,
                       std::shared_ptr<RecordBatch>* out);

ARROW_DEPRECATED("Deprecated in 0.17.0. Use version with Result return value")
ARROW_EXPORT
Status ReadRecordBatch(const Buffer& metadata, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo, io::RandomAccessFile* file,
                       std::shared_ptr<RecordBatch>* out);

ARROW_DEPRECATED("Deprecated in 0.17.0. Use version with Result return value")
ARROW_EXPORT
Status ReadRecordBatch(const Message& message, const std::shared_ptr<Schema>& schema,
                       const DictionaryMemo* dictionary_memo,
                       std::shared_ptr<RecordBatch>* out);

}  // namespace ipc
}  // namespace arrow
