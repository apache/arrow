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

// Implement Arrow file layout for IPC/RPC purposes and short-lived storage

#ifndef ARROW_IPC_READER_H
#define ARROW_IPC_READER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/metadata.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class RecordBatch;
class Schema;
class Status;
class Tensor;

namespace io {

class InputStream;
class RandomAccessFile;

}  // namespace io

namespace ipc {

/// \brief Abstract interface for reading stream of record batches
class ARROW_EXPORT RecordBatchReader {
 public:
  virtual ~RecordBatchReader();

  /// \return the shared schema of the record batches in the stream
  virtual std::shared_ptr<Schema> schema() const = 0;

  /// Read the next record batch in the stream. Return nullptr for batch when
  /// reaching end of stream
  ///
  /// \param(out) batch the next loaded batch, nullptr at end of stream
  /// \return Status
  virtual Status ReadNextRecordBatch(std::shared_ptr<RecordBatch>* batch) = 0;
};

/// \class RecordBatchStreamReader
/// \brief Synchronous batch stream reader that reads from io::InputStream
class ARROW_EXPORT RecordBatchStreamReader : public RecordBatchReader {
 public:
  virtual ~RecordBatchStreamReader();

  /// Create batch reader from generic MessageReader
  ///
  /// \param(in) message_reader a MessageReader implementation
  /// \param(out) out the created RecordBatchStreamReader object
  /// \return Status
  static Status Open(std::unique_ptr<MessageReader> message_reader,
      std::shared_ptr<RecordBatchStreamReader>* out);

  /// \Create Record batch stream reader from InputStream
  ///
  /// \param(in) stream an input stream instance
  /// \param(out) out the created RecordBatchStreamReader object
  /// \return Status
  static Status Open(const std::shared_ptr<io::InputStream>& stream,
      std::shared_ptr<RecordBatchStreamReader>* out);

  std::shared_ptr<Schema> schema() const override;
  Status ReadNextRecordBatch(std::shared_ptr<RecordBatch>* batch) override;

 private:
  RecordBatchStreamReader();

  class ARROW_NO_EXPORT RecordBatchStreamReaderImpl;
  std::unique_ptr<RecordBatchStreamReaderImpl> impl_;
};

/// \brief Reads the record batch file format
class ARROW_EXPORT RecordBatchFileReader {
 public:
  ~RecordBatchFileReader();

  // Open a file-like object that is assumed to be self-contained; i.e., the
  // end of the file interface is the end of the Arrow file. Note that there
  // can be any amount of data preceding the Arrow-formatted data, because we
  // need only locate the end of the Arrow file stream to discover the metadata
  // and then proceed to read the data into memory.
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
      std::shared_ptr<RecordBatchFileReader>* reader);

  // If the file is embedded within some larger file or memory region, you can
  // pass the absolute memory offset to the end of the file (which contains the
  // metadata footer). The metadata must have been written with memory offsets
  // relative to the start of the containing file
  //
  // @param file: the data source
  // @param footer_offset: the position of the end of the Arrow "file"
  static Status Open(const std::shared_ptr<io::RandomAccessFile>& file,
      int64_t footer_offset, std::shared_ptr<RecordBatchFileReader>* reader);

  /// The schema includes any dictionaries
  std::shared_ptr<Schema> schema() const;

  /// Returns number of record batches in the file
  int num_record_batches() const;

  /// Returns MetadataVersion in the file metadata
  MetadataVersion version() const;

  /// Read a record batch from the file. Does not copy memory if the input
  /// source supports zero-copy.
  ///
  /// \param(in) i the index of the record batch to return
  /// \param(out) batch the read batch
  /// \return Status
  Status ReadRecordBatch(int i, std::shared_ptr<RecordBatch>* batch);

 private:
  RecordBatchFileReader();

  class ARROW_NO_EXPORT RecordBatchFileReaderImpl;
  std::unique_ptr<RecordBatchFileReaderImpl> impl_;
};

// Generic read functions; does not copy data if the input supports zero copy reads

/// \brief Read record batch from file given metadata and schema
///
/// \param(in) metadata a Message containing the record batch metadata
/// \param(in) schema the record batch schema
/// \param(in) file a random access file
/// \param(out) out the read record batch
Status ARROW_EXPORT ReadRecordBatch(const Buffer& metadata,
    const std::shared_ptr<Schema>& schema, io::RandomAccessFile* file,
    std::shared_ptr<RecordBatch>* out);

/// \brief Read record batch from fully encapulated Message
///
/// \param[in] message a message instance containing metadata and body
/// \param[in] schema
/// \param[out] out the resulting RecordBatch
/// \return Status
Status ARROW_EXPORT ReadRecordBatch(const Message& message,
    const std::shared_ptr<Schema>& schema, std::shared_ptr<RecordBatch>* out);

/// Read record batch from file given metadata and schema
///
/// \param(in) metadata a Message containing the record batch metadata
/// \param(in) schema the record batch schema
/// \param(in) file a random access file
/// \param(in) max_recursion_depth the maximum permitted nesting depth
/// \param(out) out the read record batch
Status ARROW_EXPORT ReadRecordBatch(const Buffer& metadata,
    const std::shared_ptr<Schema>& schema, int max_recursion_depth,
    io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out);

/// Read record batch as encapsulated IPC message with metadata size prefix and
/// header
///
/// \param(in) schema the record batch schema
/// \param(in) offset the file location of the start of the message
/// \param(in) file the file where the batch is located
/// \param(out) out the read record batch
Status ARROW_EXPORT ReadRecordBatch(const std::shared_ptr<Schema>& schema, int64_t offset,
    io::RandomAccessFile* file, std::shared_ptr<RecordBatch>* out);

/// EXPERIMENTAL: Read arrow::Tensor as encapsulated IPC message in file
///
/// \param(in) offset the file location of the start of the message
/// \param(in) file the file where the batch is located
/// \param(out) out the read tensor
Status ARROW_EXPORT ReadTensor(
    int64_t offset, io::RandomAccessFile* file, std::shared_ptr<Tensor>* out);

/// Backwards-compatibility for Arrow < 0.4.0
///
#ifndef ARROW_NO_DEPRECATED_API
using StreamReader = RecordBatchReader;
using FileReader = RecordBatchFileReader;
#endif

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_READER_H
