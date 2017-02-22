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

#ifndef ARROW_IPC_STREAM_H
#define ARROW_IPC_STREAM_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/ipc/metadata.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class Buffer;
struct Field;
class MemoryPool;
class RecordBatch;
class Schema;
class Status;

namespace io {

class InputStream;
class OutputStream;

}  // namespace io

namespace ipc {

struct ARROW_EXPORT FileBlock {
  FileBlock() {}
  FileBlock(int64_t offset, int32_t metadata_length, int64_t body_length)
      : offset(offset), metadata_length(metadata_length), body_length(body_length) {}

  int64_t offset;
  int32_t metadata_length;
  int64_t body_length;
};

class ARROW_EXPORT StreamWriter {
 public:
  virtual ~StreamWriter() = default;

  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<StreamWriter>* out);

  virtual Status WriteRecordBatch(const RecordBatch& batch);

  /// Perform any logic necessary to finish the stream. User is responsible for
  /// closing the actual OutputStream
  virtual Status Close();

  // In some cases, writing may require memory allocation. We use the default
  // memory pool, but provide the option to override
  void set_memory_pool(MemoryPool* pool);

 protected:
  StreamWriter(io::OutputStream* sink, const std::shared_ptr<Schema>& schema);

  virtual Status Start();

  Status CheckStarted();
  Status UpdatePosition();

  Status WriteDictionaries();

  Status WriteRecordBatch(const RecordBatch& batch, FileBlock* block);

  // Adds padding bytes if necessary to ensure all memory blocks are written on
  // 8-byte boundaries.
  Status Align();

  // Write data and update position
  Status Write(const uint8_t* data, int64_t nbytes);

  // Write and align
  Status WriteAligned(const uint8_t* data, int64_t nbytes);

  io::OutputStream* sink_;
  std::shared_ptr<Schema> schema_;

  // When writing out the schema, we keep track of all the dictionaries we
  // encounter, as they must be written out first in the stream
  std::shared_ptr<DictionaryMemo> dictionary_memo_;

  MemoryPool* pool_;

  int64_t position_;
  bool started_;

  std::vector<FileBlock> dictionaries_;
  std::vector<FileBlock> record_batches_;
};

class ARROW_EXPORT StreamReader {
 public:
  ~StreamReader();

  // Open an stream.
  static Status Open(const std::shared_ptr<io::InputStream>& stream,
      std::shared_ptr<StreamReader>* reader);

  std::shared_ptr<Schema> schema() const;

  // Returned batch is nullptr when end of stream reached
  Status GetNextRecordBatch(std::shared_ptr<RecordBatch>* batch);

 private:
  StreamReader();

  class ARROW_NO_EXPORT StreamReaderImpl;
  std::unique_ptr<StreamReaderImpl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_STREAM_H
