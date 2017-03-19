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

class OutputStream;

}  // namespace io

namespace ipc {

// Write the RecordBatch (collection of equal-length Arrow arrays) to the
// output stream in a contiguous block. The record batch metadata is written as
// a flatbuffer (see format/Message.fbs -- the RecordBatch message type)
// prefixed by its size, followed by each of the memory buffers in the batch
// written end to end (with appropriate alignment and padding):
//
// <int32: metadata size> <uint8*: metadata> <buffers>
//
// Finally, the absolute offsets (relative to the start of the output stream)
// to the end of the body and end of the metadata / data header (suffixed by
// the header size) is returned in out-variables
//
// @param(in) buffer_start_offset: the start offset to use in the buffer metadata,
// default should be 0
//
// @param(out) metadata_length: the size of the length-prefixed flatbuffer
// including padding to a 64-byte boundary
//
// @param(out) body_length: the size of the contiguous buffer block plus
// padding bytes
Status WriteRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
    io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length,
    MemoryPool* pool, int max_recursion_depth = kMaxNestingDepth);

// Write Array as a DictionaryBatch message
Status WriteDictionary(int64_t dictionary_id, const std::shared_ptr<Array>& dictionary,
    int64_t buffer_start_offset, io::OutputStream* dst, int32_t* metadata_length,
    int64_t* body_length, MemoryPool* pool);

// Compute the precise number of bytes needed in a contiguous memory segment to
// write the record batch. This involves generating the complete serialized
// Flatbuffers metadata.
Status GetRecordBatchSize(const RecordBatch& batch, int64_t* size);

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
  StreamWriter();
  class ARROW_NO_EXPORT StreamWriterImpl;
  std::unique_ptr<StreamWriterImpl> impl_;
};

class ARROW_EXPORT FileWriter : public StreamWriter {
 public:
  static Status Open(io::OutputStream* sink, const std::shared_ptr<Schema>& schema,
      std::shared_ptr<FileWriter>* out);

  Status WriteRecordBatch(const RecordBatch& batch) override;
  Status Close() override;

 private:
  FileWriter();
  class ARROW_NO_EXPORT FileWriterImpl;
  std::unique_ptr<FileWriterImpl> impl_;
};

// ----------------------------------------------------------------------

/// EXPERIMENTAL: Write record batch using LargeRecordBatch IPC metadata. This
/// data may not be readable by all Arrow implementations
Status WriteLargeRecordBatch(const RecordBatch& batch, int64_t buffer_start_offset,
    io::OutputStream* dst, int32_t* metadata_length, int64_t* body_length,
    MemoryPool* pool, int max_recursion_depth = kMaxNestingDepth);

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_STREAM_H
