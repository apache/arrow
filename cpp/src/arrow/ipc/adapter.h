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

// Public API for writing and accessing (with zero copy, if possible) Arrow
// data in shared memory

#ifndef ARROW_IPC_ADAPTER_H
#define ARROW_IPC_ADAPTER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class RecordBatch;
class Schema;
class Status;

namespace io {

class ReadableFileInterface;
class OutputStream;

}  // namespace io

namespace ipc {

class RecordBatchMessage;

// ----------------------------------------------------------------------
// Write path
// We have trouble decoding flatbuffers if the size i > 70, so 64 is a nice round number
// TODO(emkornfield) investigate this more
constexpr int kMaxIpcRecursionDepth = 64;

// Write the RecordBatch (collection of equal-length Arrow arrays) to the output
// stream
//
// First, each of the memory buffers are written out end-to-end
//
// Then, this function writes the batch metadata as a flatbuffer (see
// format/Message.fbs -- the RecordBatch message type) like so:
//
// <int32: metadata size> <uint8*: metadata>
//
// Finally, the absolute offsets (relative to the start of the output stream)
// to the end of the body and end of the metadata / data header (suffixed by
// the header size) is returned in out-variables
ARROW_EXPORT Status WriteRecordBatch(const std::vector<std::shared_ptr<Array>>& columns,
    int32_t num_rows, io::OutputStream* dst, int64_t* body_end_offset,
    int64_t* header_end_offset, int max_recursion_depth = kMaxIpcRecursionDepth);

// int64_t GetRecordBatchMetadata(const RecordBatch* batch);

// Compute the precise number of bytes needed in a contiguous memory segment to
// write the record batch. This involves generating the complete serialized
// Flatbuffers metadata.
ARROW_EXPORT Status GetRecordBatchSize(const RecordBatch* batch, int64_t* size);

// ----------------------------------------------------------------------
// "Read" path; does not copy data if the input supports zero copy reads

class ARROW_EXPORT RecordBatchReader {
 public:
  // The offset is the absolute position to the *end* of the record batch data
  // header
  static Status Open(io::ReadableFileInterface* file, int64_t offset,
      std::shared_ptr<RecordBatchReader>* out);

  static Status Open(io::ReadableFileInterface* file, int64_t offset,
      int max_recursion_depth, std::shared_ptr<RecordBatchReader>* out);

  virtual ~RecordBatchReader();

  // Reassemble the record batch. A Schema is required to be able to construct
  // the right array containers
  Status GetRecordBatch(
      const std::shared_ptr<Schema>& schema, std::shared_ptr<RecordBatch>* out);

 private:
  class RecordBatchReaderImpl;
  std::unique_ptr<RecordBatchReaderImpl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_MEMORY_H
