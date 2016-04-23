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

namespace arrow {

class Array;
class RowBatch;
class Schema;
class Status;

namespace ipc {

class MemorySource;
class RecordBatchMessage;

// ----------------------------------------------------------------------
// Write path
// We have trouble decoding flatbuffers if the size i > 70, so 64 is a nice round number
// TODO(emkornfield) investigate this more
constexpr int kMaxIpcRecursionDepth = 64;
// Write the RowBatch (collection of equal-length Arrow arrays) to the memory
// source at the indicated position
//
// First, each of the memory buffers are written out end-to-end in starting at
// the indicated position.
//
// Then, this function writes the batch metadata as a flatbuffer (see
// format/Message.fbs -- the RecordBatch message type) like so:
//
// <int32: metadata size> <uint8*: metadata>
//
// Finally, the memory offset to the start of the metadata / data header is
// returned in an out-variable
Status WriteRowBatch(MemorySource* dst, const RowBatch* batch, int64_t position,
    int64_t* header_offset, int max_recursion_depth = kMaxIpcRecursionDepth);

// int64_t GetRowBatchMetadata(const RowBatch* batch);

// Compute the precise number of bytes needed in a contiguous memory segment to
// write the row batch. This involves generating the complete serialized
// Flatbuffers metadata.
Status GetRowBatchSize(const RowBatch* batch, int64_t* size);

// ----------------------------------------------------------------------
// "Read" path; does not copy data if the MemorySource does not

class RowBatchReader {
 public:
  static Status Open(
      MemorySource* source, int64_t position, std::shared_ptr<RowBatchReader>* out);

  static Status Open(MemorySource* source, int64_t position, int max_recursion_depth,
      std::shared_ptr<RowBatchReader>* out);

  // Reassemble the row batch. A Schema is required to be able to construct the
  // right array containers
  Status GetRowBatch(
      const std::shared_ptr<Schema>& schema, std::shared_ptr<RowBatch>* out);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_MEMORY_H
