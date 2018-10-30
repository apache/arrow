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

#ifndef GANDIVA_SIMPLE_ARENA_H
#define GANDIVA_SIMPLE_ARENA_H

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "gandiva/arrow.h"

namespace gandiva {

/// \brief Simple arena allocator.
///
/// Memory is allocated from system in units of chunk-size, and dished out in the
/// requested sizes. If the requested size > chunk-size, allocate directly from the
/// system.
///
/// The allocated memory gets released only when the arena is destroyed, or on
/// Reset.
///
/// This code is not multi-thread safe, and avoids all locking for efficiency.
///
class SimpleArena {
 public:
  explicit SimpleArena(arrow::MemoryPool* pool, int64_t min_chunk_size = 4096);

  ~SimpleArena();

  // Allocate buffer of requested size.
  uint8_t* Allocate(int64_t size);

  // Reset arena state.
  void Reset();

  // total bytes allocated from system.
  int64_t total_bytes() { return total_bytes_; }

  // total bytes available for allocations.
  int64_t avail_bytes() { return avail_bytes_; }

 private:
  struct Chunk {
    Chunk(uint8_t* buf, int64_t size) : buf_(buf), size_(size) {}

    uint8_t* buf_;
    int64_t size_;
  };

  // Allocate new chunk.
  arrow::Status AllocateChunk(int64_t size);

  // release memory from buffers.
  void ReleaseChunks(bool retain_first);

  // Memory pool used for allocs.
  arrow::MemoryPool* pool_;

  // The chunk-size used for allocations from system.
  int64_t min_chunk_size_;

  // Total bytes allocated from system.
  int64_t total_bytes_;

  // Bytes available from allocated chunk.
  int64_t avail_bytes_;

  // buffer from current chunk.
  uint8_t* avail_buf_;

  // List of alloced chunks.
  std::vector<Chunk> chunks_;
};

inline SimpleArena::SimpleArena(arrow::MemoryPool* pool, int64_t min_chunk_size)
    : pool_(pool),
      min_chunk_size_(min_chunk_size),
      total_bytes_(0),
      avail_bytes_(0),
      avail_buf_(NULL) {}

inline SimpleArena::~SimpleArena() { ReleaseChunks(false /*retain_first*/); }

inline uint8_t* SimpleArena::Allocate(int64_t size) {
  if (avail_bytes_ < size) {
    auto status = AllocateChunk(std::max(size, min_chunk_size_));
    if (!status.ok()) {
      return NULL;
    }
  }

  uint8_t* ret = avail_buf_;
  avail_buf_ += size;
  avail_bytes_ -= size;
  return ret;
}

inline arrow::Status SimpleArena::AllocateChunk(int64_t size) {
  uint8_t* out;

  auto status = pool_->Allocate(size, &out);
  ARROW_RETURN_NOT_OK(status);

  chunks_.emplace_back(out, size);
  avail_buf_ = out;
  avail_bytes_ = size;  // left-over bytes in the previous chunk cannot be used anymore.
  total_bytes_ += size;
  return arrow::Status::OK();
}

// In the most common case, a chunk will be allocated when processing the first record.
// And, the same chunk can be used for processing the remaining records in the batch.
// By retaining the first chunk, the number of malloc calls are reduced to one per batch,
// instead of one per record.
inline void SimpleArena::Reset() {
  if (chunks_.size() == 0) {
    // if there are no chunks, nothing to do.
    return;
  }

  // Release all but the first chunk.
  if (chunks_.size() > 1) {
    ReleaseChunks(true);
    chunks_.erase(chunks_.begin() + 1, chunks_.end());
  }

  avail_buf_ = chunks_.at(0).buf_;
  avail_bytes_ = total_bytes_ = chunks_.at(0).size_;
}

inline void SimpleArena::ReleaseChunks(bool retain_first) {
  for (auto& chunk : chunks_) {
    if (retain_first) {
      // skip freeing first chunk.
      retain_first = false;
      continue;
    }
    pool_->Free(chunk.buf_, chunk.size_);
  }
}

}  // namespace gandiva

#endif  // GANDIVA_SIMPLE_ARENA_H
