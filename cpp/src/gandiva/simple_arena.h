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

#include <memory>
#include <utility>
#include <vector>

namespace gandiva {

#ifdef GDV_HELPERS
namespace helpers {
#endif

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
  explicit SimpleArena(int32_t chunk_size = 4096);

  // Allocate buffer of requested size.
  uint8_t* Allocate(int32_t size);

  // Reset arena state.
  void Reset();

  // total bytes allocated from system.
  int32_t total_bytes() { return total_bytes_; }

  // total bytes available for allocations.
  int32_t avail_bytes() { return avail_bytes_; }

 private:
  // Allocate new chunk.
  void AllocateChunk(int size);

  // The chunk-size used for allocations from system.
  int32_t chunk_size_;

  // The size of the first chunk.
  int32_t first_chunk_size_;

  // Total bytes allocated from system.
  int32_t total_bytes_;

  // Bytes available from allocated chunk.
  int32_t avail_bytes_;

  // buffer from current chunk.
  uint8_t* avail_buf_;

  // List of alloced chunks.
  std::vector<std::unique_ptr<uint8_t>> chunks_;
};

inline SimpleArena::SimpleArena(int32_t chunk_size)
    : chunk_size_(chunk_size),
      first_chunk_size_(0),
      total_bytes_(0),
      avail_bytes_(0),
      avail_buf_(nullptr) {}

inline uint8_t* SimpleArena::Allocate(int32_t size) {
  if (avail_bytes_ < size) {
    AllocateChunk(size <= chunk_size_ ? chunk_size_ : size);
  }

  uint8_t* ret = avail_buf_;
  avail_buf_ += size;
  avail_bytes_ -= size;
  return ret;
}

inline void SimpleArena::AllocateChunk(int size) {
  std::unique_ptr<uint8_t> chunk(new uint8_t[size]);
  avail_buf_ = chunk.get();
  avail_bytes_ = size;  // left-over bytes in the previous chunk cannot be used anymore.
  if (total_bytes_ == 0) {
    first_chunk_size_ = size;
  }
  total_bytes_ += size;

  chunks_.push_back(std::move(chunk));
}

inline void SimpleArena::Reset() {
  // Release all but the first chunk.
  if (first_chunk_size_ > 0) {
    chunks_.erase(chunks_.begin() + 1, chunks_.end());

    avail_buf_ = chunks_.at(0).get();
    avail_bytes_ = total_bytes_ = first_chunk_size_;
  }
}

#ifdef GDV_HELPERS
}  // namespace helpers
#endif

}  // namespace gandiva

#endif  // GANDIVA_SIMPLE_ARENA_H
