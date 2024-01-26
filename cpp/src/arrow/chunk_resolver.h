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

#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

struct ChunkLocation {
  /// \brief Index of the chunk in the array of chunks
  ///
  /// The value is always in the range `[0, chunks.size()]`. `chunks.size()` is used
  /// to represent out-of-bounds locations.
  int64_t chunk_index;

  /// \brief Index of the value in the chunk
  ///
  /// The value is undefined if chunk_index >= chunks.size()
  int64_t index_in_chunk;
};

/// \brief An utility that incrementally resolves logical indices into
/// physical indices in a chunked array.
struct ARROW_EXPORT ChunkResolver {
 private:
  /// \brief Array containing `chunks.size() + 1` offsets.
  ///
  /// `offsets_[i]` is the starting logical index of chunk `i`. `offsets_[0]` is always 0
  /// and `offsets_[chunks.size()]` is the logical length of the chunked array.
  std::vector<int64_t> offsets_;

  /// \brief Cache of the index of the last resolved chunk.
  ///
  /// \invariant `cached_chunk_ in [0, chunks.size()]`
  mutable std::atomic<int64_t> cached_chunk_;

 public:
  explicit ChunkResolver(const ArrayVector& chunks);
  explicit ChunkResolver(const std::vector<const Array*>& chunks);
  explicit ChunkResolver(const RecordBatchVector& batches);

  ChunkResolver(ChunkResolver&& other) noexcept
      : offsets_(std::move(other.offsets_)), cached_chunk_(other.cached_chunk_.load()) {}

  ChunkResolver& operator=(ChunkResolver&& other) {
    offsets_ = std::move(other.offsets_);
    cached_chunk_.store(other.cached_chunk_.load());
    return *this;
  }

  /// \brief Resolve a logical index to a ChunkLocation.
  ///
  /// The returned ChunkLocation contains the chunk index and the within-chunk index
  /// equivalent to the logical index.
  ///
  /// \pre index >= 0
  /// \post location.chunk_index in [0, chunks.size()]
  /// \param index The logical index to resolve
  /// \return ChunkLocation with a valid chunk_index if index is within
  ///         bounds, or with chunk_index == chunks.size() if logical index is
  ///         `>= chunked_array.length()`.
  inline ChunkLocation Resolve(const int64_t index) const {
    // It is common for algorithms sequentially processing arrays to make consecutive
    // accesses at a relatively small distance from each other, hence often falling in the
    // same chunk.
    //
    // This is guaranteed when merging (assuming each side of the merge uses its
    // own resolver), and is the most common case in recursive invocations of
    // partitioning.
    if (offsets_.size() <= 1) {
      return {0, index};
    }
    const auto cached_chunk = cached_chunk_.load();
    // XXX: the access below is unsafe because cached_chunk+1 can be out-of-bounds
    const bool cache_hit =
        (index >= offsets_[cached_chunk] && index < offsets_[cached_chunk + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk, index - offsets_[cached_chunk]};
    }
    auto chunk_index = Bisect(index);
    assert(chunk_index < static_cast<int64_t>(offsets_.size()));
    cached_chunk_.store(chunk_index);
    return {chunk_index, index - offsets_[chunk_index]};
  }

 protected:
  /// \brief Find the index of the chunk that contains the logical index.
  ///
  /// Any non-negative index is accepted.
  ///
  /// \pre index >= 0
  /// \return Chunk index in `[0, chunks.size())` or `chunks.size()` if the logical index
  /// is out-of-bounds.
  inline int64_t Bisect(const int64_t index) const {
    // Like std::upper_bound(), but hand-written as it can help the compiler.
    // Search [lo, lo + n)
    int64_t lo = 0;
    auto n = static_cast<int64_t>(offsets_.size());
    while (n > 1) {
      const int64_t m = n >> 1;
      const int64_t mid = lo + m;
      if (static_cast<int64_t>(index) >= offsets_[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    }
    return lo;
  }
};

}  // namespace arrow::internal
