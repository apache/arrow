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
#include <cstdint>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index
struct ChunkResolver {
  // TODO: This copy constructor is a hack and not semantically correct because this
  // class contains a `std::atomic` member (non copyable/movable),
  // but this explicit copy constructor allows compilation even if copies are performed
  // such as in TableSorter/ResolveSortKey in vector_sort.cc.
  ChunkResolver(const ChunkResolver& chunks) : offsets_{chunks.offsets_} {
    cached_chunk_.store(chunks.cached_chunk_);
  }

  explicit ChunkResolver(const ArrayVector& chunks);

  explicit ChunkResolver(const std::vector<const Array*>& chunks);

  explicit ChunkResolver(const RecordBatchVector& batches);

  /// \brief Return a ChunkLocation containing the chunk index and in-chunk value index of
  /// the chunked array at logical index
  inline ChunkLocation Resolve(const int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    if (offsets_.size() <= 1) {
      return {0, index};
    }
    const auto cached_chunk = cached_chunk_.load();
    const bool cache_hit =
        (index >= offsets_[cached_chunk] && index < offsets_[cached_chunk + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk, index - offsets_[cached_chunk]};
    }
    auto chunk_index = Bisect(index);
    cached_chunk_.store(chunk_index);
    return {chunk_index, index - offsets_[chunk_index]};
  }

 protected:
  // Find the chunk index corresponding to a value index using binary search
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

 private:
  const std::vector<int64_t> offsets_;
  mutable std::atomic<int64_t> cached_chunk_;
};

}  // namespace internal
}  // namespace arrow
