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
#include <limits>
#include <vector>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"

namespace arrow::internal {

struct ChunkResolver;

struct ChunkLocation {
  /// \brief Index of the chunk in the array of chunks
  ///
  /// The value is always in the range `[0, chunks.size()]`. `chunks.size()` is used
  /// to represent out-of-bounds locations.
  int64_t chunk_index = 0;

  /// \brief Index of the value in the chunk
  ///
  /// The value is UNDEFINED if chunk_index >= chunks.size()
  int64_t index_in_chunk = 0;

  /// \brief Create a ChunkLocation without asserting any preconditions.
  static ChunkLocation Forge(int64_t chunk_index, int64_t index_in_chunk) {
    ChunkLocation loc;
    loc.chunk_index = chunk_index;
    loc.index_in_chunk = index_in_chunk;
    return loc;
  }

  ChunkLocation() = default;

 public:
  template <class ChunkResolver>
  ChunkLocation(const ChunkResolver& resolver, int64_t chunk_index,
                int64_t index_in_chunk)
      : chunk_index(chunk_index), index_in_chunk(index_in_chunk) {
#ifndef NDEBUG
    assert(chunk_index >= 0 && chunk_index <= resolver.num_chunks());
    if (chunk_index == resolver.num_chunks()) {
#if defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER)
      // The value of index_in_chunk is undefined (can't be trusted) when
      // chunk_index==chunks.size(), so make it easier for ASAN
      // to detect misuse.
      this->index_in_chunk = std::numeric_limits<int64_t>::min();
#endif
    } else {
      assert(index_in_chunk == 0 || index_in_chunk < resolver.chunk_length(chunk_index));
    }
#endif  // NDEBUG
  }

  bool operator==(ChunkLocation other) const {
    return chunk_index == other.chunk_index && index_in_chunk == other.index_in_chunk;
  }
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
  explicit ChunkResolver(const ArrayVector& chunks) noexcept;
  explicit ChunkResolver(const std::vector<const Array*>& chunks) noexcept;
  explicit ChunkResolver(const RecordBatchVector& batches) noexcept;

  /// \brief Construct a ChunkResolver from a vector of chunks.size() + 1 offsets.
  ///
  /// The first offset must be 0 and the last offset must be the logical length of the
  /// chunked array. Each offset before the last represents the starting logical index of
  /// the corresponding chunk.
  explicit ChunkResolver(std::vector<int64_t> offsets) noexcept
      : offsets_(std::move(offsets)), cached_chunk_(0) {
#ifndef NDEBUG
    assert(offsets_.size() >= 1);
    assert(offsets_[0] == 0);
    for (size_t i = 1; i < offsets_.size(); i++) {
      assert(offsets_[i] >= offsets_[i - 1]);
    }
#endif
  }

  ChunkResolver(ChunkResolver&& other) noexcept;
  ChunkResolver& operator=(ChunkResolver&& other) noexcept;

  ChunkResolver(const ChunkResolver& other) noexcept;
  ChunkResolver& operator=(const ChunkResolver& other) noexcept;

  int64_t logical_array_length() const { return offsets_.back(); }
  int64_t num_chunks() const { return static_cast<int64_t>(offsets_.size()) - 1; }

  int64_t chunk_length(int64_t chunk_index) const {
    return offsets_[chunk_index + 1] - offsets_[chunk_index];
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
  inline ChunkLocation Resolve(int64_t index) const {
    const auto cached_chunk = cached_chunk_.load(std::memory_order_relaxed);
    const auto chunk_index =
        ResolveChunkIndex</*StoreCachedChunk=*/true>(index, cached_chunk);
    return ChunkLocation{*this, chunk_index, index - offsets_[chunk_index]};
  }

  /// \brief Resolve a logical index to a ChunkLocation.
  ///
  /// The returned ChunkLocation contains the chunk index and the within-chunk index
  /// equivalent to the logical index.
  ///
  /// \pre index >= 0
  /// \post location.chunk_index in [0, chunks.size()]
  /// \param index The logical index to resolve
  /// \param hint ChunkLocation{} or the last ChunkLocation returned by
  ///             this ChunkResolver.
  /// \return ChunkLocation with a valid chunk_index if index is within
  ///         bounds, or with chunk_index == chunks.size() if logical index is
  ///         `>= chunked_array.length()`.
  inline ChunkLocation ResolveWithHint(int64_t index, ChunkLocation hint) const {
    assert(hint.chunk_index < static_cast<int64_t>(offsets_.size()));
    const auto chunk_index =
        ResolveChunkIndex</*StoreCachedChunk=*/false>(index, hint.chunk_index);
    return ChunkLocation{*this, chunk_index, index - offsets_[chunk_index]};
  }

  /// \brief Resolve `n` logical indices to chunk indices.
  ///
  /// \pre logical_index_vec[i] < n (for valid chunk index results)
  /// \pre out_chunk_index_vec has space for `n` elements
  /// \post chunk_hint in [0, chunks.size()]
  /// \post out_chunk_index_vec[i] in [0, chunks.size()] for i in [0, n)
  /// \post if logical_index_vec[i] >= chunked_array.length(), then
  ///       out_chunk_index_vec[i] == chunks.size()
  ///       and out_index_in_chunk_vec[i] is undefined (can be out-of-bounds)
  ///
  /// \param n The number of logical indices to resolve
  /// \param logical_index_vec The logical indices to resolve
  /// \param out_chunk_index_vec The output array where the chunk indices will be written
  /// \param chunk_hint 0 or the last chunk_index produced by ResolveMany
  /// \param out_index_in_chunk_vec If not NULLPTR, the output array where the
  ///                               within-chunk indices will be written
  /// \return false iff chunks.size() > std::numeric_limits<IndexType>::max()
  template <typename IndexType>
  [[nodiscard]] std::enable_if_t<std::is_unsigned_v<IndexType>, bool> ResolveMany(
      int64_t n, const IndexType* ARROW_RESTRICT logical_index_vec,
      IndexType* ARROW_RESTRICT out_chunk_index_vec, IndexType chunk_hint = 0,
      IndexType* ARROW_RESTRICT out_index_in_chunk_vec = nullptr) const {
    return ResolveManyTyped(n, logical_index_vec, out_chunk_index_vec, chunk_hint,
                            out_index_in_chunk_vec);
  }

 private:
  template <bool StoreCachedChunk>
  inline int64_t ResolveChunkIndex(int64_t index, int64_t cached_chunk) const {
    // It is common for algorithms sequentially processing arrays to make consecutive
    // accesses at a relatively small distance from each other, hence often falling in the
    // same chunk.
    //
    // This is guaranteed when merging (assuming each side of the merge uses its
    // own resolver), and is the most common case in recursive invocations of
    // partitioning.
    const auto num_offsets = static_cast<int64_t>(offsets_.size());
    const int64_t* offsets = offsets_.data();
    if (ARROW_PREDICT_TRUE(index >= offsets[cached_chunk]) &&
        (cached_chunk + 1 == num_offsets || index < offsets[cached_chunk + 1])) {
      return cached_chunk;
    }
    // lo < hi is guaranteed by `num_offsets = chunks.size() + 1`
    const auto chunk_index = Bisect(index, offsets, /*lo=*/0, /*hi=*/num_offsets);
    if constexpr (StoreCachedChunk) {
      assert(chunk_index < static_cast<int64_t>(offsets_.size()));
      cached_chunk_.store(chunk_index, std::memory_order_relaxed);
    }
    return chunk_index;
  }

  template <typename IndexType>
  bool ResolveManyTyped(int64_t n, const IndexType* logical_index_vec,
                        IndexType* out_chunk_index_vec, IndexType chunk_hint,
                        IndexType* out_index_in_chunk_vec) const {
    // The max value returned by Bisect is `offsets.size() - 1` (= chunks.size()).
    constexpr auto kMaxIndexTypeValue = std::numeric_limits<IndexType>::max();
    const bool chunk_index_fits_on_type =
        static_cast<uint64_t>(offsets_.size() - 1) <= kMaxIndexTypeValue;
    if (ARROW_PREDICT_FALSE(!chunk_index_fits_on_type)) {
      return false;
    }
    ResolveManyImpl(n, logical_index_vec, out_chunk_index_vec, chunk_hint,
                    out_index_in_chunk_vec);
    return true;
  }

  template <>
  bool ResolveManyTyped<uint64_t>(int64_t n, const uint64_t* logical_index_vec,
                                  uint64_t* out_chunk_index_vec, uint64_t chunk_hint,
                                  uint64_t* out_index_in_chunk_vec) const {
    ResolveManyImpl(n, logical_index_vec, out_chunk_index_vec, chunk_hint,
                    out_index_in_chunk_vec);
    return true;
  }

  /// \pre all the pre-conditions of ChunkResolver::ResolveMany()
  /// \pre num_offsets - 1 <= std::numeric_limits<IndexType>::max()
  void ResolveManyImpl(int64_t n, const uint8_t*, uint8_t*, uint8_t, uint8_t*) const;
  void ResolveManyImpl(int64_t n, const uint16_t*, uint16_t*, uint16_t, uint16_t*) const;
  void ResolveManyImpl(int64_t n, const uint32_t*, uint32_t*, uint32_t, uint32_t*) const;
  void ResolveManyImpl(int64_t n, const uint64_t*, uint64_t*, uint64_t, uint64_t*) const;

 public:
  /// \brief Find the index of the chunk that contains the logical index.
  ///
  /// Any non-negative index is accepted. When `hi=num_offsets`, the largest
  /// possible return value is `num_offsets-1` which is equal to
  /// `chunks.size()`. Which is returned when the logical index is greater or
  /// equal the logical length of the chunked array.
  ///
  /// \pre index >= 0 (otherwise, when index is negative, lo is returned)
  /// \pre lo < hi
  /// \pre lo >= 0 && hi <= offsets_.size()
  static inline int64_t Bisect(int64_t index, const int64_t* offsets, int64_t lo,
                               int64_t hi) {
    // Similar to std::upper_bound(), but slightly different as our offsets
    // array always starts with 0.
    auto n = hi - lo;
    // First iteration does not need to check for n > 1
    // (lo < hi is guaranteed by the precondition).
    assert(n > 1 && "lo < hi is a precondition of Bisect");
    do {
      const int64_t m = n >> 1;
      const int64_t mid = lo + m;
      if (index >= offsets[mid]) {
        lo = mid;
        n -= m;
      } else {
        n = m;
      }
    } while (n > 1);
    return lo;
  }
};

}  // namespace arrow::internal
