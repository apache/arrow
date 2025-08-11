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

#include "arrow/chunk_resolver.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"

namespace arrow {

using util::span;

namespace {
template <typename T>
int64_t GetLength(const T& array) {
  // General case assumes argument is an Array pointer
  return array->length();
}

template <>
int64_t GetLength<std::shared_ptr<RecordBatch>>(
    const std::shared_ptr<RecordBatch>& batch) {
  return batch->num_rows();
}

template <typename T>
inline std::vector<int64_t> MakeChunksOffsets(span<T> chunks) {
  std::vector<int64_t> offsets(chunks.size() + 1);
  int64_t offset = 0;
  std::transform(chunks.begin(), chunks.end(), offsets.begin(),
                 [&offset](const T& chunk) {
                   auto curr_offset = offset;
                   offset += GetLength(chunk);
                   return curr_offset;
                 });
  offsets[chunks.size()] = offset;
  return offsets;
}

template <typename IndexType>
inline TypedChunkLocation<IndexType> ResolveOneInline(uint32_t num_offsets,
                                                      const uint64_t* offsets,
                                                      IndexType typed_logical_index,
                                                      int32_t num_chunks,
                                                      int32_t chunk_hint) {
  const auto index = static_cast<uint64_t>(typed_logical_index);
  // use or update chunk_hint
  if (index >= offsets[chunk_hint] &&
      (chunk_hint == num_chunks || index < offsets[chunk_hint + 1])) {
    // hint is correct!
  } else {
    // lo < hi is guaranteed by `num_offsets = chunks.size() + 1`
    auto chunk_index =
        ChunkResolver::Bisect(index, offsets, /*lo=*/0, /*hi=*/num_offsets);
    chunk_hint = static_cast<int32_t>(chunk_index);
  }
  // chunk_index is in [0, chunks.size()] no matter what the value
  // of logical_index is, so it's always safe to dereference offsets
  // as it contains chunks.size()+1 values.
  auto loc = TypedChunkLocation<IndexType>(
      /*chunk_index=*/chunk_hint,
      /*index_in_chunk=*/typed_logical_index -
          static_cast<IndexType>(offsets[chunk_hint]));
#if defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER)
  // Make it more likely that Valgrind/ASAN can catch an invalid memory
  // access by poisoning the index-in-chunk value when the logical
  // index is out-of-bounds.
  if (static_cast<int32_t>(loc.chunk_index) == num_chunks) {
    loc.index_in_chunk = std::numeric_limits<IndexType>::max();
  }
#endif
  return loc;
}

/// \pre all the pre-conditions of ChunkResolver::ResolveMany()
/// \pre num_offsets - 1 <= std::numeric_limits<IndexType>::max()
template <typename IndexType>
void ResolveManyInline(uint32_t num_offsets, const int64_t* signed_offsets,
                       int64_t n_indices, const IndexType* logical_index_vec,
                       TypedChunkLocation<IndexType>* out_chunk_location_vec,
                       int32_t chunk_hint) {
  auto* offsets = reinterpret_cast<const uint64_t*>(signed_offsets);
  const auto num_chunks = static_cast<int32_t>(num_offsets - 1);
  // chunk_hint in [0, num_offsets) per the precondition.
  for (int64_t i = 0; i < n_indices; i++) {
    const auto typed_logical_index = logical_index_vec[i];
    const auto loc = ResolveOneInline(num_offsets, offsets, typed_logical_index,
                                      num_chunks, chunk_hint);
    out_chunk_location_vec[i] = loc;
    chunk_hint = static_cast<int32_t>(loc.chunk_index);
  }
}

}  // namespace

ChunkResolver::ChunkResolver(const ArrayVector& chunks) noexcept
    : offsets_(MakeChunksOffsets(span(chunks))), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(span<const Array* const> chunks) noexcept
    : offsets_(MakeChunksOffsets(chunks)), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(const RecordBatchVector& batches) noexcept
    : offsets_(MakeChunksOffsets(span(batches))), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(ChunkResolver&& other) noexcept
    : offsets_(std::move(other.offsets_)),
      cached_chunk_(other.cached_chunk_.load(std::memory_order_relaxed)) {}

ChunkResolver& ChunkResolver::operator=(ChunkResolver&& other) noexcept {
  offsets_ = std::move(other.offsets_);
  cached_chunk_.store(other.cached_chunk_.load(std::memory_order_relaxed));
  return *this;
}

ChunkResolver::ChunkResolver(const ChunkResolver& other) noexcept
    : offsets_(other.offsets_), cached_chunk_(0) {}

ChunkResolver& ChunkResolver::operator=(const ChunkResolver& other) noexcept {
  offsets_ = other.offsets_;
  cached_chunk_.store(0, std::memory_order_relaxed);
  return *this;
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint8_t* logical_index_vec,
                                    TypedChunkLocation<uint8_t>* out_chunk_location_vec,
                                    int32_t chunk_hint) const {
  ResolveManyInline(static_cast<uint32_t>(offsets_.size()), offsets_.data(), n_indices,
                    logical_index_vec, out_chunk_location_vec, chunk_hint);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint16_t* logical_index_vec,
                                    TypedChunkLocation<uint16_t>* out_chunk_location_vec,
                                    int32_t chunk_hint) const {
  ResolveManyInline(static_cast<uint32_t>(offsets_.size()), offsets_.data(), n_indices,
                    logical_index_vec, out_chunk_location_vec, chunk_hint);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint32_t* logical_index_vec,
                                    TypedChunkLocation<uint32_t>* out_chunk_location_vec,
                                    int32_t chunk_hint) const {
  ResolveManyInline(static_cast<uint32_t>(offsets_.size()), offsets_.data(), n_indices,
                    logical_index_vec, out_chunk_location_vec, chunk_hint);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint64_t* logical_index_vec,
                                    TypedChunkLocation<uint64_t>* out_chunk_location_vec,
                                    int32_t chunk_hint) const {
  ResolveManyInline(static_cast<uint32_t>(offsets_.size()), offsets_.data(), n_indices,
                    logical_index_vec, out_chunk_location_vec, chunk_hint);
}

}  // namespace arrow
