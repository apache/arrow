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

namespace arrow::internal {

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
inline std::vector<int64_t> MakeChunksOffsets(const std::vector<T>& chunks) {
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

/// \pre all the pre-conditions of ChunkResolver::ResolveMany()
/// \pre num_offsets - 1 <= std::numeric_limits<IndexType>::max()
template <typename IndexType>
void ResolveManyInline(size_t num_offsets, const int64_t* signed_offsets,
                       int64_t n_indices, const IndexType* logical_index_vec,
                       IndexType* out_chunk_index_vec, IndexType chunk_hint,
                       IndexType* out_index_in_chunk_vec) {
  auto* offsets = reinterpret_cast<const uint64_t*>(signed_offsets);
  const auto num_chunks = static_cast<IndexType>(num_offsets - 1);
  // chunk_hint in [0, num_offsets) per the precondition.
  for (int64_t i = 0; i < n_indices; i++) {
    const auto index = static_cast<uint64_t>(logical_index_vec[i]);
    if (index >= offsets[chunk_hint] &&
        (chunk_hint == num_chunks || index < offsets[chunk_hint + 1])) {
      out_chunk_index_vec[i] = chunk_hint;  // hint is correct!
      continue;
    }
    // lo < hi is guaranteed by `num_offsets = chunks.size() + 1`
    auto chunk_index =
        ChunkResolver::Bisect(index, offsets, /*lo=*/0, /*hi=*/num_offsets);
    chunk_hint = static_cast<IndexType>(chunk_index);
    out_chunk_index_vec[i] = chunk_hint;
  }
  if (out_index_in_chunk_vec != NULLPTR) {
    for (int64_t i = 0; i < n_indices; i++) {
      auto logical_index = logical_index_vec[i];
      auto chunk_index = out_chunk_index_vec[i];
      // chunk_index is in [0, chunks.size()] no matter what the
      // value of logical_index is, so it's always safe to dereference
      // offset_ as it contains chunks.size()+1 values.
      out_index_in_chunk_vec[i] =
          logical_index - static_cast<IndexType>(offsets[chunk_index]);
#if defined(ARROW_VALGRIND) || defined(ADDRESS_SANITIZER)
      // Make it more likely that Valgrind/ASAN can catch an invalid memory
      // access by poisoning out_index_in_chunk_vec[i] when the logical
      // index is out-of-bounds.
      if (chunk_index == num_chunks) {
        out_index_in_chunk_vec[i] = std::numeric_limits<IndexType>::max();
      }
#endif
    }
  }
}

}  // namespace

ChunkResolver::ChunkResolver(const ArrayVector& chunks) noexcept
    : offsets_(MakeChunksOffsets(chunks)), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(const std::vector<const Array*>& chunks) noexcept
    : offsets_(MakeChunksOffsets(chunks)), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(const RecordBatchVector& batches) noexcept
    : offsets_(MakeChunksOffsets(batches)), cached_chunk_(0) {}

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
                                    uint8_t* out_chunk_index_vec, uint8_t chunk_hint,
                                    uint8_t* out_index_in_chunk_vec) const {
  ResolveManyInline(offsets_.size(), offsets_.data(), n_indices, logical_index_vec,
                    out_chunk_index_vec, chunk_hint, out_index_in_chunk_vec);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint32_t* logical_index_vec,
                                    uint32_t* out_chunk_index_vec, uint32_t chunk_hint,
                                    uint32_t* out_index_in_chunk_vec) const {
  ResolveManyInline(offsets_.size(), offsets_.data(), n_indices, logical_index_vec,
                    out_chunk_index_vec, chunk_hint, out_index_in_chunk_vec);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint16_t* logical_index_vec,
                                    uint16_t* out_chunk_index_vec, uint16_t chunk_hint,
                                    uint16_t* out_index_in_chunk_vec) const {
  ResolveManyInline(offsets_.size(), offsets_.data(), n_indices, logical_index_vec,
                    out_chunk_index_vec, chunk_hint, out_index_in_chunk_vec);
}

void ChunkResolver::ResolveManyImpl(int64_t n_indices, const uint64_t* logical_index_vec,
                                    uint64_t* out_chunk_index_vec, uint64_t chunk_hint,
                                    uint64_t* out_index_in_chunk_vec) const {
  ResolveManyInline(offsets_.size(), offsets_.data(), n_indices, logical_index_vec,
                    out_chunk_index_vec, chunk_hint, out_index_in_chunk_vec);
}

}  // namespace arrow::internal
