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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/chunk_resolver.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/span.h"
#include "arrow/util/visibility.h"

namespace arrow::compute::internal {

// The target chunk in a chunked array.
struct ResolvedChunk {
  // The target array in chunked array.
  const Array* array;
  // The index in the target array.
  int64_t index;

  ResolvedChunk(const Array* array, int64_t index) : array(array), index(index) {}

  friend bool operator==(const ResolvedChunk& left, const ResolvedChunk& right) {
    return left.array == right.array && left.index == right.index;
  }
  friend bool operator!=(const ResolvedChunk& left, const ResolvedChunk& right) {
    return left.array != right.array || left.index != right.index;
  }

  bool IsNull() const { return array->IsNull(index); }

  template <typename ArrowType, typename ViewType = GetViewType<ArrowType>>
  typename ViewType::T Value() const {
    using LogicalArrayType = typename TypeTraits<ArrowType>::ArrayType;
    auto* typed_array = checked_cast<const LogicalArrayType*>(array);
    return ViewType::LogicalValue(typed_array->GetView(index));
  }
};

// A compressed (chunk_index, index_in_chunk) pair.
// The goal of compression is to make it fit in 64 bits, allowing in place
// replacement of logical uint64_t indices with physical indices.
// (see ChunkedIndexMapper)
struct CompressedChunkLocation {
  static constexpr int kChunkIndexBits = 24;
  static constexpr int KIndexInChunkBits = 64 - kChunkIndexBits;

  static constexpr uint64_t kMaxChunkIndex = (1ULL << kChunkIndexBits) - 1;
  static constexpr uint64_t kMaxIndexInChunk = (1ULL << KIndexInChunkBits) - 1;

  CompressedChunkLocation() = default;

  constexpr uint64_t chunk_index() const { return data_ & kMaxChunkIndex; }
  constexpr uint64_t index_in_chunk() const { return data_ >> kChunkIndexBits; }

  explicit constexpr CompressedChunkLocation(uint64_t chunk_index,
                                             uint64_t index_in_chunk)
      : data_((index_in_chunk << kChunkIndexBits) | chunk_index) {}

  template <typename IndexType>
  explicit operator TypedChunkLocation<IndexType>() {
    return {static_cast<IndexType>(chunk_index()),
            static_cast<IndexType>(index_in_chunk())};
  }

 private:
  uint64_t data_;
};

static_assert(sizeof(uint64_t) == sizeof(CompressedChunkLocation));

class ChunkedArrayResolver {
 private:
  ChunkResolver resolver_;
  util::span<const Array* const> chunks_;
  std::vector<const Array*> owned_chunks_;

 public:
  explicit ChunkedArrayResolver(std::vector<const Array*>&& chunks)
      : resolver_(chunks), chunks_(chunks), owned_chunks_(std::move(chunks)) {}
  explicit ChunkedArrayResolver(util::span<const Array* const> chunks)
      : resolver_(chunks), chunks_(chunks) {}

  ARROW_DEFAULT_MOVE_AND_ASSIGN(ChunkedArrayResolver);

  ChunkedArrayResolver(const ChunkedArrayResolver& other)
      : resolver_(other.resolver_), owned_chunks_(other.owned_chunks_) {
    // Rebind span to owned_chunks_ if necessary
    chunks_ = owned_chunks_.empty() ? other.chunks_ : owned_chunks_;
  }
  ChunkedArrayResolver& operator=(const ChunkedArrayResolver& other) {
    resolver_ = other.resolver_;
    owned_chunks_ = other.owned_chunks_;
    chunks_ = owned_chunks_.empty() ? other.chunks_ : owned_chunks_;
    return *this;
  }

  ResolvedChunk Resolve(int64_t index) const {
    const auto loc = resolver_.Resolve(index);
    return {chunks_[loc.chunk_index], loc.index_in_chunk};
  }
};

ARROW_EXPORT std::vector<const Array*> GetArrayPointers(const ArrayVector& arrays);

// A class that turns logical (linear) indices into physical (chunked) indices,
// and vice-versa.
class ARROW_EXPORT ChunkedIndexMapper {
 public:
  ChunkedIndexMapper(const std::vector<const Array*>& chunks, uint64_t* indices_begin,
                     uint64_t* indices_end)
      : ChunkedIndexMapper(util::span(chunks), indices_begin, indices_end) {}
  ChunkedIndexMapper(util::span<const Array* const> chunks, uint64_t* indices_begin,
                     uint64_t* indices_end)
      : chunk_lengths_(GetChunkLengths(chunks)),
        indices_begin_(indices_begin),
        indices_end_(indices_end) {}
  ChunkedIndexMapper(const RecordBatchVector& chunks, uint64_t* indices_begin,
                     uint64_t* indices_end)
      : chunk_lengths_(GetChunkLengths(chunks)),
        indices_begin_(indices_begin),
        indices_end_(indices_end) {}

  // Turn the original uint64_t logical indices into physical. This reuses the
  // same memory area, so the logical indices cannot be used anymore until
  // PhysicalToLogical() is called.
  //
  // This assumes that the logical indices are originally chunk-partitioned.
  Result<std::pair<CompressedChunkLocation*, CompressedChunkLocation*>>
  LogicalToPhysical();

  // Turn the physical indices back into logical, making the uint64_t indices
  // usable again.
  Status PhysicalToLogical();

 private:
  static std::vector<int64_t> GetChunkLengths(util::span<const Array* const> chunks);
  static std::vector<int64_t> GetChunkLengths(const RecordBatchVector& chunks);

  std::vector<int64_t> chunk_lengths_;
  uint64_t* indices_begin_;
  uint64_t* indices_end_;
};

}  // namespace arrow::compute::internal
