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
#include "arrow/record_batch.h"
#include "arrow/util/bisect.h"

namespace arrow {
namespace internal {

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index.
struct ChunkResolver {
  explicit ChunkResolver(std::vector<int64_t> lengths)
      : offsets_(ConvertLengthsToOffsets(std::move(lengths))) {}

  explicit ChunkResolver(const ArrayVector& chunks) {
    std::vector<int64_t> lengths(chunks.size());
    std::transform(chunks.begin(), chunks.end(), lengths.begin(),
                   [](const std::shared_ptr<Array>& arr) { return arr->length(); });
    offsets_ = ConvertLengthsToOffsets(std::move(lengths));
  }

  explicit ChunkResolver(const std::vector<const Array*>& chunks) {
    std::vector<int64_t> lengths(chunks.size());
    std::transform(chunks.begin(), chunks.end(), lengths.begin(),
                   [](const Array* arr) { return arr->length(); });
    offsets_ = ConvertLengthsToOffsets(std::move(lengths));
  }

  explicit ChunkResolver(const RecordBatchVector& batches) {
    std::vector<int64_t> lengths(batches.size());
    std::transform(
        batches.begin(), batches.end(), lengths.begin(),
        [](const std::shared_ptr<RecordBatch>& batch) { return batch->num_rows(); });
    offsets_ = ConvertLengthsToOffsets(std::move(lengths));
  }

  inline ChunkLocation Resolve(int64_t index) const {
    // It is common for the algorithms below to make consecutive accesses at
    // a relatively small distance from each other, hence often falling in
    // the same chunk.
    // This is trivial when merging (assuming each side of the merge uses
    // its own resolver), but also in the inner recursive invocations of
    // partitioning.
    const bool cache_hit =
        (index >= offsets_[cached_chunk_] && index < offsets_[cached_chunk_ + 1]);
    if (ARROW_PREDICT_TRUE(cache_hit)) {
      return {cached_chunk_, index - offsets_[cached_chunk_]};
    } else {
      auto chunk_index = Bisect(index, offsets_);
      cached_chunk_ = chunk_index;
      return {chunk_index, index - offsets_[chunk_index]};
    }
  }

 protected:
  std::vector<int64_t> offsets_;
  mutable int64_t cached_chunk_ = 0;
};

}  // namespace internal
}  // namespace arrow
