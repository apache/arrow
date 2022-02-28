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

#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/type_fwd.h"

namespace arrow {
namespace compute {
namespace internal {

struct ChunkLocation {
  int64_t chunk_index, index_in_chunk;
};

// An object that resolves an array chunk depending on the index.
struct ChunkResolver {
  explicit ChunkResolver(std::vector<int64_t> lengths)
      : num_chunks_(static_cast<int64_t>(lengths.size())),
        offsets_(MakeEndOffsets(std::move(lengths))),
        cached_chunk_(0) {}

  ChunkLocation Resolve(int64_t index) const;

  static ChunkResolver FromChunks(const ArrayVector& chunks);

  static ChunkResolver FromBatches(const RecordBatchVector& batches);

 protected:
  ChunkLocation ResolveMissBisect(int64_t index) const;

  static std::vector<int64_t> MakeEndOffsets(std::vector<int64_t> lengths);

  int64_t num_chunks_;
  std::vector<int64_t> offsets_;

  mutable int64_t cached_chunk_;
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
