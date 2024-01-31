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
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"

namespace arrow {
namespace internal {

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
}  // namespace

ChunkResolver::ChunkResolver(const ArrayVector& chunks)
    : offsets_(MakeChunksOffsets(chunks)), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(const std::vector<const Array*>& chunks)
    : offsets_(MakeChunksOffsets(chunks)), cached_chunk_(0) {}

ChunkResolver::ChunkResolver(const RecordBatchVector& batches)
    : offsets_(MakeChunksOffsets(batches)), cached_chunk_(0) {}

}  // namespace internal
}  // namespace arrow
