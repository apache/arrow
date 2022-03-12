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
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/record_batch.h"

namespace arrow {
namespace internal {

ChunkResolver::ChunkResolver(const ArrayVector& chunks) {
  std::vector<int64_t> lengths(chunks.size());
  std::transform(chunks.begin(), chunks.end(), lengths.begin(),
                 [](const std::shared_ptr<Array>& arr) { return arr->length(); });
  num_chunks_ = static_cast<int64_t>(lengths.size());
  offsets_ = MakeEndOffsets(std::move(lengths));
}

ChunkResolver::ChunkResolver(const RecordBatchVector& batches) {
  std::vector<int64_t> lengths(batches.size());
  std::transform(
      batches.begin(), batches.end(), lengths.begin(),
      [](const std::shared_ptr<RecordBatch>& batch) { return batch->num_rows(); });
  num_chunks_ = static_cast<int64_t>(lengths.size());
  offsets_ = MakeEndOffsets(std::move(lengths));
}

}  // namespace internal
}  // namespace arrow
