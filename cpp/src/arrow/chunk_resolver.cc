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
inline std::vector<int64_t> MakeArraysOffsets(const ArrayVector& chunks) {
  std::vector<int64_t> offsets(chunks.size() + 1);
  int64_t offset = 0;
  std::transform(chunks.begin(), chunks.end(), offsets.begin(),
                 [&offset](const std::shared_ptr<Array>& arr) {
                   auto curr_offset = offset;
                   offset += arr->length();
                   return curr_offset;
                 });
  offsets[chunks.size()] = offset;
  return offsets;
}

inline std::vector<int64_t> MakeArrayPointersOffsets(
    const std::vector<const Array*>& chunks) {
  std::vector<int64_t> offsets(chunks.size() + 1);
  int64_t offset = 0;
  std::transform(chunks.begin(), chunks.end(), offsets.begin(),
                 [&offset](const Array* arr) {
                   auto curr_offset = offset;
                   offset += arr->length();
                   return curr_offset;
                 });
  offsets[chunks.size()] = offset;
  return offsets;
}

inline std::vector<int64_t> MakeRecordBatchesOffsets(const RecordBatchVector& batches) {
  std::vector<int64_t> offsets(batches.size() + 1);
  int64_t offset = 0;
  std::transform(batches.begin(), batches.end(), offsets.begin(),
                 [&offset](const std::shared_ptr<RecordBatch>& batch) {
                   auto curr_offset = offset;
                   offset += batch->num_rows();
                   return curr_offset;
                 });
  offsets[batches.size()] = offset;
  return offsets;
}
}  // namespace

ChunkResolver::ChunkResolver(const ArrayVector& chunks)
    : offsets_(MakeArraysOffsets(chunks)) {}

ChunkResolver::ChunkResolver(const std::vector<const Array*>& chunks)
    : offsets_(MakeArrayPointersOffsets(chunks)) {}

ChunkResolver::ChunkResolver(const RecordBatchVector& batches)
    : offsets_(MakeRecordBatchesOffsets(batches)) {}

}  // namespace internal
}  // namespace arrow
