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

#include "arrow/util/byte_size.h"

#include <cstdint>
#include <unordered_set>

#include "arrow/array.h"
#include "arrow/buffer.h"

namespace arrow {

namespace util {

namespace {

int64_t DoTotalBufferSize(const ArrayData& array_data,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& buffer : array_data.buffers) {
    if (buffer && seen_buffers->insert(buffer->data()).second) {
      sum += buffer->size();
    }
  }
  for (const auto& child : array_data.child_data) {
    sum += DoTotalBufferSize(*child, seen_buffers);
  }
  if (array_data.dictionary) {
    sum += DoTotalBufferSize(*array_data.dictionary, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const Array& array,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  return DoTotalBufferSize(*array.data(), seen_buffers);
}

int64_t DoTotalBufferSize(const ChunkedArray& chunked_array,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& chunk : chunked_array.chunks()) {
    sum += DoTotalBufferSize(*chunk, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const RecordBatch& record_batch,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& column : record_batch.columns()) {
    sum += DoTotalBufferSize(*column, seen_buffers);
  }
  return sum;
}

int64_t DoTotalBufferSize(const Table& table,
                          std::unordered_set<const uint8_t*>* seen_buffers) {
  int64_t sum = 0;
  for (const auto& column : table.columns()) {
    sum += DoTotalBufferSize(*column, seen_buffers);
  }
  return sum;
}

}  // namespace

int64_t TotalBufferSize(const ArrayData& array_data) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(array_data, &seen_buffers);
}

int64_t TotalBufferSize(const Array& array) { return TotalBufferSize(*array.data()); }

int64_t TotalBufferSize(const ChunkedArray& chunked_array) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(chunked_array, &seen_buffers);
}

int64_t TotalBufferSize(const RecordBatch& record_batch) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(record_batch, &seen_buffers);
}

int64_t TotalBufferSize(const Table& table) {
  std::unordered_set<const uint8_t*> seen_buffers;
  return DoTotalBufferSize(table, &seen_buffers);
}

}  // namespace util

}  // namespace arrow
