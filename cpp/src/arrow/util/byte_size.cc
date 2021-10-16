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

namespace arrow {

namespace util {

int64_t EstimateBufferSize(const ArrayData& array_data) {
  int64_t sum = 0;
  for (const auto& buffer : array_data.buffers) {
    if (buffer) {
      sum += buffer->size();
    }
  }
  for (const auto& child : array_data.child_data) {
    sum += EstimateBufferSize(*child);
  }
  return sum;
}

int64_t EstimateBufferSize(const Array& array) {
  return EstimateBufferSize(*array.data());
}

int64_t EstimateBufferSize(const ChunkedArray& chunked_array) {
  int64_t sum = 0;
  for (const auto& chunk : chunked_array.chunks()) {
    sum += EstimateBufferSize(*chunk);
  }
  return sum;
}

int64_t EstimateBufferSize(const RecordBatch& record_batch) {
  int64_t sum = 0;
  for (const auto& column : record_batch.columns()) {
    sum += EstimateBufferSize(*column);
  }
  return sum;
}

int64_t EstimateBufferSize(const Table& table) {
  int64_t sum = 0;
  for (const auto& column : table.columns()) {
    sum += EstimateBufferSize(*column);
  }
  return sum;
}

}  // namespace util

}  // namespace arrow