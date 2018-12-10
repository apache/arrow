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

#include "benchmark/benchmark.h"

#include <cstdint>
#include <sstream>
#include <string>

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/test-util.h"

namespace arrow {

template <typename TYPE>
std::shared_ptr<RecordBatch> MakeRecordBatch(int64_t total_size, int64_t num_fields) {
  using T = typename TYPE::c_type;
  size_t itemsize = sizeof(T);
  int64_t length = total_size / num_fields / itemsize;

  auto type = TypeTraits<TYPE>::type_singleton();

  std::vector<bool> is_valid;
  random_is_valid(length, 0.1, &is_valid);

  std::vector<T> values;
  randint<T>(length, 0, 100, &values);

  typename TypeTraits<TYPE>::BuilderType builder(type, default_memory_pool());
  for (size_t i = 0; i < values.size(); ++i) {
    if (is_valid[i]) {
      ABORT_NOT_OK(builder.Append(values[i]));
    } else {
      ABORT_NOT_OK(builder.AppendNull());
    }
  }
  std::shared_ptr<Array> array;
  ABORT_NOT_OK(builder.Finish(&array));

  ArrayVector arrays;
  std::vector<std::shared_ptr<Field>> fields;
  for (int64_t i = 0; i < num_fields; ++i) {
    std::stringstream ss;
    ss << "f" << i;
    fields.push_back(field(ss.str(), type));
    arrays.push_back(array);
  }

  auto schema = std::make_shared<Schema>(fields);
  return RecordBatch::Make(schema, length, arrays);
}

static void BM_WriteRecordBatch(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;

  std::shared_ptr<ResizableBuffer> buffer;
  ABORT_NOT_OK(AllocateResizableBuffer(kTotalSize & 2, &buffer));
  auto record_batch = MakeRecordBatch<Int64Type>(kTotalSize, state.range(0));

  while (state.KeepRunning()) {
    io::BufferOutputStream stream(buffer);
    int32_t metadata_length;
    int64_t body_length;
    if (!ipc::WriteRecordBatch(*record_batch, 0, &stream, &metadata_length, &body_length,
                               default_memory_pool())
             .ok()) {
      state.SkipWithError("Failed to write!");
    }
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

static void BM_ReadRecordBatch(benchmark::State& state) {  // NOLINT non-const reference
  // 1MB
  constexpr int64_t kTotalSize = 1 << 20;

  std::shared_ptr<ResizableBuffer> buffer;
  ABORT_NOT_OK(AllocateResizableBuffer(kTotalSize & 2, &buffer));
  auto record_batch = MakeRecordBatch<Int64Type>(kTotalSize, state.range(0));

  io::BufferOutputStream stream(buffer);

  int32_t metadata_length;
  int64_t body_length;
  if (!ipc::WriteRecordBatch(*record_batch, 0, &stream, &metadata_length, &body_length,
                             default_memory_pool())
           .ok()) {
    state.SkipWithError("Failed to write!");
  }

  while (state.KeepRunning()) {
    std::shared_ptr<RecordBatch> result;
    io::BufferReader reader(buffer);

    if (!ipc::ReadRecordBatch(record_batch->schema(), &reader, &result).ok()) {
      state.SkipWithError("Failed to read!");
    }
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize);
}

BENCHMARK(BM_WriteRecordBatch)
    ->RangeMultiplier(4)
    ->Range(1, 1 << 13)
    ->MinTime(1.0)
    ->UseRealTime();

BENCHMARK(BM_ReadRecordBatch)
    ->RangeMultiplier(4)
    ->Range(1, 1 << 13)
    ->MinTime(1.0)
    ->UseRealTime();

}  // namespace arrow
