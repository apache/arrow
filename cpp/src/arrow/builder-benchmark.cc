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

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"

namespace arrow {

constexpr int64_t kFinalSize = 256;

static void BM_BuildPrimitiveArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    Int64Builder builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data(), data.size(), nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t) *
                          kFinalSize);
}

static void BM_BuildVectorNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<int64_t> data(256 * 1024, 100);
  while (state.KeepRunning()) {
    std::vector<int64_t> builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      builder.insert(builder.end(), data.cbegin(), data.cend());
    }
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t) *
                          kFinalSize);
}

static void BM_BuildAdaptiveIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<int16_t>::max()) * 256;
  int64_t chunk_size = size / 8;
  std::vector<int64_t> data;
  for (int64_t i = 0; i < size; i++) {
    data.push_back(i);
  }
  while (state.KeepRunning()) {
    AdaptiveIntBuilder builder;
    for (int64_t i = 0; i < size; i += chunk_size) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data() + i, chunk_size, nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildAdaptiveIntNoNullsScalarAppend(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<int16_t>::max()) * 256;
  std::vector<int64_t> data;
  for (int64_t i = 0; i < size; i++) {
    data.push_back(i);
  }
  while (state.KeepRunning()) {
    AdaptiveIntBuilder builder;
    for (int64_t i = 0; i < size; i++) {
      ABORT_NOT_OK(builder.Append(data[i]));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildAdaptiveUIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  int64_t size = static_cast<int64_t>(std::numeric_limits<uint16_t>::max()) * 256;
  int64_t chunk_size = size / 8;
  std::vector<uint64_t> data;
  for (uint64_t i = 0; i < static_cast<uint64_t>(size); i++) {
    data.push_back(i);
  }
  while (state.KeepRunning()) {
    AdaptiveUIntBuilder builder;
    for (int64_t i = 0; i < size; i += chunk_size) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data() + i, chunk_size, nullptr));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * sizeof(int64_t));
}

static void BM_BuildBooleanArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  // 2 MiB block
  std::vector<uint8_t> data(2 * 1024 * 1024);
  constexpr uint8_t bit_pattern = 0xcc;  // 0b11001100
  uint64_t index = 0;
  std::generate(data.begin(), data.end(),
                [&index]() -> uint8_t { return (bit_pattern >> ((index++) % 8)) & 1; });

  while (state.KeepRunning()) {
    BooleanBuilder builder;
    for (int i = 0; i < kFinalSize; i++) {
      // Build up an array of 512 MiB in size
      ABORT_NOT_OK(builder.AppendValues(data.data(), data.size()));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * data.size() * kFinalSize);
}

static void BM_BuildBinaryArray(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1 << 20;

  std::string value = "1234567890";
  while (state.KeepRunning()) {
    BinaryBuilder builder;
    for (int64_t i = 0; i < iterations; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  // Assuming a string here needs on average 2 bytes
  state.SetBytesProcessed(state.iterations() * iterations * value.size());
}

static void BM_BuildFixedSizeBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  const int64_t iterations = 1 << 20;
  const int width = 10;

  auto type = fixed_size_binary(width);
  const char value[width + 1] = "1234567890";

  while (state.KeepRunning()) {
    FixedSizeBinaryBuilder builder(type);
    for (int64_t i = 0; i < iterations; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }
    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }
  state.SetBytesProcessed(state.iterations() * iterations * width);
}

BENCHMARK(BM_BuildPrimitiveArrayNoNulls)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildVectorNoNulls)->Repetitions(3)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildBooleanArrayNoNulls)->Repetitions(3)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildAdaptiveIntNoNulls)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildAdaptiveIntNoNullsScalarAppend)
    ->Repetitions(3)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildAdaptiveUIntNoNulls)->Repetitions(3)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BuildBinaryArray)->Repetitions(3)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_BuildFixedSizeBinaryArray)->Repetitions(3)->Unit(benchmark::kMicrosecond);

}  // namespace arrow
