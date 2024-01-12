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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/hashing.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/key_hash.h"

namespace arrow {
namespace internal {

namespace {
// copied from scalar_string_benchmark
constexpr auto kSeed = 0x94378165;

static random::RandomArrayGenerator hashing_rng(kSeed);
}  // namespace

template <class Integer>
static std::vector<Integer> MakeIntegers(int32_t n_values) {
  std::vector<Integer> values(n_values);

  std::default_random_engine gen(42);
  std::uniform_int_distribution<Integer> values_dist(0,
                                                     std::numeric_limits<Integer>::max());
  std::generate(values.begin(), values.end(),
                [&]() { return static_cast<Integer>(values_dist(gen)); });
  return values;
}

static std::vector<std::string> MakeStrings(int32_t n_values, int32_t min_length,
                                            int32_t max_length) {
  std::default_random_engine gen(42);
  std::vector<std::string> values(n_values);

  // Generate strings between 2 and 20 bytes
  std::uniform_int_distribution<int32_t> length_dist(min_length, max_length);
  std::independent_bits_engine<std::default_random_engine, 8, uint16_t> bytes_gen(42);

  std::generate(values.begin(), values.end(), [&]() {
    auto length = length_dist(gen);
    std::string s(length, 'X');
    for (int32_t i = 0; i < length; ++i) {
      s[i] = static_cast<uint8_t>(bytes_gen());
    }
    return s;
  });
  return values;
}

static void HashIntegers32(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int32_t> values = MakeIntegers<int32_t>(10000);

  while (state.KeepRunning()) {
    hash_t total = 0;
    for (const int32_t v : values) {
      total += ScalarHelper<int32_t, 0>::ComputeHash(v);
      total += ScalarHelper<int32_t, 1>::ComputeHash(v);
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetBytesProcessed(2 * state.iterations() * values.size() * sizeof(int32_t));
  state.SetItemsProcessed(2 * state.iterations() * values.size());
}

static void HashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int64_t> values = MakeIntegers<int64_t>(10000);

  while (state.KeepRunning()) {
    hash_t total = 0;
    for (const int64_t v : values) {
      total += ScalarHelper<int64_t, 0>::ComputeHash(v);
      total += ScalarHelper<int64_t, 1>::ComputeHash(v);
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetBytesProcessed(2 * state.iterations() * values.size() * sizeof(int64_t));
  state.SetItemsProcessed(2 * state.iterations() * values.size());
}

static void BenchmarkStringHashing(benchmark::State& state,  // NOLINT non-const reference
                                   const std::vector<std::string>& values) {
  uint64_t total_size = 0;
  for (const std::string& v : values) {
    total_size += v.size();
  }

  while (state.KeepRunning()) {
    hash_t total = 0;
    for (const std::string& v : values) {
      total += ComputeStringHash<0>(v.data(), static_cast<int64_t>(v.size()));
      total += ComputeStringHash<1>(v.data(), static_cast<int64_t>(v.size()));
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetBytesProcessed(2 * state.iterations() * total_size);
  state.SetItemsProcessed(2 * state.iterations() * values.size());
}

static void HashSmallStrings(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<std::string> values = MakeStrings(10000, 2, 20);
  BenchmarkStringHashing(state, values);
}

static void HashMediumStrings(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<std::string> values = MakeStrings(10000, 20, 120);
  BenchmarkStringHashing(state, values);
}

static void HashLargeStrings(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<std::string> values = MakeStrings(1000, 120, 2000);
  BenchmarkStringHashing(state, values);
}

static void KeyHashIntegers32(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int32(10000, 0, std::numeric_limits<int32_t>::max());

  // initialize the stack allocator
  util::TempVectorStack stack_memallocator;
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // prepare the execution context for Hashing32
  compute::LightContext hash_ctx;
  hash_ctx.hardware_flags = compute::default_exec_context()->cpu_info()->hardware_flags();
  hash_ctx.stack = &stack_memallocator;

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(test_vals->length() * sizeof(int32_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(test_vals->data(), 0, test_vals->length()));

    compute::Hashing32::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint32_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void KeyHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int64(10000, 0, std::numeric_limits<int64_t>::max());

  // initialize the stack allocator
  util::TempVectorStack stack_memallocator;
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // prepare the execution context for Hashing32
  compute::LightContext hash_ctx;
  hash_ctx.hardware_flags = compute::default_exec_context()->cpu_info()->hardware_flags();
  hash_ctx.stack = &stack_memallocator;

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(test_vals->length() * sizeof(int64_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(test_vals->data(), 0, test_vals->length()));

    compute::Hashing64::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint64_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

// ----------------------------------------------------------------------
// Benchmark declarations

// Directly uses "Hashing" hash functions from hashing.h (xxHash)
BENCHMARK(HashIntegers32);
BENCHMARK(HashIntegers64);
BENCHMARK(HashSmallStrings);
BENCHMARK(HashMediumStrings);
BENCHMARK(HashLargeStrings);

// Directly uses "KeyHash" hash functions from key_hash.h (xxHash-like)
BENCHMARK(KeyHashIntegers32);
BENCHMARK(KeyHashIntegers64);

}  // namespace internal
}  // namespace arrow
