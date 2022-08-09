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
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/light_array.h"

namespace arrow {
namespace internal {

namespace {
// copied from scalar_string_benchmark
constexpr auto kSeed = 0x94378165;
constexpr double null_prob = 0.2;

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

static Result<std::shared_ptr<StructArray>> MakeStructArray(int64_t n_values,
                                                            int32_t min_strlen,
                                                            int32_t max_strlen) {
  auto vals_first = hashing_rng.Int64(n_values, 0, std::numeric_limits<int64_t>::max());
  auto vals_second = hashing_rng.String(n_values, min_strlen, max_strlen, null_prob);
  auto vals_third = hashing_rng.Int64(n_values, 0, std::numeric_limits<int64_t>::max());

  return arrow::StructArray::Make(
      arrow::ArrayVector{vals_first, vals_second, vals_third},
      arrow::FieldVector{arrow::field("first", arrow::int64()),
                         arrow::field("second", arrow::utf8()),
                         arrow::field("third", arrow::int64())});
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

static void XxHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int64(10000, 0, std::numeric_limits<int64_t>::max());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("xx_hash", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void XxHashSmallStrings(benchmark::State& state) {  // NOLINT non-const reference
  auto str_array = hashing_rng.String(10000, 2, 20, null_prob);
  auto test_vals = std::static_pointer_cast<StringArray>(str_array);

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  int32_t total_string_size = test_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * total_string_size);
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void XxHashMediumStrings(benchmark::State& state) {  // NOLINT non-const reference
  auto str_array = hashing_rng.String(10000, 20, 120, null_prob);
  auto test_vals = std::static_pointer_cast<StringArray>(str_array);

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  int32_t total_string_size = test_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * total_string_size);
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void XxHashLargeStrings(benchmark::State& state) {  // NOLINT non-const reference
  auto str_array = hashing_rng.String(10000, 120, 2000, null_prob);
  auto test_vals = std::static_pointer_cast<StringArray>(str_array);

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  int32_t total_string_size = test_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * total_string_size);
  state.SetItemsProcessed(state.iterations() * test_vals->length());
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

static void FastHash32Int32(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int32(10000, 0, std::numeric_limits<int32_t>::max());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_32", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void FastHash64Int64(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int64(10000, 0, std::numeric_limits<int64_t>::max());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void FastHash64StructSmallStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 2, 20));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  std::shared_ptr<StringArray> str_vals =
      std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void FastHash64StructMediumStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 20, 120));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  std::shared_ptr<StringArray> str_vals =
      std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void FastHash64StructLargeStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 120, 2000));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  std::shared_ptr<StringArray> str_vals =
      std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void FastHash64Map(benchmark::State& state) {  // NOLINT non-const reference
  constexpr int64_t test_size = 10000;
  auto test_keys = hashing_rng.String(test_size, 2, 20, /*null_probability=*/0);
  auto test_vals = hashing_rng.Int64(test_size, 0, std::numeric_limits<int64_t>::max());
  auto test_keyvals = hashing_rng.Map(test_keys, test_vals, test_size);

  auto key_arr = std::static_pointer_cast<StringArray>(test_keys);
  int32_t total_key_size = key_arr->total_values_length();
  int32_t total_val_size = test_size * sizeof(int64_t);

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {test_keyvals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * (total_key_size + total_val_size));
  state.SetItemsProcessed(state.iterations() * 2 * test_size);
}

// ----------------------------------------------------------------------
// Benchmark declarations

// Directly uses "Hashing" hash functions from hashing.h (xxHash)
BENCHMARK(HashSmallStrings);
BENCHMARK(HashMediumStrings);
BENCHMARK(HashLargeStrings);
BENCHMARK(HashIntegers32);
BENCHMARK(HashIntegers64);

// Uses "XxHash" compute function (wraps Hashing functions)
BENCHMARK(XxHashIntegers64);
BENCHMARK(XxHashSmallStrings);
BENCHMARK(XxHashMediumStrings);
BENCHMARK(XxHashLargeStrings);

// Directly uses "KeyHash" hash functions from key_hash.h (xxHash-like)
BENCHMARK(KeyHashIntegers32);
BENCHMARK(KeyHashIntegers64);

// Uses "FastHash" compute functions (wraps KeyHash functions)
BENCHMARK(FastHash32Int32);
BENCHMARK(FastHash64Int64);

BENCHMARK(FastHash64StructSmallStrings);
BENCHMARK(FastHash64StructMediumStrings);
BENCHMARK(FastHash64StructLargeStrings);

BENCHMARK(FastHash64Map);

}  // namespace internal
}  // namespace arrow
