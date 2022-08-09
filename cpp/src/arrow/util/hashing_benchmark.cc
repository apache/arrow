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
#include "arrow/util/hashing.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/compute/light_array.h"

namespace arrow {
namespace internal {

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

template <BuilderType, ValueType>
static Result<std::shared_ptr<Array>> MakeArray(std::vector<ValueType> vals) {
  BuilderType arr_builder;
  ARROW_RETURN_NOT_OK(arr_builder.Reserve(vals.size()));
  ARROW_RETURN_NOT_OK(arr_builder.AppendValues(vals));

  return arr_builder.Finish();
}

static Result<std::shared_ptr<Array>> MakeStructArray(
    std::vector<int64_t> vals_first, std::vector<std::string> vals_second,
    std::vector<int64_t> vals_third) {
  std::shared_ptr<Array> valarray_first, valarray_second, valarray_third;

  // vals_first
  ARROW_ASSIGN_OR_RAISE(valarray_first,
                        MakeArray<arrow::Int64Builder, int64_t>(vals_first));

  // vals_second
  ARROW_ASSIGN_OR_RAISE(valarray_second,
                        MakeArray<arrow::StringBuilder, std::string>(vals_second));

  // vals_third
  ARROW_ASSIGN_OR_RAISE(valarray_third,
                        MakeArray<arrow::Int64Builder, int64_t>(vals_third));

  return arrow::StructArray::Make(
      arrow::ArrayVector{valarray_first, valarray_second, valarray_third},
      arrow::FieldVector{arrow::field("first", arrow::int64()),
                         arrow::field("second", arrow::utf8()),
                         arrow::field("third", arrow::int64())});
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

static void FastHashIntegers32(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int32_t> values = MakeIntegers<int32_t>(10000);

  arrow::Int32Builder builder;
  ASSERT_OK(builder.Reserve(values.size()));
  ASSERT_OK(builder.AppendValues(values));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array, builder.Finish());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_32", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void FastHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int64_t> values = MakeIntegers<int64_t>(10000);

  arrow::Int64Builder builder;
  ASSERT_OK(builder.Reserve(values.size()));
  ASSERT_OK(builder.AppendValues(values));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array, builder.Finish());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void FastHashStruct64(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int64_t> vals_first = MakeIntegers<int64_t>(10000);
  const std::vector<std::string> vals_second = MakeStrings(10000, 20, 120);
  const std::vector<int64_t> vals_third = MakeIntegers<int64_t>(10000);

  uint64_t vals_second_total_size = 0;
  for (const std::string& v : vals_second) {
    vals_second_total_size += v.size();
  }

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array,
                       MakeStructArray(vals_first, vals_second, vals_third));

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("fast_hash_64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * ((vals_first.size() * sizeof(int64_t)) +
                                                (vals_second_total_size) +
                                                (vals_third.size() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * vals_first.size());
}

static void KeyHashIntegers32(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int32_t> values = MakeIntegers<int32_t>(10000);

  arrow::Int32Builder builder;
  util::TempVectorStack stack_memallocator;
  compute::LightContext hash_ctx;

  // build the array
  ASSERT_OK(builder.Reserve(values.size()));
  ASSERT_OK(builder.AppendValues(values));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array, builder.Finish());

  // initialize the stack allocator
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(values.size() * sizeof(uint32_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(values_array->data(), 0, values.size()));

    hash_ctx.hardware_flags =
        compute::default_exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    compute::Hashing32::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint32_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void KeyHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int64_t> values = MakeIntegers<int64_t>(10000);

  arrow::Int64Builder builder;
  util::TempVectorStack stack_memallocator;
  compute::LightContext hash_ctx;

  // build the array
  ASSERT_OK(builder.Reserve(values.size()));
  ASSERT_OK(builder.AppendValues(values));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array, builder.Finish());

  // initialize the stack allocator
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(values.size() * sizeof(uint64_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(values_array->data(), 0, values.size()));

    hash_ctx.hardware_flags =
        compute::default_exec_context()->cpu_info()->hardware_flags();
    hash_ctx.stack = &stack_memallocator;

    compute::Hashing64::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint64_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void XxHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  const std::vector<int64_t> values = MakeIntegers<int64_t>(10000);

  arrow::Int64Builder builder;
  ASSERT_OK(builder.Reserve(values.size()));
  ASSERT_OK(builder.AppendValues(values));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_array, builder.Finish());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("xx_hash", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * values.size() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * values.size());
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

// Directly uses "KeyHash" hash functions from key_hash.h (xxHash-like)
BENCHMARK(KeyHashIntegers64);

// Uses "FastHash" compute functions (wraps KeyHash functions)
BENCHMARK(FastHashIntegers32);
BENCHMARK(FastHashIntegers64);
BENCHMARK(FastHashStruct64);

}  // namespace internal
}  // namespace arrow
