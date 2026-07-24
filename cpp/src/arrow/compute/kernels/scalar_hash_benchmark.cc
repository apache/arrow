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

#include "arrow/array/array_nested.h"
#include "arrow/compute/exec.h"

namespace arrow {
namespace internal {

// ------------------------------
// Anonymous namespace with global params

namespace {
// copied from scalar_string_benchmark
constexpr auto kSeed = 0x94378165;
constexpr double null_prob = 0.2;

static random::RandomArrayGenerator hashing_rng(kSeed);
}  // namespace

// ------------------------------
// Convenience functions

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

// ------------------------------
// Benchmark implementations

static void Hash64Int64(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int64(10000, 0, std::numeric_limits<int64_t>::max());

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, compute::CallFunction("hash64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void Hash64StructSmallStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 2, 20));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  auto str_vals = std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("hash64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void Hash64StructMediumStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 20, 120));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  auto str_vals = std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("hash64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void Hash64StructLargeStrings(
    benchmark::State& state) {  // NOLINT non-const reference
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<StructArray> values_array,
                       MakeStructArray(10000, 120, 2000));

  // 2nd column (index 1) is a string column, which has offset type of int32_t
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> values_second,
                       values_array->GetFlattenedField(1));
  auto str_vals = std::static_pointer_cast<StringArray>(values_second);
  int32_t total_string_size = str_vals->total_values_length();

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("hash64", {values_array}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() *
                          ((values_array->length() * sizeof(int64_t)) +
                           (total_string_size) +
                           (values_array->length() * sizeof(int64_t))));
  state.SetItemsProcessed(state.iterations() * 3 * values_array->length());
}

static void Hash64ListInt64(benchmark::State& state) {  // NOLINT non-const reference
  constexpr int64_t test_size = 10000;
  auto test_vals = hashing_rng.ArrayOf(list(int64()), test_size, null_prob);

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, compute::CallFunction("hash64", {test_vals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetItemsProcessed(state.iterations() * test_size);
}

// Hashing a small slice of a much larger list array; child_data isn't sliced by
// ArrayData::Slice(), so this used to cost proportionally to the whole array.
static void Hash64ListInt64HeavilySliced(
    benchmark::State& state) {  // NOLINT non-const reference
  constexpr int64_t total_size = 1000000;
  constexpr int64_t slice_size = 100;
  auto test_vals = hashing_rng.ArrayOf(list(int64()), total_size, null_prob);
  auto sliced = test_vals->Slice(total_size / 2, slice_size);

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, compute::CallFunction("hash64", {sliced}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetItemsProcessed(state.iterations() * slice_size);
}

// Unlike lists (see Hash64ListInt64HeavilySliced), binary-like arrays scale with the
// slice, not the underlying array: KeyColumnArray::Slice() narrows the offsets buffer
// itself, so there's no unsliced "child" to worry about.
static void Hash64StringHeavilySliced(
    benchmark::State& state) {  // NOLINT non-const reference
  constexpr int64_t total_size = 1000000;
  constexpr int64_t slice_size = 100;
  auto test_vals = hashing_rng.String(total_size, 2, 20, null_prob);
  auto sliced = test_vals->Slice(total_size / 2, slice_size);

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result, compute::CallFunction("hash64", {sliced}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetItemsProcessed(state.iterations() * slice_size);
}

static void Hash64Map(benchmark::State& state) {  // NOLINT non-const reference
  constexpr int64_t test_size = 10000;
  auto test_keys = hashing_rng.String(test_size, 2, 20, /*null_probability=*/0);
  auto test_vals = hashing_rng.Int64(test_size, 0, std::numeric_limits<int64_t>::max());
  auto test_keyvals = hashing_rng.Map(test_keys, test_vals, test_size);

  auto key_arr = std::static_pointer_cast<StringArray>(test_keys);
  int32_t total_key_size = key_arr->total_values_length();
  int32_t total_val_size = test_size * sizeof(int64_t);

  while (state.KeepRunning()) {
    ASSERT_OK_AND_ASSIGN(Datum hash_result,
                         compute::CallFunction("hash64", {test_keyvals}));
    benchmark::DoNotOptimize(hash_result);
  }

  state.SetBytesProcessed(state.iterations() * (total_key_size + total_val_size));
  state.SetItemsProcessed(state.iterations() * 2 * test_size);
}

// ------------------------------
// Benchmark declarations

// Uses "FastHash" compute functions (wraps KeyHash functions)
BENCHMARK(Hash64Int64);

BENCHMARK(Hash64StructSmallStrings);
BENCHMARK(Hash64StructMediumStrings);
BENCHMARK(Hash64StructLargeStrings);

BENCHMARK(Hash64Map);
BENCHMARK(Hash64ListInt64);
BENCHMARK(Hash64ListInt64HeavilySliced);
BENCHMARK(Hash64StringHeavilySliced);

}  // namespace internal
}  // namespace arrow
