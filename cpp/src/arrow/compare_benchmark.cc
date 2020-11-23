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

#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/array.h"
#include "arrow/compare.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

constexpr auto kSeed = 0x94378165;

static void BenchmarkArrayRangeEquals(const std::shared_ptr<Array>& array,
                                      benchmark::State& state) {
  const auto left_array = array;
  // Make sure pointer equality can't be used as a shortcut
  // (should we would deep-copy child_data and buffers?)
  const auto right_array =
      MakeArray(array->data()->Copy())->Slice(1, array->length() - 1);

  for (auto _ : state) {
    const bool are_ok = ArrayRangeEquals(*left_array, *right_array,
                                         /*left_start_idx=*/1,
                                         /*left_end_idx=*/array->length() - 2,
                                         /*right_start_idx=*/0);
    if (ARROW_PREDICT_FALSE(!are_ok)) {
      ARROW_LOG(FATAL) << "Arrays should have compared equal";
    }
  }
}

static void ArrayRangeEqualsInt32(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto array = rng.Int32(args.size, 0, 100, args.null_proportion);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsFloat32(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto array = rng.Float32(args.size, 0, 100, args.null_proportion);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsBoolean(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto array = rng.Boolean(args.size, /*true_probability=*/0.3, args.null_proportion);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsString(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto array =
      rng.String(args.size, /*min_length=*/0, /*max_length=*/15, args.null_proportion);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsFixedSizeBinary(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto array = rng.FixedSizeBinary(args.size, /*byte_width=*/8, args.null_proportion);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsListOfInt32(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto values = rng.Int32(args.size * 10, 0, 100, args.null_proportion);
  // Force empty list null entries, since it is overwhelmingly the common case.
  auto array = rng.List(*values, /*size=*/args.size, args.null_proportion,
                        /*force_empty_nulls=*/true);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsStruct(benchmark::State& state) {
  // struct<int32, utf8>
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto values1 = rng.Int32(args.size, 0, 100, args.null_proportion);
  auto values2 =
      rng.String(args.size, /*min_length=*/0, /*max_length=*/15, args.null_proportion);
  auto null_bitmap = rng.NullBitmap(args.size, args.null_proportion);
  auto array = *StructArray::Make({values1, values2},
                                  std::vector<std::string>{"ints", "strs"}, null_bitmap);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsSparseUnion(benchmark::State& state) {
  // sparse_union<int32, utf8>
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto values1 = rng.Int32(args.size, 0, 100, args.null_proportion);
  auto values2 =
      rng.String(args.size, /*min_length=*/0, /*max_length=*/15, args.null_proportion);
  auto array = rng.SparseUnion({values1, values2}, args.size);

  BenchmarkArrayRangeEquals(array, state);
}

static void ArrayRangeEqualsDenseUnion(benchmark::State& state) {
  // dense_union<int32, utf8>
  RegressionArgs args(state, /*size_is_bytes=*/false);

  auto rng = random::RandomArrayGenerator(kSeed);
  auto values1 = rng.Int32(args.size, 0, 100, args.null_proportion);
  auto values2 =
      rng.String(args.size, /*min_length=*/0, /*max_length=*/15, args.null_proportion);
  auto array = rng.DenseUnion({values1, values2}, args.size);

  BenchmarkArrayRangeEquals(array, state);
}

BENCHMARK(ArrayRangeEqualsInt32)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsFloat32)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsBoolean)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsString)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsFixedSizeBinary)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsListOfInt32)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsStruct)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsSparseUnion)->Apply(RegressionSetArgs);
BENCHMARK(ArrayRangeEqualsDenseUnion)->Apply(RegressionSetArgs);

}  // namespace arrow
