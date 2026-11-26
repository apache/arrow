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
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/builder_binary.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util_internal.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;

template <typename Type>
static void BenchArrayScalar(benchmark::State& state, const std::string& op) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto array = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto scalar = *rand.ArrayOf(ty, 1, 0)->GetScalar(0);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(op, {array, Datum(scalar)}));
  }
}

template <typename Type>
static void BenchArrayArray(benchmark::State& state, const std::string& op) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto ty = TypeTraits<Type>::type_singleton();
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  auto rhs = rand.ArrayOf(ty, args.size, args.null_proportion);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(op, {lhs, rhs}));
  }
}

static void GreaterArrayArrayInt64(benchmark::State& state) {
  BenchArrayArray<Int64Type>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayScalarInt64(benchmark::State& state) {
  BenchArrayScalar<Int64Type>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayArrayString(benchmark::State& state) {
  BenchArrayArray<StringType>(state, CompareOperatorToFunctionName(GREATER));
}

static void GreaterArrayScalarString(benchmark::State& state) {
  BenchArrayScalar<StringType>(state, CompareOperatorToFunctionName(GREATER));
}

static void MaxElementWiseArrayArrayInt64(benchmark::State& state) {
  BenchArrayArray<Int64Type>(state, "max_element_wise");
}

static void MaxElementWiseArrayScalarInt64(benchmark::State& state) {
  BenchArrayScalar<Int64Type>(state, "max_element_wise");
}

static void MaxElementWiseArrayArrayString(benchmark::State& state) {
  BenchArrayArray<StringType>(state, "max_element_wise");
}

static void MaxElementWiseArrayScalarString(benchmark::State& state) {
  BenchArrayScalar<StringType>(state, "max_element_wise");
}

static std::shared_ptr<Array> MakeSharedPrefixStringViewArray(int64_t size,
                                                              double null_proportion,
                                                              uint64_t seed_offset) {
  StringViewBuilder builder;
  ABORT_NOT_OK(builder.Reserve(size));
  const auto null_threshold = static_cast<uint64_t>(null_proportion * 10000);
  for (int64_t i = 0; i < size; ++i) {
    if (null_proportion > 0 && ((static_cast<uint64_t>(i) * 2654435761u + seed_offset) %
                                10000) < null_threshold) {
      ABORT_NOT_OK(builder.AppendNull());
    } else {
      ABORT_NOT_OK(builder.Append("shared_prefix_0123456789_" +
                                  std::to_string((i + seed_offset) & 0x3f)));
    }
  }
  std::shared_ptr<Array> out;
  ABORT_NOT_OK(builder.Finish(&out));
  return out;
}

static void BenchStringViewEqualArrayArray(benchmark::State& state, int32_t min_length,
                                           int32_t max_length,
                                           bool rhs_is_string = false) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.StringView(args.size, min_length, max_length, args.null_proportion);
  auto rhs =
      rhs_is_string
          ? rand.String(args.size, min_length, max_length, args.null_proportion)
          : rand.StringView(args.size, min_length, max_length, args.null_proportion);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("equal", {lhs, rhs}));
  }
}

template <typename ScalarType>
static void BenchStringViewEqualArrayScalar(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = rand.StringView(args.size, /*min_length=*/24, /*max_length=*/64,
                             args.null_proportion);
  auto scalar = std::make_shared<ScalarType>("scalar_out_of_line_payload_value");
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("equal", {lhs, Datum(scalar)}));
  }
}

static void EqualArrayArrayStringView(benchmark::State& state) {
  BenchArrayArray<StringViewType>(state, CompareOperatorToFunctionName(EQUAL));
}

static void EqualArrayScalarStringView(benchmark::State& state) {
  BenchArrayScalar<StringViewType>(state, CompareOperatorToFunctionName(EQUAL));
}

static void EqualArrayArrayStringViewInline(benchmark::State& state) {
  BenchStringViewEqualArrayArray(state, /*min_length=*/4, /*max_length=*/12);
}

static void EqualArrayArrayStringViewOutOfLine(benchmark::State& state) {
  BenchStringViewEqualArrayArray(state, /*min_length=*/24, /*max_length=*/64);
}

static void EqualArrayScalarStringViewOutOfLine(benchmark::State& state) {
  BenchStringViewEqualArrayScalar<StringViewScalar>(state);
}

static void EqualArrayArrayStringViewSharedPrefix(benchmark::State& state) {
  RegressionArgs args(state, /*size_is_bytes=*/false);
  auto lhs = MakeSharedPrefixStringViewArray(args.size, args.null_proportion, 0);
  auto rhs = MakeSharedPrefixStringViewArray(args.size, args.null_proportion, 17);
  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("equal", {lhs, rhs}));
  }
}

static void EqualArrayArrayStringViewString(benchmark::State& state) {
  BenchStringViewEqualArrayArray(state, /*min_length=*/24, /*max_length=*/64,
                                 /*rhs_is_string=*/true);
}

static void EqualArrayScalarStringViewString(benchmark::State& state) {
  BenchStringViewEqualArrayScalar<StringScalar>(state);
}

BENCHMARK(GreaterArrayArrayInt64)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarInt64)->Apply(RegressionSetArgs);

BENCHMARK(GreaterArrayArrayString)->Apply(RegressionSetArgs);
BENCHMARK(GreaterArrayScalarString)->Apply(RegressionSetArgs);

BENCHMARK(MaxElementWiseArrayArrayInt64)->Apply(RegressionSetArgs);
BENCHMARK(MaxElementWiseArrayScalarInt64)->Apply(RegressionSetArgs);

BENCHMARK(MaxElementWiseArrayArrayString)->Apply(RegressionSetArgs);
BENCHMARK(MaxElementWiseArrayScalarString)->Apply(RegressionSetArgs);

BENCHMARK(EqualArrayArrayStringView)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayScalarStringView)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayArrayStringViewInline)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayArrayStringViewOutOfLine)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayScalarStringViewOutOfLine)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayArrayStringViewSharedPrefix)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayArrayStringViewString)->Apply(RegressionSetArgs);
BENCHMARK(EqualArrayScalarStringViewString)->Apply(RegressionSetArgs);

}  // namespace compute
}  // namespace arrow
