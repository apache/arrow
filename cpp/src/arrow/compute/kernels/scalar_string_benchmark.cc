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

#include <functional>

#include "benchmark/benchmark.h"

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

constexpr auto kSeed = 0x94378165;

static void UnaryStringBenchmark(benchmark::State& state, const std::string& func_name,
                                 const FunctionOptions* options = nullptr) {
  const int64_t array_length = 1 << 20;
  const int64_t value_min_size = 0;
  const int64_t value_max_size = 32;
  const double null_probability = 0.01;
  random::RandomArrayGenerator rng(kSeed);

  // NOTE: this produces only-Ascii data
  auto values =
      rng.String(array_length, value_min_size, value_max_size, null_probability);
  // Make sure lookup tables are initialized before measuring
  ABORT_NOT_OK(CallFunction(func_name, {values}, options));

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction(func_name, {values}, options));
  }
  state.SetItemsProcessed(state.iterations() * array_length);
  state.SetBytesProcessed(state.iterations() * values->data()->buffers[2]->size());
}

static void AsciiLower(benchmark::State& state) {
  UnaryStringBenchmark(state, "ascii_lower");
}

static void AsciiUpper(benchmark::State& state) {
  UnaryStringBenchmark(state, "ascii_upper");
}

static void IsAlphaNumericAscii(benchmark::State& state) {
  UnaryStringBenchmark(state, "ascii_is_alnum");
}

static void MatchSubstring(benchmark::State& state) {
  MatchSubstringOptions options("abac");
  UnaryStringBenchmark(state, "match_substring", &options);
}

static void SplitPattern(benchmark::State& state) {
  SplitPatternOptions options("a");
  UnaryStringBenchmark(state, "split_pattern", &options);
}

static void TrimSingleAscii(benchmark::State& state) {
  TrimOptions options("a");
  UnaryStringBenchmark(state, "ascii_trim", &options);
}

static void TrimManyAscii(benchmark::State& state) {
  TrimOptions options("abcdefgABCDEFG");
  UnaryStringBenchmark(state, "ascii_trim", &options);
}

#ifdef ARROW_WITH_RE2
static void MatchLike(benchmark::State& state) {
  MatchSubstringOptions options("ab%ac");
  UnaryStringBenchmark(state, "match_like", &options);
}

// MatchLike optimizes the following three into a substring/prefix/suffix search instead
// of using RE2
static void MatchLikeSubstring(benchmark::State& state) {
  MatchSubstringOptions options("%abac%");
  UnaryStringBenchmark(state, "match_like", &options);
}

static void MatchLikePrefix(benchmark::State& state) {
  MatchSubstringOptions options("%abac");
  UnaryStringBenchmark(state, "match_like", &options);
}

static void MatchLikeSuffix(benchmark::State& state) {
  MatchSubstringOptions options("%abac");
  UnaryStringBenchmark(state, "match_like", &options);
}
#endif

#ifdef ARROW_WITH_UTF8PROC
static void Utf8Upper(benchmark::State& state) {
  UnaryStringBenchmark(state, "utf8_upper");
}

static void Utf8Lower(benchmark::State& state) {
  UnaryStringBenchmark(state, "utf8_lower");
}

static void IsAlphaNumericUnicode(benchmark::State& state) {
  UnaryStringBenchmark(state, "utf8_is_alnum");
}
static void TrimSingleUtf8(benchmark::State& state) {
  TrimOptions options("a");
  UnaryStringBenchmark(state, "utf8_trim", &options);
}

static void TrimManyUtf8(benchmark::State& state) {
  TrimOptions options("abcdefgABCDEFG");
  UnaryStringBenchmark(state, "utf8_trim", &options);
}
#endif

using SeparatorFactory = std::function<Datum(int64_t n, double null_probability)>;

static void BinaryJoin(benchmark::State& state, SeparatorFactory make_separator) {
  const int64_t n_strings = 10000;
  const int64_t n_lists = 1000;
  const double null_probability = 0.02;

  random::RandomArrayGenerator rng(kSeed);

  auto strings =
      rng.String(n_strings, /*min_length=*/5, /*max_length=*/20, null_probability);
  auto lists = rng.List(*strings, n_lists, null_probability, /*force_empty_nulls=*/true);
  auto separator = make_separator(n_lists, null_probability);

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("binary_join", {lists, separator}));
  }
  state.SetBytesProcessed(
      state.iterations() *
      checked_cast<const StringArray&>(*strings).total_values_length());
}

static void BinaryJoinArrayScalar(benchmark::State& state) {
  BinaryJoin(state, [](int64_t n, double null_probability) -> Datum {
    return ScalarFromJSON(utf8(), R"("--")");
  });
}

static void BinaryJoinArrayArray(benchmark::State& state) {
  BinaryJoin(state, [](int64_t n, double null_probability) -> Datum {
    random::RandomArrayGenerator rng(kSeed + 1);
    return rng.String(n, /*min_length=*/0, /*max_length=*/4, null_probability);
  });
}

static void BinaryJoinElementWise(benchmark::State& state,
                                  SeparatorFactory make_separator) {
  // Unfortunately benchmark is not 1:1 with BinaryJoin since BinaryJoin can join a
  // varying number of inputs per output
  const int64_t n_rows = 10000;
  const int64_t n_cols = state.range(0);
  const double null_probability = 0.02;

  random::RandomArrayGenerator rng(kSeed);

  DatumVector args;
  ArrayVector strings;
  int64_t total_values_length = 0;
  for (int i = 0; i < n_cols; i++) {
    auto arr = rng.String(n_rows, /*min_length=*/5, /*max_length=*/20, null_probability);
    strings.push_back(arr);
    args.emplace_back(arr);
    total_values_length += checked_cast<const StringArray&>(*arr).total_values_length();
  }
  auto separator = make_separator(n_rows, null_probability);
  args.emplace_back(separator);

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("binary_join_element_wise", args));
  }
  state.SetBytesProcessed(state.iterations() * total_values_length);
}

static void BinaryJoinElementWiseArrayScalar(benchmark::State& state) {
  BinaryJoinElementWise(state, [](int64_t n, double null_probability) -> Datum {
    return ScalarFromJSON(utf8(), R"("--")");
  });
}

static void BinaryJoinElementWiseArrayArray(benchmark::State& state) {
  BinaryJoinElementWise(state, [](int64_t n, double null_probability) -> Datum {
    random::RandomArrayGenerator rng(kSeed + 1);
    return rng.String(n, /*min_length=*/0, /*max_length=*/4, null_probability);
  });
}

static void BinaryRepeat(benchmark::State& state) {
  const int64_t array_length = 1 << 20;
  const int64_t value_min_size = 0;
  const int64_t value_max_size = 32;
  const double null_probability = 0.01;
  const int64_t repeat_min_size = 0;
  const int64_t repeat_max_size = 8;
  random::RandomArrayGenerator rng(kSeed);

  // NOTE: this produces only-Ascii data
  auto values =
      rng.String(array_length, value_min_size, value_max_size, null_probability);
  auto num_repeats = rng.Int64(array_length, repeat_min_size, repeat_max_size, 0);
  // Make sure lookup tables are initialized before measuring
  ABORT_NOT_OK(CallFunction("binary_repeat", {values, num_repeats}));

  for (auto _ : state) {
    ABORT_NOT_OK(CallFunction("binary_repeat", {values, num_repeats}));
  }
  state.SetItemsProcessed(state.iterations() * array_length);
  state.SetBytesProcessed(state.iterations() * values->data()->buffers[2]->size());
}

BENCHMARK(AsciiLower);
BENCHMARK(AsciiUpper);
BENCHMARK(IsAlphaNumericAscii);
BENCHMARK(MatchSubstring);
BENCHMARK(SplitPattern);
BENCHMARK(TrimSingleAscii);
BENCHMARK(TrimManyAscii);
#ifdef ARROW_WITH_RE2
BENCHMARK(MatchLike);
BENCHMARK(MatchLikeSubstring);
BENCHMARK(MatchLikePrefix);
BENCHMARK(MatchLikeSuffix);
#endif
#ifdef ARROW_WITH_UTF8PROC
BENCHMARK(Utf8Lower);
BENCHMARK(Utf8Upper);
BENCHMARK(IsAlphaNumericUnicode);
BENCHMARK(TrimSingleUtf8);
BENCHMARK(TrimManyUtf8);
#endif

BENCHMARK(BinaryJoinArrayScalar);
BENCHMARK(BinaryJoinArrayArray);
BENCHMARK(BinaryJoinElementWiseArrayScalar)->RangeMultiplier(8)->Range(2, 128);
BENCHMARK(BinaryJoinElementWiseArrayArray)->RangeMultiplier(8)->Range(2, 128);

BENCHMARK(BinaryRepeat);

}  // namespace compute
}  // namespace arrow
