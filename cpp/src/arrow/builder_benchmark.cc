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
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/string_view.h"

namespace arrow {

using ValueType = int64_t;
using VectorType = std::vector<ValueType>;
constexpr int64_t kNumberOfElements = 256 * 512;

static VectorType AlmostU8CompressibleVector() {
  VectorType data(kNumberOfElements, 64);

  // Insert an element late in the game that does not fit in the 8bit
  // representation. This forces AdaptiveIntBuilder's to resize.
  data[kNumberOfElements - 2] = 1L << 13;

  return data;
}

constexpr int64_t kRounds = 256;
static VectorType kData = AlmostU8CompressibleVector();
constexpr int64_t kBytesProcessPerRound = kNumberOfElements * sizeof(ValueType);
constexpr int64_t kBytesProcessed = kRounds * kBytesProcessPerRound;

static const char* kBinaryString = "12345678";
static arrow::util::string_view kBinaryView(kBinaryString);

static void BuildIntArrayNoNulls(benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    Int64Builder builder;

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(kData.data(), kData.size(), nullptr));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildAdaptiveIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    AdaptiveIntBuilder builder;

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(kData.data(), kData.size(), nullptr));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildAdaptiveIntNoNullsScalarAppend(
    benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    AdaptiveIntBuilder builder;

    for (int i = 0; i < kRounds; i++) {
      for (size_t j = 0; j < kData.size(); j++) {
        ABORT_NOT_OK(builder.Append(kData[i]))
      }
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildBooleanArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference

  size_t n_bytes = kBytesProcessPerRound;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(kData.data());

  for (auto _ : state) {
    BooleanBuilder builder;

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(data, n_bytes));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildBinaryArray(benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    BinaryBuilder builder;

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildChunkedBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  // 1MB chunks
  const int32_t kChunkSize = 1 << 20;

  for (auto _ : state) {
    internal::ChunkedBinaryBuilder builder(kChunkSize);

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    ArrayVector out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildFixedSizeBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  auto type = fixed_size_binary(static_cast<int32_t>(kBinaryView.size()));

  for (auto _ : state) {
    FixedSizeBinaryBuilder builder(type);

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildDecimalArray(benchmark::State& state) {  // NOLINT non-const reference
  auto type = decimal(10, 5);
  Decimal128 value;
  int32_t precision = 0;
  int32_t scale = 0;
  ABORT_NOT_OK(Decimal128::FromString("1234.1234", &value, &precision, &scale));
  for (auto _ : state) {
    Decimal128Builder builder(type);

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kRounds * kNumberOfElements * 16);
}

// ----------------------------------------------------------------------
// DictionaryBuilder benchmarks

size_t kDistinctElements = kNumberOfElements / 100;

// Testing with different distributions of integer values helps stress
// the hash table's robustness.

// Make a vector out of `n_distinct` sequential int values
template <class Integer = ValueType>
static std::vector<Integer> MakeSequentialIntDictFodder() {
  std::default_random_engine gen(42);
  std::vector<Integer> values(kNumberOfElements);
  {
    std::uniform_int_distribution<Integer> values_dist(0, kDistinctElements - 1);
    std::generate(values.begin(), values.end(), [&]() { return values_dist(gen); });
  }
  return values;
}

// Make a vector out of `n_distinct` int values with potentially colliding hash
// entries as only their highest bits differ.
template <class Integer = ValueType>
static std::vector<Integer> MakeSimilarIntDictFodder() {
  std::default_random_engine gen(42);
  std::vector<Integer> values(kNumberOfElements);
  {
    std::uniform_int_distribution<Integer> values_dist(0, kDistinctElements - 1);
    auto max_int = std::numeric_limits<Integer>::max();
    auto multiplier =
        static_cast<Integer>(BitUtil::NextPower2(max_int / kDistinctElements / 2));
    std::generate(values.begin(), values.end(),
                  [&]() { return multiplier * values_dist(gen); });
  }
  return values;
}

// Make a vector out of `n_distinct` random int values
template <class Integer = ValueType>
static std::vector<Integer> MakeRandomIntDictFodder() {
  std::default_random_engine gen(42);
  std::vector<Integer> values_dict(kDistinctElements);
  std::vector<Integer> values(kNumberOfElements);

  {
    std::uniform_int_distribution<Integer> values_dist(
        0, std::numeric_limits<Integer>::max());
    std::generate(values_dict.begin(), values_dict.end(),
                  [&]() { return static_cast<Integer>(values_dist(gen)); });
  }
  {
    std::uniform_int_distribution<int32_t> indices_dist(
        0, static_cast<int32_t>(kDistinctElements - 1));
    std::generate(values.begin(), values.end(),
                  [&]() { return values_dict[indices_dist(gen)]; });
  }
  return values;
}

// Make a vector out of `kDistinctElements` string values
static std::vector<std::string> MakeStringDictFodder() {
  std::default_random_engine gen(42);
  std::vector<std::string> values_dict(kDistinctElements);
  std::vector<std::string> values(kNumberOfElements);

  {
    auto it = values_dict.begin();
    // Add empty string
    *it++ = "";
    // Add a few similar strings
    *it++ = "abc";
    *it++ = "abcdef";
    *it++ = "abcfgh";
    // Add random strings
    std::uniform_int_distribution<int32_t> length_dist(2, 20);
    std::independent_bits_engine<std::default_random_engine, 8, uint16_t> bytes_gen(42);

    std::generate(it, values_dict.end(), [&] {
      auto length = length_dist(gen);
      std::string s(length, 'X');
      for (int32_t i = 0; i < length; ++i) {
        s[i] = static_cast<char>(bytes_gen());
      }
      return s;
    });
  }
  {
    std::uniform_int_distribution<int32_t> indices_dist(
        0, static_cast<int32_t>(kDistinctElements - 1));
    std::generate(values.begin(), values.end(),
                  [&] { return values_dict[indices_dist(gen)]; });
  }
  return values;
}

template <class DictionaryBuilderType, class Scalar>
static void BenchmarkScalarDictionaryArray(
    benchmark::State& state,  // NOLINT non-const reference
    const std::vector<Scalar>& fodder) {
  for (auto _ : state) {
    DictionaryBuilder<Int64Type> builder(default_memory_pool());

    for (int64_t i = 0; i < kRounds; i++) {
      for (const auto value : fodder) {
        ABORT_NOT_OK(builder.Append(value));
      }
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

static void BuildInt64DictionaryArrayRandom(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeRandomIntDictFodder();
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildInt64DictionaryArraySequential(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSequentialIntDictFodder();
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildInt64DictionaryArraySimilar(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSimilarIntDictFodder();
  BenchmarkScalarDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildStringDictionaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeStringDictFodder();
  auto fodder_size =
      std::accumulate(fodder.begin(), fodder.end(), 0ULL,
                      [&](size_t acc, const std::string& s) { return acc + s.size(); });

  for (auto _ : state) {
    BinaryDictionaryBuilder builder(default_memory_pool());

    for (int64_t i = 0; i < kRounds; i++) {
      for (const auto& value : fodder) {
        ABORT_NOT_OK(builder.Append(value));
      }
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * fodder_size * kRounds);
}

static void ArrayDataConstructDestruct(
    benchmark::State& state) {  // NOLINT non-const reference
  std::vector<std::shared_ptr<ArrayData>> arrays;

  const int kNumArrays = 1000;
  auto InitArrays = [&]() {
    for (int i = 0; i < kNumArrays; ++i) {
      arrays.emplace_back(new ArrayData);
    }
  };

  for (auto _ : state) {
    InitArrays();
    arrays.clear();
  }
}

// ----------------------------------------------------------------------
// Benchmark declarations
//

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

// This benchmarks acts as a reference to the native std::vector
// implementation. It appends kRounds chunks into a vector.
static void ReferenceBuildVectorNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    std::vector<int64_t> builder;

    for (int i = 0; i < kRounds; i++) {
      builder.insert(builder.end(), kData.cbegin(), kData.cend());
    }
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
}

BENCHMARK(ReferenceBuildVectorNoNulls);

#endif

BENCHMARK(BuildBooleanArrayNoNulls);

BENCHMARK(BuildIntArrayNoNulls);
BENCHMARK(BuildAdaptiveIntNoNulls);
BENCHMARK(BuildAdaptiveIntNoNullsScalarAppend);

BENCHMARK(BuildBinaryArray);
BENCHMARK(BuildChunkedBinaryArray);
BENCHMARK(BuildFixedSizeBinaryArray);
BENCHMARK(BuildDecimalArray);

BENCHMARK(BuildInt64DictionaryArrayRandom);
BENCHMARK(BuildInt64DictionaryArraySequential);
BENCHMARK(BuildInt64DictionaryArraySimilar);
BENCHMARK(BuildStringDictionaryArray);

BENCHMARK(ArrayDataConstructDestruct);

}  // namespace arrow
