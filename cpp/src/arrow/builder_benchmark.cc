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
#include <string_view>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/decimal.h"

namespace arrow {

::arrow::BenchmarkMemoryTracker memory_tracker;

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
constexpr int64_t kItemsProcessed = kRounds * kNumberOfElements;

static const char* kBinaryString = "12345678";
static std::string_view kBinaryView(kBinaryString);

static void BuildIntArrayNoNulls(benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    Int64Builder builder(memory_tracker.memory_pool());

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(kData.data(), kData.size(), nullptr));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildAdaptiveIntNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    AdaptiveIntBuilder builder(memory_tracker.memory_pool());

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(kData.data(), kData.size(), nullptr));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildAdaptiveIntNoNullsScalarAppend(
    benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    AdaptiveIntBuilder builder(memory_tracker.memory_pool());

    for (int i = 0; i < kRounds; i++) {
      for (size_t j = 0; j < kData.size(); j++) {
        ABORT_NOT_OK(builder.Append(kData[i]))
      }
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildBooleanArrayNoNulls(
    benchmark::State& state) {  // NOLINT non-const reference

  size_t n_bytes = kBytesProcessPerRound;
  const uint8_t* data = reinterpret_cast<const uint8_t*>(kData.data());

  for (auto _ : state) {
    BooleanBuilder builder(memory_tracker.memory_pool());

    for (int i = 0; i < kRounds; i++) {
      ABORT_NOT_OK(builder.AppendValues(data, n_bytes));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildBinaryArray(benchmark::State& state) {  // NOLINT non-const reference
  for (auto _ : state) {
    BinaryBuilder builder(memory_tracker.memory_pool());

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildChunkedBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  // 1MB chunks
  const int32_t kChunkSize = 1 << 20;

  for (auto _ : state) {
    internal::ChunkedBinaryBuilder builder(kChunkSize, memory_tracker.memory_pool());

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    ArrayVector out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildFixedSizeBinaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  auto type = fixed_size_binary(static_cast<int32_t>(kBinaryView.size()));

  for (auto _ : state) {
    FixedSizeBinaryBuilder builder(type, memory_tracker.memory_pool());

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(kBinaryView));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kBytesProcessed);
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
}

static void BuildDecimalArray(benchmark::State& state) {  // NOLINT non-const reference
  auto type = decimal(10, 5);
  Decimal128 value;
  int32_t precision = 0;
  int32_t scale = 0;
  ABORT_NOT_OK(Decimal128::FromString("1234.1234", &value, &precision, &scale));
  for (auto _ : state) {
    Decimal128Builder builder(type, memory_tracker.memory_pool());

    for (int64_t i = 0; i < kRounds * kNumberOfElements; i++) {
      ABORT_NOT_OK(builder.Append(value));
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  state.SetBytesProcessed(state.iterations() * kRounds * kNumberOfElements * 16);
  state.SetItemsProcessed(state.iterations() * kRounds * kNumberOfElements);
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
        static_cast<Integer>(bit_util::NextPower2(max_int / kDistinctElements / 2));
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
static void BenchmarkDictionaryArray(
    benchmark::State& state,  // NOLINT non-const reference
    const std::vector<Scalar>& fodder, size_t fodder_nbytes = 0) {
  for (auto _ : state) {
    DictionaryBuilderType builder(memory_tracker.memory_pool());

    for (int64_t i = 0; i < kRounds; i++) {
      for (const auto& value : fodder) {
        ABORT_NOT_OK(builder.Append(value));
      }
    }

    std::shared_ptr<Array> out;
    ABORT_NOT_OK(builder.Finish(&out));
  }

  if (fodder_nbytes == 0) {
    fodder_nbytes = fodder.size() * sizeof(Scalar);
  }
  state.SetBytesProcessed(state.iterations() * fodder_nbytes * kRounds);
  state.SetItemsProcessed(state.iterations() * fodder.size() * kRounds);
}

static void BuildInt64DictionaryArrayRandom(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeRandomIntDictFodder();
  BenchmarkDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildInt64DictionaryArraySequential(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSequentialIntDictFodder();
  BenchmarkDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildInt64DictionaryArraySimilar(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeSimilarIntDictFodder();
  BenchmarkDictionaryArray<DictionaryBuilder<Int64Type>>(state, fodder);
}

static void BuildStringDictionaryArray(
    benchmark::State& state) {  // NOLINT non-const reference
  const auto fodder = MakeStringDictFodder();
  auto fodder_nbytes =
      std::accumulate(fodder.begin(), fodder.end(), 0ULL,
                      [&](size_t acc, const std::string& s) { return acc + s.size(); });
  BenchmarkDictionaryArray<BinaryDictionaryBuilder>(state, fodder, fodder_nbytes);
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
  state.SetItemsProcessed(state.iterations() * kNumArrays);
}

// ----------------------------------------------------------------------
// BufferBuilder benchmarks

static void BenchmarkBufferBuilder(
    const std::string& datum,
    benchmark::State& state) {  // NOLINT non-const reference
  const void* raw_data = datum.data();
  int64_t raw_nbytes = static_cast<int64_t>(datum.size());
  // Write approx. 256 MB to BufferBuilder
  int64_t num_raw_values = (1 << 28) / raw_nbytes;
  for (auto _ : state) {
    BufferBuilder builder(memory_tracker.memory_pool());
    std::shared_ptr<Buffer> buf;
    for (int64_t i = 0; i < num_raw_values; ++i) {
      ABORT_NOT_OK(builder.Append(raw_data, raw_nbytes));
    }
    ABORT_NOT_OK(builder.Finish(&buf));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * num_raw_values * raw_nbytes);
}

static void BufferBuilderTinyWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 8-byte datum
  return BenchmarkBufferBuilder("abdefghi", state);
}

static void BufferBuilderSmallWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 700-byte datum
  std::string datum;
  for (int i = 0; i < 100; ++i) {
    datum += "abcdefg";
  }
  return BenchmarkBufferBuilder(datum, state);
}

static void BufferBuilderLargeWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  // A 1.5MB datum
  std::string datum(1500000, 'x');
  return BenchmarkBufferBuilder(datum, state);
}

BENCHMARK(BufferBuilderTinyWrites)->UseRealTime();
BENCHMARK(BufferBuilderSmallWrites)->UseRealTime();
BENCHMARK(BufferBuilderLargeWrites)->UseRealTime();

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
  state.SetItemsProcessed(state.iterations() * kItemsProcessed);
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
