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

#include "arrow/util/logging.h"
#include "parquet/bloom_filter.h"
#include "parquet/properties.h"

#include <random>

namespace parquet {
namespace benchmark {

constexpr static uint32_t kNumBloomFilterInserts = 16 * 1024;
// The sample string length for FLBA and ByteArray benchmarks
constexpr static uint32_t kDataStringLength = 8;

std::unique_ptr<BloomFilter> CreateBloomFilter(uint32_t num_values) {
  std::unique_ptr<BlockSplitBloomFilter> block_split_bloom_filter =
      std::make_unique<BlockSplitBloomFilter>();
  block_split_bloom_filter->Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(num_values, /*fpp=*/0.05));
  std::unique_ptr<BloomFilter> bloom_filter = std::move(block_split_bloom_filter);
  ::benchmark::DoNotOptimize(bloom_filter);
  return bloom_filter;
}

void GenerateRandomString(uint32_t length, uint32_t seed, std::vector<uint8_t>* heap) {
  // Character set used to generate random string
  const std::string charset =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  std::default_random_engine gen(seed);
  std::uniform_int_distribution<uint32_t> dist(0, static_cast<int>(charset.size() - 1));

  for (uint32_t i = 0; i < length; i++) {
    heap->push_back(charset[dist(gen)]);
  }
}

template <typename T>
void GenerateBenchmarkData(uint32_t size, uint32_t seed, T* data,
                           [[maybe_unused]] std::vector<uint8_t>* heap = nullptr) {
  if constexpr (std::is_integral_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<T> d(std::numeric_limits<T>::min(),
                                       std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_floating_point_v<T>) {
    std::default_random_engine gen(seed);
    std::uniform_real_distribution<T> d(std::numeric_limits<T>::lowest(),
                                        std::numeric_limits<T>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i] = d(gen);
    }
  } else if constexpr (std::is_same_v<FLBA, T>) {
    GenerateRandomString(kDataStringLength * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * kDataStringLength;
    }
  } else if constexpr (std::is_same_v<ByteArray, T>) {
    GenerateRandomString(kDataStringLength * size, seed, heap);
    for (uint32_t i = 0; i < size; ++i) {
      data[i].ptr = heap->data() + i * kDataStringLength;
      data[i].len = kDataStringLength;
    }
  } else if constexpr (std::is_same_v<Int96, T>) {
    std::default_random_engine gen(seed);
    std::uniform_int_distribution<int> d(std::numeric_limits<int>::min(),
                                         std::numeric_limits<int>::max());
    for (uint32_t i = 0; i < size; ++i) {
      data[i].value[0] = d(gen);
      data[i].value[1] = d(gen);
      data[i].value[2] = d(gen);
    }
  }
}

std::vector<uint64_t> GetHashValues(uint32_t num_values, uint32_t seed) {
  // Generate sample data values
  std::vector<int64_t> values(num_values);
  GenerateBenchmarkData(num_values, seed, values.data());
  // Create a temp filter to compute hash values
  auto filter = CreateBloomFilter(/*num_values=*/8);
  std::vector<uint64_t> hashes(num_values);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  return hashes;
}

template <typename DType>
static void BM_ComputeHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(kNumBloomFilterInserts);
  std::vector<uint8_t> heap;
  GenerateBenchmarkData(kNumBloomFilterInserts, /*seed=*/0, values.data(), &heap);
  auto filter = CreateBloomFilter(kNumBloomFilterInserts);
  for (auto _ : state) {
    uint64_t total = 0;
    for (const auto& value : values) {
      uint64_t hash = 0;
      if constexpr (std::is_same_v<DType, FLBAType>) {
        hash = filter->Hash(&value, kDataStringLength);
      } else if constexpr (std::is_same_v<DType, Int96Type>) {
        hash = filter->Hash(&value);
      } else if constexpr (std::is_same_v<DType, ByteArrayType>) {
        hash = filter->Hash(&value);
      } else {
        hash = filter->Hash(value);
      }
      total += hash;
    }
    ::benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

template <typename DType>
static void BM_BatchComputeHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(kNumBloomFilterInserts);
  std::vector<uint8_t> heap;
  GenerateBenchmarkData(kNumBloomFilterInserts, /*seed=*/0, values.data(), &heap);
  auto filter = CreateBloomFilter(kNumBloomFilterInserts);
  std::vector<uint64_t> hashes(kNumBloomFilterInserts);
  for (auto _ : state) {
    if constexpr (std::is_same_v<DType, FLBAType>) {
      filter->Hashes(values.data(), kDataStringLength, static_cast<int>(values.size()),
                     hashes.data());
    } else {
      filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
    }
    ::benchmark::DoNotOptimize(hashes);
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_InsertHash(::benchmark::State& state) {
  std::vector<uint64_t> hashes = GetHashValues(kNumBloomFilterInserts, /*seed=*/0);
  for (auto _ : state) {
    state.PauseTiming();
    auto filter = CreateBloomFilter(kNumBloomFilterInserts);
    state.ResumeTiming();
    for (auto hash : hashes) {
      filter->InsertHash(hash);
    }
    ::benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * hashes.size());
}

static void BM_BatchInsertHash(::benchmark::State& state) {
  std::vector<uint64_t> hashes = GetHashValues(kNumBloomFilterInserts, /*seed=*/0);
  for (auto _ : state) {
    state.PauseTiming();
    auto filter = CreateBloomFilter(kNumBloomFilterInserts);
    state.ResumeTiming();
    filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()));
    ::benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * hashes.size());
}

static void BM_FindExistingHash(::benchmark::State& state) {
  std::vector<uint64_t> hashes = GetHashValues(kNumBloomFilterInserts, /*seed=*/0);
  auto filter = CreateBloomFilter(kNumBloomFilterInserts);
  filter->InsertHashes(hashes.data(), kNumBloomFilterInserts);
  for (auto _ : state) {
    uint64_t total = 0;
    for (auto hash : hashes) {
      bool found = filter->FindHash(hash);
      total += found;
    }
    ARROW_CHECK_EQ(total, hashes.size());
    ::benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * hashes.size());
}

static void BM_FindNonExistingHash(::benchmark::State& state) {
  std::vector<uint64_t> hashes = GetHashValues(kNumBloomFilterInserts, /*seed=*/0);
  auto filter = CreateBloomFilter(kNumBloomFilterInserts);
  filter->InsertHashes(hashes.data(), kNumBloomFilterInserts);
  // Use different seed to generate non-existing data.
  hashes = GetHashValues(kNumBloomFilterInserts, /*seed=*/100000);
  for (auto _ : state) {
    uint64_t total = 0;
    for (auto hash : hashes) {
      bool found = filter->FindHash(hash);
      total += found;
    }
    ARROW_CHECK_LE(total, 0.1 * hashes.size());
    ::benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * hashes.size());
}

BENCHMARK_TEMPLATE(BM_ComputeHash, Int32Type);
BENCHMARK_TEMPLATE(BM_ComputeHash, Int64Type);
BENCHMARK_TEMPLATE(BM_ComputeHash, FloatType);
BENCHMARK_TEMPLATE(BM_ComputeHash, DoubleType);
BENCHMARK_TEMPLATE(BM_ComputeHash, ByteArrayType);
BENCHMARK_TEMPLATE(BM_ComputeHash, FLBAType);
BENCHMARK_TEMPLATE(BM_ComputeHash, Int96Type);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, Int32Type);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, Int64Type);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, FloatType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, DoubleType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, ByteArrayType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, FLBAType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, Int96Type);

BENCHMARK(BM_InsertHash);
BENCHMARK(BM_BatchInsertHash);
BENCHMARK(BM_FindExistingHash);
BENCHMARK(BM_FindNonExistingHash);

}  // namespace benchmark
}  // namespace parquet
