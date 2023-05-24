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

#include "parquet/bloom_filter.h"
#include "parquet/test_util.h"

namespace parquet {
namespace benchmark {

constexpr static uint32_t kBloomFilterElementSize = 1024;

std::unique_ptr<BloomFilter> createBloomFilter(uint32_t elementSize) {
  std::unique_ptr<BlockSplitBloomFilter> block_split_bloom_filter =
      std::make_unique<BlockSplitBloomFilter>();
  block_split_bloom_filter->Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(elementSize, /*fpp=*/0.05));
  std::unique_ptr<BloomFilter> bloom_filter = std::move(block_split_bloom_filter);
  ::benchmark::DoNotOptimize(bloom_filter);
  return bloom_filter;
}

template <typename DType>
static void BM_ComputeHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  for (auto _ : state) {
    for (const auto& value : values) {
      if constexpr (std::is_same_v<DType, FLBAType>) {
        filter->Hash(&value, test::kGenerateDataFLBALength);
      } else {
        filter->Hash(value);
      }
      ::benchmark::ClobberMemory();
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

template <typename DType>
static void BM_BatchComputeHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  std::vector<uint64_t> hashes(kBloomFilterElementSize);
  for (auto _ : state) {
    if constexpr (std::is_same_v<DType, FLBAType>) {
      filter->Hashes(values.data(), test::kGenerateDataFLBALength,
                     static_cast<int>(values.size()), hashes.data());
    } else {
      filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
    }
    ::benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_InsertHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  std::vector<uint64_t> hashes(1024);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  for (auto _ : state) {
    for (auto hash : hashes) {
      filter->InsertHash(hash);
      ::benchmark::ClobberMemory();
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_BatchInsertHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  std::vector<uint64_t> hashes(kBloomFilterElementSize);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  for (auto _ : state) {
    filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()));
    ::benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_FindExistsHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  std::vector<uint64_t> hashes(kBloomFilterElementSize);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  // Insert all but the last hash.
  filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()) - 1);
  uint32_t exists = values[0];
  for (auto _ : state) {
    bool found = filter->FindHash(exists);
    ::benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations());
}

static void BM_FindNotExistsHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(kBloomFilterElementSize);
  std::vector<uint8_t> heap;
  test::GenerateData(kBloomFilterElementSize, values.data(), &heap);
  auto filter = createBloomFilter(kBloomFilterElementSize);
  std::vector<uint64_t> hashes(kBloomFilterElementSize);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  // Insert all but the last hash.
  filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()) - 1);
  uint32_t unexist = values.back();
  for (auto _ : state) {
    bool found = filter->FindHash(unexist);
    ::benchmark::DoNotOptimize(found);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(BM_ComputeHash, Int32Type);
BENCHMARK_TEMPLATE(BM_ComputeHash, Int64Type);
BENCHMARK_TEMPLATE(BM_ComputeHash, FloatType);
BENCHMARK_TEMPLATE(BM_ComputeHash, DoubleType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, Int32Type);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, Int64Type);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, FloatType);
BENCHMARK_TEMPLATE(BM_BatchComputeHash, DoubleType);

BENCHMARK(BM_InsertHash);
BENCHMARK(BM_BatchInsertHash);
BENCHMARK(BM_FindExistsHash);
BENCHMARK(BM_FindNotExistsHash);

}  // namespace benchmark
}  // namespace parquet
