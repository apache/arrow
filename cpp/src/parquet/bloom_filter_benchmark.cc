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

template <typename DType>
static void BM_CountHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(1024);
  std::vector<uint8_t> heap;
  test::GenerateData(1024, values.data(), &heap);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(1024, /*fpp=*/0.05));
  BloomFilter* filter = &block_split_bloom_filter;
  ::benchmark::DoNotOptimize(filter);
  for (auto _ : state) {
    for (int i = 0; i < 1024; ++i) {
      if constexpr (std::is_same_v<DType, FLBAType>) {
        filter->Hash(&values[i], test::kGenerateDataFLBALength);
      } else {
        filter->Hash(values[i]);
      }
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

template <typename DType>
static void BM_BatchCountHash(::benchmark::State& state) {
  using T = typename DType::c_type;
  std::vector<T> values(1024);
  std::vector<uint8_t> heap;
  test::GenerateData(1024, values.data(), &heap);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(1024, /*fpp=*/0.05));
  BloomFilter* filter = &block_split_bloom_filter;
  ::benchmark::DoNotOptimize(filter);
  std::vector<uint64_t> hashes(1024);
  for (auto _ : state) {
    if constexpr (std::is_same_v<DType, FLBAType>) {
      filter->Hashes(values.data(), test::kGenerateDataFLBALength,
                     static_cast<int>(values.size()), hashes.data());
    } else {
      filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_InsertHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(1024);
  std::vector<uint8_t> heap;
  test::GenerateData(1024, values.data(), &heap);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(1024, /*fpp=*/0.05));
  BloomFilter* filter = &block_split_bloom_filter;
  ::benchmark::DoNotOptimize(filter);
  std::vector<uint64_t> hashes(1024);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  for (auto _ : state) {
    for (auto hash : hashes) {
      filter->InsertHash(hash);
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_BatchInsertHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(1024);
  std::vector<uint8_t> heap;
  test::GenerateData(1024, values.data(), &heap);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(1024, /*fpp=*/0.05));
  BloomFilter* filter = &block_split_bloom_filter;
  ::benchmark::DoNotOptimize(filter);
  std::vector<uint64_t> hashes(1024);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  for (auto _ : state) {
    filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()));
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

static void BM_FindHash(::benchmark::State& state) {
  using T = int32_t;
  std::vector<T> values(1024);
  std::vector<uint8_t> heap;
  test::GenerateData(1024, values.data(), &heap);
  BlockSplitBloomFilter block_split_bloom_filter;
  block_split_bloom_filter.Init(
      BlockSplitBloomFilter::OptimalNumOfBytes(1024, /*fpp=*/0.05));
  BloomFilter* filter = &block_split_bloom_filter;
  ::benchmark::DoNotOptimize(filter);
  std::vector<uint64_t> hashes(1024);
  filter->Hashes(values.data(), static_cast<int>(values.size()), hashes.data());
  filter->InsertHashes(hashes.data(), static_cast<int>(hashes.size()));
  for (auto _ : state) {
    for (auto hash : hashes) {
      ::benchmark::DoNotOptimize(filter->FindHash(hash));
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}

BENCHMARK_TEMPLATE(BM_CountHash, Int32Type);
BENCHMARK_TEMPLATE(BM_CountHash, Int64Type);
BENCHMARK_TEMPLATE(BM_CountHash, FloatType);
BENCHMARK_TEMPLATE(BM_CountHash, DoubleType);
BENCHMARK_TEMPLATE(BM_BatchCountHash, Int32Type);
BENCHMARK_TEMPLATE(BM_BatchCountHash, Int64Type);
BENCHMARK_TEMPLATE(BM_BatchCountHash, FloatType);
BENCHMARK_TEMPLATE(BM_BatchCountHash, DoubleType);

BENCHMARK(BM_InsertHash);
BENCHMARK(BM_InsertHash);
BENCHMARK(BM_BatchInsertHash);
BENCHMARK(BM_BatchInsertHash);
BENCHMARK(BM_FindHash);
BENCHMARK(BM_FindHash);

}  // namespace benchmark
}  // namespace parquet
