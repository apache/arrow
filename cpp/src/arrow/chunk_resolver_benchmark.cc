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

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "arrow/chunk_resolver.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/pcg_random.h"

namespace arrow {

using internal::ChunkResolver;
using internal::TypedChunkLocation;

namespace {

int64_t constexpr kChunkedArrayLength = std::numeric_limits<uint16_t>::max();

struct ResolveManyBenchmark {
  benchmark::State& state;
  random::pcg64 rng;
  // Values from the state.range(i)
  int64_t chunked_array_length;
  int32_t num_chunks;
  int64_t num_logical_indices;

  explicit ResolveManyBenchmark(benchmark::State& state)
      : state(state),
        rng(42),
        chunked_array_length(state.range(0)),
        num_chunks(static_cast<int32_t>(state.range(1))),
        num_logical_indices(state.range(2)) {}

  std::vector<int64_t> GenChunkedArrayOffsets() {
    std::uniform_int_distribution<int64_t> offset_gen(1, chunked_array_length);
    std::vector<int64_t> offsets;
    offsets.reserve(num_chunks + 1);
    offsets.push_back(0);
    while (offsets.size() < static_cast<size_t>(num_chunks)) {
      offsets.push_back(offset_gen(rng));
    }
    offsets.push_back(chunked_array_length);
    std::sort(offsets.begin() + 1, offsets.end());
    return offsets;
  }

  template <typename IndexType>
  std::vector<IndexType> GenRandomIndices(IndexType max_index, bool sorted) {
    std::uniform_int_distribution<IndexType> index_gen(0, max_index);
    std::vector<IndexType> indices;
    indices.reserve(num_logical_indices);
    while (indices.size() < static_cast<size_t>(num_logical_indices)) {
      indices.push_back(index_gen(rng));
    }
    if (sorted) {
      std::sort(indices.begin(), indices.end());
    }
    return indices;
  }

  template <typename IndexType>
  void Bench(bool sorted) {
    if constexpr (sizeof(IndexType) < 8) {
      constexpr uint64_t kLimitIndex = std::numeric_limits<IndexType>::max();
      ARROW_CHECK_LE(static_cast<uint64_t>(chunked_array_length), kLimitIndex);
    }
    const auto max_random_index = static_cast<IndexType>(chunked_array_length);
    auto offsets = GenChunkedArrayOffsets();
    auto logical_indices = GenRandomIndices<IndexType>(max_random_index, sorted);
    ChunkResolver resolver(std::move(offsets));
    std::vector<TypedChunkLocation<IndexType>> chunk_location_vec(num_logical_indices);
    BENCHMARK_UNUSED bool all_succeeded = true;
    for (auto _ : state) {
      const bool success = resolver.ResolveMany<IndexType>(
          num_logical_indices, logical_indices.data(), chunk_location_vec.data());
      all_succeeded &= success;
    }
    ARROW_CHECK(all_succeeded);
    state.SetItemsProcessed(state.iterations() * num_logical_indices);
  }
};

template <typename IndexType>
void ResolveManySetArgs(benchmark::internal::Benchmark* bench) {
  constexpr int32_t kNonAligned = 3;
  const int64_t kNumIndicesFew = (kChunkedArrayLength >> 7) - kNonAligned;
  const int64_t kNumIndicesMany = (kChunkedArrayLength >> 1) - kNonAligned;

  bench->ArgNames({"chunked_array_length", "num_chunks", "num_indices"});

  switch (sizeof(IndexType)) {
    case 1:
      // Unexpected. See comments below.
    case 2:
    case 4:
    case 8:
      bench->Args({kChunkedArrayLength, /*num_chunks*/ 10000, kNumIndicesFew});
      bench->Args({kChunkedArrayLength, /*num_chunks*/ 100, kNumIndicesFew});
      bench->Args({kChunkedArrayLength, /*num_chunks*/ 10000, kNumIndicesMany});
      bench->Args({kChunkedArrayLength, /*num_chunks*/ 100, kNumIndicesMany});
      break;
  }
}

template <typename IndexType>
void ResolveManyBench(benchmark::State& state, bool sorted) {
  ResolveManyBenchmark{state}.Bench<IndexType>(sorted);
}

void ResolveManyUInt16Random(benchmark::State& state) {
  ResolveManyBench<uint16_t>(state, false);
}

void ResolveManyUInt32Random(benchmark::State& state) {
  ResolveManyBench<uint32_t>(state, false);
}

void ResolveManyUInt64Random(benchmark::State& state) {
  ResolveManyBench<uint64_t>(state, false);
}

void ResolveManyUInt16Sorted(benchmark::State& state) {
  ResolveManyBench<uint16_t>(state, true);
}

void ResolveManyUInt32Sorted(benchmark::State& state) {
  ResolveManyBench<uint32_t>(state, true);
}

void ResolveManyUInt64Sorted(benchmark::State& state) {
  ResolveManyBench<uint64_t>(state, true);
}

}  // namespace

// We don't benchmark with uint8_t because it's too fast -- any meaningful
// array of logical indices will contain all the possible values.

BENCHMARK(ResolveManyUInt16Random)->Apply(ResolveManySetArgs<uint16_t>);
BENCHMARK(ResolveManyUInt32Random)->Apply(ResolveManySetArgs<uint32_t>);
BENCHMARK(ResolveManyUInt64Random)->Apply(ResolveManySetArgs<uint64_t>);

BENCHMARK(ResolveManyUInt16Sorted)->Apply(ResolveManySetArgs<uint16_t>);
BENCHMARK(ResolveManyUInt32Sorted)->Apply(ResolveManySetArgs<uint32_t>);
BENCHMARK(ResolveManyUInt64Sorted)->Apply(ResolveManySetArgs<uint64_t>);

}  // namespace arrow
