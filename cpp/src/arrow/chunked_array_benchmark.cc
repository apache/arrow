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
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/chunked_array.h"
#include "arrow/type.h"

namespace arrow {
namespace {

constexpr int64_t kChunkLength = 8;

std::shared_ptr<ChunkedArray> MakeChunkedArray(int32_t num_chunks) {
  ArrayVector chunks;
  chunks.reserve(num_chunks);
  for (int32_t i = 0; i < num_chunks; ++i) {
    chunks.push_back(MakeArrayOfNull(int64(), kChunkLength).ValueOrDie());
  }
  return std::make_shared<ChunkedArray>(std::move(chunks), int64());
}

// Reproduce the pre-resolver lookup so both implementations can be compared in
// the same binary with the same input data and benchmark settings.
std::shared_ptr<ChunkedArray> SliceWithLinearLookup(const ChunkedArray& array,
                                                   int64_t offset, int64_t length) {
  const bool offset_equals_length = offset == array.length();
  int curr_chunk = 0;
  while (curr_chunk < array.num_chunks() && offset >= array.chunk(curr_chunk)->length()) {
    offset -= array.chunk(curr_chunk)->length();
    curr_chunk++;
  }

  ArrayVector new_chunks;
  if (array.num_chunks() > 0 && (offset_equals_length || length == 0)) {
    new_chunks.push_back(
        array.chunk(std::min(curr_chunk, array.num_chunks() - 1))->Slice(0, 0));
  } else {
    while (curr_chunk < array.num_chunks() && length > 0) {
      new_chunks.push_back(array.chunk(curr_chunk)->Slice(offset, length));
      length -= array.chunk(curr_chunk)->length() - offset;
      offset = 0;
      curr_chunk++;
    }
  }

  return std::make_shared<ChunkedArray>(new_chunks, array.type());
}

template <bool UseLinearLookup>
void ChunkedArraySliceFixedOffsetImpl(benchmark::State& state) {
  const int32_t num_chunks = static_cast<int32_t>(state.range(0));
  const int32_t start_percent = static_cast<int32_t>(state.range(1));
  auto array = MakeChunkedArray(num_chunks);
  const int64_t offset = (array->length() - 1) * start_percent / 100;

  for (auto _ : state) {
    if constexpr (UseLinearLookup) {
      benchmark::DoNotOptimize(SliceWithLinearLookup(*array, offset, 1));
    } else {
      benchmark::DoNotOptimize(array->Slice(offset, 1));
    }
  }
  state.SetItemsProcessed(state.iterations());
}

void ChunkedArraySliceFixedOffsetLinear(benchmark::State& state) {
  ChunkedArraySliceFixedOffsetImpl<true>(state);
}

void ChunkedArraySliceFixedOffsetResolver(benchmark::State& state) {
  ChunkedArraySliceFixedOffsetImpl<false>(state);
}

// Vary offsets to measure lookup when the cached chunk from the previous call
// is unlikely to match.
template <bool UseLinearLookup>
void ChunkedArraySliceVaryingOffsetsImpl(benchmark::State& state) {
  const int32_t num_chunks = static_cast<int32_t>(state.range(0));
  auto array = MakeChunkedArray(num_chunks);
  constexpr int32_t kNumOffsets = 4096;
  std::vector<int64_t> offsets;
  offsets.reserve(kNumOffsets);
  for (int32_t i = 0; i < kNumOffsets; ++i) {
    const int32_t chunk_index = (i * 7919) % num_chunks;
    offsets.push_back(static_cast<int64_t>(chunk_index) * kChunkLength);
  }

  int32_t offset_index = 0;
  for (auto _ : state) {
    if constexpr (UseLinearLookup) {
      benchmark::DoNotOptimize(
          SliceWithLinearLookup(*array, offsets[offset_index], 1));
    } else {
      benchmark::DoNotOptimize(array->Slice(offsets[offset_index], 1));
    }
    offset_index = (offset_index + 1) % kNumOffsets;
  }
  state.SetItemsProcessed(state.iterations());
}

void ChunkedArraySliceVaryingOffsetsLinear(benchmark::State& state) {
  ChunkedArraySliceVaryingOffsetsImpl<true>(state);
}

void ChunkedArraySliceVaryingOffsetsResolver(benchmark::State& state) {
  ChunkedArraySliceVaryingOffsetsImpl<false>(state);
}

}  // namespace

BENCHMARK(ChunkedArraySliceFixedOffsetLinear)
    ->ArgNames({"num_chunks", "start_percent"})
    ->Args({16, 0})
    ->Args({16, 50})
    ->Args({16, 99})
    ->Args({256, 0})
    ->Args({256, 50})
    ->Args({256, 99})
    ->Args({4096, 0})
    ->Args({4096, 50})
    ->Args({4096, 99})
    ->Args({16384, 0})
    ->Args({16384, 50})
    ->Args({16384, 99});

BENCHMARK(ChunkedArraySliceFixedOffsetResolver)
    ->ArgNames({"num_chunks", "start_percent"})
    ->Args({16, 0})
    ->Args({16, 50})
    ->Args({16, 99})
    ->Args({256, 0})
    ->Args({256, 50})
    ->Args({256, 99})
    ->Args({4096, 0})
    ->Args({4096, 50})
    ->Args({4096, 99})
    ->Args({16384, 0})
    ->Args({16384, 50})
    ->Args({16384, 99});

BENCHMARK(ChunkedArraySliceVaryingOffsetsLinear)
    ->ArgNames({"num_chunks"})
    ->Arg(256)
    ->Arg(4096)
    ->Arg(16384);

BENCHMARK(ChunkedArraySliceVaryingOffsetsResolver)
    ->ArgNames({"num_chunks"})
    ->Arg(256)
    ->Arg(4096)
    ->Arg(16384);

}  // namespace arrow
