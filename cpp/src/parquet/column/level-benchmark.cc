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

#include "parquet/column/levels.h"
#include "parquet/util/memory.h"

namespace parquet {

namespace benchmark {

static void BM_RleEncoding(::benchmark::State& state) {
  std::vector<int16_t> levels(state.range_x(), 0);
  int64_t n = 0;
  std::generate(levels.begin(), levels.end(),
      [&state, &n] { return (n++ % state.range_y()) == 0; });
  int16_t max_level = 1;
  int64_t rle_size = LevelEncoder::MaxBufferSize(Encoding::RLE, max_level, levels.size());
  auto buffer_rle = std::make_shared<PoolBuffer>();
  PARQUET_THROW_NOT_OK(buffer_rle->Resize(rle_size));

  while (state.KeepRunning()) {
    LevelEncoder level_encoder;
    level_encoder.Init(Encoding::RLE, max_level, levels.size(),
        buffer_rle->mutable_data(), buffer_rle->size());
    level_encoder.Encode(levels.size(), levels.data());
  }
  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(int16_t));
  state.SetItemsProcessed(state.iterations() * state.range_x());
}

BENCHMARK(BM_RleEncoding)->RangePair(1024, 65536, 1, 16);

static void BM_RleDecoding(::benchmark::State& state) {
  LevelEncoder level_encoder;
  std::vector<int16_t> levels(state.range_x(), 0);
  int64_t n = 0;
  std::generate(levels.begin(), levels.end(),
      [&state, &n] { return (n++ % state.range_y()) == 0; });
  int16_t max_level = 1;
  int64_t rle_size = LevelEncoder::MaxBufferSize(Encoding::RLE, max_level, levels.size());
  auto buffer_rle = std::make_shared<PoolBuffer>();
  PARQUET_THROW_NOT_OK(buffer_rle->Resize(rle_size + sizeof(int32_t)));
  level_encoder.Init(Encoding::RLE, max_level, levels.size(),
      buffer_rle->mutable_data() + sizeof(int32_t), rle_size);
  level_encoder.Encode(levels.size(), levels.data());
  reinterpret_cast<int32_t*>(buffer_rle->mutable_data())[0] = level_encoder.len();

  while (state.KeepRunning()) {
    LevelDecoder level_decoder;
    level_decoder.SetData(Encoding::RLE, max_level, levels.size(), buffer_rle->data());
    level_decoder.Decode(state.range_x(), levels.data());
  }

  state.SetBytesProcessed(state.iterations() * state.range_x() * sizeof(int16_t));
  state.SetItemsProcessed(state.iterations() * state.range_x());
}

BENCHMARK(BM_RleDecoding)->RangePair(1024, 65536, 1, 16);

}  // namespace benchmark

}  // namespace parquet
