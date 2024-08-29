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

#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "benchmark/benchmark.h"

namespace arrow {
namespace bit_util {

using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::BitmapWordReader;

const int64_t kBufferSize = 1024 * (std::rand() % 25 + 1000);

// const int seed = std::rand();

static std::shared_ptr<Buffer> CreateRandomBuffer(int64_t nbytes) {
  auto buffer = *AllocateBuffer(nbytes);
  memset(buffer->mutable_data(), 0, nbytes);
  random_bytes(nbytes, /*seed=*/0, buffer->mutable_data());
  return std::move(buffer);
}

static void BitBlockCounterBench(benchmark::State& state) {
  int64_t nbytes = state.range(0);
  std::shared_ptr<Buffer> cond_buf = CreateRandomBuffer(nbytes);
  for (auto _ : state) {
    BitBlockCounter counter(cond_buf->data(), 0, nbytes * 8);

    int64_t offset = 0;
    uint64_t set_bits = 0;

    while (offset < nbytes * 8) {
      const BitBlockCount& word = counter.NextWord();
      //      if (word.AllSet()) {
      //        set_bits += word.length;
      //      } else if (word.popcount) {
      //        set_bits += word.popcount;
      //      }
      set_bits += word.popcount;
      benchmark::DoNotOptimize(set_bits);
      offset += word.length;
    }
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * nbytes);
}

static void BitmapWordReaderBench(benchmark::State& state) {
  int64_t nbytes = state.range(0);
  std::shared_ptr<Buffer> cond_buf = CreateRandomBuffer(nbytes);
  for (auto _ : state) {
    BitmapWordReader<uint64_t> counter(cond_buf->data(), 0, nbytes * 8);

    int64_t set_bits = 0;

    int64_t cnt = counter.words();
    while (cnt--) {
      const auto& word = counter.NextWord();
      //      if (word == UINT64_MAX) {
      //        set_bits += sizeof(uint64_t) * 8;
      //      } else if (word) {
      //        set_bits += PopCount(word);
      //      }
      set_bits += PopCount(word);
      benchmark::DoNotOptimize(set_bits);
    }

    cnt = counter.trailing_bytes();
    while (cnt--) {
      int valid_bits;
      const auto& byte = static_cast<uint32_t>(counter.NextTrailingByte(valid_bits));
      set_bits += PopCount(kPrecedingBitmask[valid_bits] & byte);
      benchmark::DoNotOptimize(set_bits);
    }
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

BENCHMARK(BitBlockCounterBench)->Arg(kBufferSize);
BENCHMARK(BitmapWordReaderBench)->Arg(kBufferSize);

}  // namespace bit_util
}  // namespace arrow
