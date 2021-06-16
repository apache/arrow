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
namespace BitUtil {

using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::BitmapWordReader;

const int64_t kBufferSize = 1024 * 1024;

static std::shared_ptr<Buffer> CreateRandomBuffer(int64_t nbytes) {
  auto buffer = *AllocateBuffer(nbytes);
  memset(buffer->mutable_data(), 0, nbytes);
  random_bytes(nbytes, /*seed=*/0, buffer->mutable_data());
  return std::move(buffer);
}

static void BitBlockCounterBench(benchmark::State& state) {
  int64_t nbytes = state.range(0);
  std::shared_ptr<Buffer> cond_buf = CreateRandomBuffer(nbytes);
  std::shared_ptr<Buffer> data_buf = CreateRandomBuffer(nbytes * 8 * 8);
  std::shared_ptr<Buffer> dest_buf = CreateRandomBuffer(nbytes * 8 * 8);
  for (auto _ : state) {
    BitBlockCounter counter(cond_buf->data(), 0, nbytes * 8);

    const uint8_t* cond_ptr = cond_buf->data();
    const uint64_t* data_ptr = reinterpret_cast<const uint64_t*>(data_buf->data());
    uint64_t* dest_ptr = reinterpret_cast<uint64_t*>(dest_buf->mutable_data());

    int64_t offset = 0;
    while (offset < nbytes * 8) {
      const BitBlockCount& word = counter.NextWord();
      if (word.AllSet()) {
        std::memcpy(dest_ptr + offset, data_ptr + offset, word.length * 8);
      } else if (word.popcount) {
        for (int64_t i = 0; i < word.length; i++) {
          if (GetBit(cond_ptr, offset + i)) {
            dest_ptr[offset + i] = data_ptr[offset + i];
          }
        }
      }
      offset += word.length;
    }
    benchmark::ClobberMemory();
  }

  state.SetBytesProcessed(state.iterations() * nbytes);
}

static void BitmapWordReaderBench(benchmark::State& state) {
  int64_t nbytes = state.range(0);
  std::shared_ptr<Buffer> cond_buf = CreateRandomBuffer(nbytes);
  std::shared_ptr<Buffer> data_buf = CreateRandomBuffer(nbytes * 8 * 8);
  std::shared_ptr<Buffer> dest_buf = CreateRandomBuffer(nbytes * 8 * 8);

  for (auto _ : state) {
    BitmapWordReader<uint64_t> counter(cond_buf->data(), 0, nbytes * 8);

    const uint8_t* cond_ptr = cond_buf->data();
    const auto* data_ptr = reinterpret_cast<const uint64_t*>(data_buf->data());
    auto* dest_ptr = reinterpret_cast<uint64_t*>(dest_buf->mutable_data());

    int64_t offset = 0;
    int64_t cnt = counter.words();
    while (cnt--) {
      const auto& word = counter.NextWord();
      if (word == UINT64_MAX) {
        std::memcpy(dest_ptr + offset, data_ptr + offset, 64 * 8);
      } else if (word) {
        for (int64_t i = 0; i < 8; i++) {
          if (GetBit(cond_ptr, offset + i)) {
            dest_ptr[offset + i] = data_ptr[offset + i];
          }
        }
      }
      offset += 8;
    }

    cnt = counter.trailing_bytes();
    while (cnt--) {
      int valid_bits;
      const auto& byte = counter.NextTrailingByte(valid_bits);
      if (byte == UINT8_MAX && valid_bits == 8) {
        std::memcpy(dest_ptr, data_ptr, 8 * 8);
      } else {
        for (int64_t i = 0; i < valid_bits; i++) {
          if (GetBit(cond_ptr, offset + i)) {
            dest_ptr[offset + i] = data_ptr[offset + i];
          }
        }
      }

      offset += valid_bits;
    }
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

BENCHMARK(BitBlockCounterBench)->Arg(kBufferSize);
BENCHMARK(BitmapWordReaderBench)->Arg(kBufferSize);

}  // namespace BitUtil
}  // namespace arrow
