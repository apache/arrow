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

#include <vector>

#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-util.h"

namespace arrow {
namespace BitUtil {

static void BM_CopyBitmap(benchmark::State& state) {  // NOLINT non-const reference
  const int kBufferSize = state.range(0);

  std::shared_ptr<Buffer> buffer;
  ASSERT_OK(AllocateBuffer(default_memory_pool(), kBufferSize, &buffer));
  memset(buffer->mutable_data(), 0, kBufferSize);
  test::random_bytes(kBufferSize, 0, buffer->mutable_data());

  const int num_bits = kBufferSize * 8;
  const uint8_t* src = buffer->data();

  std::shared_ptr<Buffer> copy;
  while (state.KeepRunning()) {
    ABORT_NOT_OK(CopyBitmap(default_memory_pool(), src, state.range(1), num_bits, &copy));
  }
  state.SetBytesProcessed(state.iterations() * kBufferSize * sizeof(int8_t));
}

BENCHMARK(BM_CopyBitmap)
    ->Args({100000, 0})
    ->Args({1000000, 0})
    ->Args({100000, 4})
    ->Args({1000000, 4})
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

}  // namespace BitUtil
}  // namespace arrow
