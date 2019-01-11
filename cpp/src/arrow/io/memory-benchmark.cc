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

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/test-util.h"
#include "arrow/util/cpu-info.h"

#include "benchmark/benchmark.h"

#include <iostream>

namespace arrow {

// This include hyperthread cores which may slightly negatively impact the
// bandwidth result.
static int kNumCores = internal::CpuInfo::GetInstance()->num_cores();

static void BM_SerialMemcopy(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t kTotalSize = 128 * 1024 * 1024;          // 128MB

  std::shared_ptr<Buffer> buffer1, buffer2;
  ABORT_NOT_OK(AllocateBuffer(kTotalSize, &buffer1));
  ABORT_NOT_OK(AllocateBuffer(kTotalSize, &buffer2));
  random_bytes(kTotalSize, 0, buffer2->mutable_data());

  while (state.KeepRunning()) {
    io::FixedSizeBufferWriter writer(buffer1);
    ABORT_NOT_OK(writer.Write(buffer2->data(), buffer2->size()));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize * 2);
}

static void BM_ParallelMemcopy(benchmark::State& state) {   // NOLINT non-const reference
  const int64_t kTotalSize = kNumCores * 64 * 1024 * 1024;  // 64MB per core

  std::shared_ptr<Buffer> buffer1, buffer2;
  ABORT_NOT_OK(AllocateBuffer(kTotalSize, &buffer1));
  ABORT_NOT_OK(AllocateBuffer(kTotalSize, &buffer2));

  random_bytes(kTotalSize, 0, buffer2->mutable_data());

  while (state.KeepRunning()) {
    io::FixedSizeBufferWriter writer(buffer1);
    writer.set_memcopy_threads(kNumCores);
    ABORT_NOT_OK(writer.Write(buffer2->data(), buffer2->size()));
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * kTotalSize * 2);
}

BENCHMARK(BM_SerialMemcopy)->MinTime(1.0)->Repetitions(2)->UseRealTime();

BENCHMARK(BM_ParallelMemcopy)->MinTime(1.0)->Repetitions(2)->UseRealTime();

}  // namespace arrow
