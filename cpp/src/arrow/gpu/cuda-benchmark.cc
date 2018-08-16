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

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/memory_pool.h"
#include "arrow/test-util.h"

#include "arrow/gpu/cuda_api.h"

namespace arrow {
namespace gpu {

constexpr int64_t kGpuNumber = 0;

static void CudaBufferWriterBenchmark(benchmark::State& state, const int64_t total_bytes,
                                      const int64_t chunksize,
                                      const int64_t buffer_size) {
  CudaDeviceManager* manager;
  ABORT_NOT_OK(CudaDeviceManager::GetInstance(&manager));
  std::shared_ptr<CudaContext> context;
  ABORT_NOT_OK(manager->GetContext(kGpuNumber, &context));

  std::shared_ptr<CudaBuffer> device_buffer;
  ABORT_NOT_OK(context->Allocate(total_bytes, &device_buffer));
  CudaBufferWriter writer(device_buffer);

  if (buffer_size > 0) {
    ABORT_NOT_OK(writer.SetBufferSize(buffer_size));
  }

  std::shared_ptr<ResizableBuffer> buffer;
  ASSERT_OK(MakeRandomByteBuffer(total_bytes, default_memory_pool(), &buffer));

  const uint8_t* host_data = buffer->data();
  while (state.KeepRunning()) {
    int64_t bytes_written = 0;
    ABORT_NOT_OK(writer.Seek(0));
    while (bytes_written < total_bytes) {
      int64_t bytes_to_write = std::min(chunksize, total_bytes - bytes_written);
      ABORT_NOT_OK(writer.Write(host_data + bytes_written, bytes_to_write));
      bytes_written += bytes_to_write;
    }
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * total_bytes);
}

static void BM_Writer_Buffered(benchmark::State& state) {
  // 128MB
  const int64_t kTotalBytes = 1 << 27;

  // 8MB
  const int64_t kBufferSize = 1 << 23;

  CudaBufferWriterBenchmark(state, kTotalBytes, state.range(0), kBufferSize);
}

static void BM_Writer_Unbuffered(benchmark::State& state) {
  // 128MB
  const int64_t kTotalBytes = 1 << 27;
  CudaBufferWriterBenchmark(state, kTotalBytes, state.range(0), 0);
}

// Vary chunk write size from 256 bytes to 64K
BENCHMARK(BM_Writer_Buffered)
    ->RangeMultiplier(16)
    ->Range(1 << 8, 1 << 16)
    ->MinTime(1.0)
    ->UseRealTime();

BENCHMARK(BM_Writer_Unbuffered)
    ->RangeMultiplier(4)
    ->RangeMultiplier(16)
    ->Range(1 << 8, 1 << 16)
    ->MinTime(1.0)
    ->UseRealTime();

}  // namespace gpu
}  // namespace arrow
