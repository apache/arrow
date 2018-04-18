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
#include "arrow/io/buffered.h"
#include "arrow/io/file.h"
#include "arrow/test-util.h"

#include "benchmark/benchmark.h"

#include <algorithm>
#include <iostream>
#include <valarray>

namespace arrow {

// XXX Writing to /dev/null is irrealistic as the kernel likely doesn't
// copy the data at all.  Use a socketpair instead?
std::string GetNullFile() { return "/dev/null"; }

const std::valarray<int64_t> small_sizes = {8, 24, 33, 1, 32, 192, 16, 40};
const std::valarray<int64_t> large_sizes = {8192, 100000};

static void BenchmarkStreamingWrites(benchmark::State& state,
                                     std::valarray<int64_t> sizes,
                                     io::OutputStream* stream) {
  const std::string datastr(*std::max_element(std::begin(sizes), std::end(sizes)), 'x');
  const void* data = datastr.data();
  const int64_t sum_sizes = sizes.sum();

  while (state.KeepRunning()) {
    for (const int64_t size : sizes) {
      ABORT_NOT_OK(stream->Write(data, size));
    }
  }
  state.SetBytesProcessed(int64_t(state.iterations()) * sum_sizes);
}

static void BM_FileOutputStreamSmallWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &stream));

  BenchmarkStreamingWrites(state, small_sizes, stream.get());
}

static void BM_FileOutputStreamLargeWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &stream));

  BenchmarkStreamingWrites(state, large_sizes, stream.get());
}

static void BM_BufferedOutputStreamSmallWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &stream));
  stream = std::make_shared<io::BufferedOutputStream>(std::move(stream));

  BenchmarkStreamingWrites(state, small_sizes, stream.get());
}

static void BM_BufferedOutputStreamLargeWrites(
    benchmark::State& state) {  // NOLINT non-const reference
  std::shared_ptr<io::OutputStream> stream;
  ABORT_NOT_OK(io::FileOutputStream::Open(GetNullFile(), &stream));
  stream = std::make_shared<io::BufferedOutputStream>(std::move(stream));

  BenchmarkStreamingWrites(state, large_sizes, stream.get());
}

BENCHMARK(BM_FileOutputStreamSmallWrites)->Repetitions(2)->MinTime(1.0);

BENCHMARK(BM_FileOutputStreamLargeWrites)->Repetitions(2)->MinTime(1.0);

BENCHMARK(BM_BufferedOutputStreamSmallWrites)->Repetitions(2)->MinTime(1.0);

BENCHMARK(BM_BufferedOutputStreamLargeWrites)->Repetitions(2)->MinTime(1.0);

}  // namespace arrow
