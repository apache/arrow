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
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/compressed.h"
#include "arrow/io/memory.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/compression.h"
#include "arrow/util/config.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow::io {

using ::arrow::Compression;

std::vector<uint8_t> MakeCompressibleData(int data_size) {
  // XXX This isn't a real-world corpus so doesn't really represent the
  // comparative qualities of the algorithms

  // First make highly compressible data
  std::string base_data =
      "Apache Arrow is a cross-language development platform for in-memory data";
  int nrepeats = static_cast<int>(1 + data_size / base_data.size());

  std::vector<uint8_t> data(base_data.size() * nrepeats);
  for (int i = 0; i < nrepeats; ++i) {
    std::memcpy(data.data() + i * base_data.size(), base_data.data(), base_data.size());
  }
  data.resize(data_size);

  // Then randomly mutate some bytes so as to make things harder
  std::mt19937 engine(42);
  std::exponential_distribution<> offsets(0.05);
  std::uniform_int_distribution<> values(0, 255);

  int64_t pos = 0;
  while (pos < data_size) {
    data[pos] = static_cast<uint8_t>(values(engine));
    pos += static_cast<int64_t>(offsets(engine));
  }

  return data;
}

// Using a non-zero copy buffer reader to benchmark the non-zero copy path.
class NonZeroCopyBufferReader final : public InputStream {
 public:
  NonZeroCopyBufferReader(std::shared_ptr<Buffer> buffer) : reader_(std::move(buffer)) {}

  bool supports_zero_copy() const override { return false; }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    return reader_.Read(nbytes, out);
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    // Testing the non-zero copy path like reading from local file or Object store,
    // so we need to allocate a buffer and copy the data.
    ARROW_ASSIGN_OR_RAISE(auto buf, ::arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t size, Read(nbytes, buf->mutable_data()));
    ARROW_RETURN_NOT_OK(buf->Resize(size));
    return buf;
  }
  Status Close() override { return reader_.Close(); }
  Result<int64_t> Tell() const override { return reader_.Tell(); }
  bool closed() const override { return reader_.closed(); }

 private:
  ::arrow::io::BufferReader reader_;
};

enum class BufferReadMode { ProvidedByCaller, ReturnedByCallee };

template <typename BufReader, BufferReadMode Mode>
static void CompressedInputStreamBenchmark(::benchmark::State& state,
                                           Compression::type compression) {
  const int64_t input_size = state.range(0);
  const int64_t batch_size = state.range(1);

  const std::vector<uint8_t> data = MakeCompressibleData(static_cast<int>(input_size));
  auto codec = ::arrow::util::Codec::Create(compression).ValueOrDie();
  int64_t max_compress_len =
      codec->MaxCompressedLen(static_cast<int64_t>(data.size()), data.data());
  std::shared_ptr<::arrow::ResizableBuffer> buf =
      ::arrow::AllocateResizableBuffer(max_compress_len).ValueOrDie();
  const int64_t compressed_length =
      codec
          ->Compress(static_cast<int64_t>(data.size()), data.data(), max_compress_len,
                     buf->mutable_data())
          .ValueOrDie();
  ABORT_NOT_OK(buf->Resize(compressed_length));
  for (auto _ : state) {
    state.PauseTiming();
    auto reader = std::make_shared<BufReader>(buf);
    [[maybe_unused]] std::unique_ptr<Buffer> read_buffer;
    if constexpr (Mode == BufferReadMode::ProvidedByCaller) {
      read_buffer = ::arrow::AllocateBuffer(batch_size).ValueOrDie();
    }
    state.ResumeTiming();
    // Put `CompressedInputStream::Make` in timing.
    auto input_stream =
        ::arrow::io::CompressedInputStream::Make(codec.get(), reader).ValueOrDie();
    auto remaining_size = input_size;
    while (remaining_size > 0) {
      if constexpr (Mode == BufferReadMode::ProvidedByCaller) {
        auto value = input_stream->Read(batch_size, read_buffer->mutable_data());
        ABORT_NOT_OK(value);
        remaining_size -= value.ValueOrDie();
      } else {
        auto value = input_stream->Read(batch_size);
        ABORT_NOT_OK(value);
        remaining_size -= value.ValueOrDie()->size();
      }
    }
  }
  state.SetBytesProcessed(input_size * state.iterations());
}

template <Compression::type kCompression>
static void CompressedInputStreamZeroCopyBufferProvidedByCaller(
    ::benchmark::State& state) {
  CompressedInputStreamBenchmark<::arrow::io::BufferReader,
                                 BufferReadMode::ProvidedByCaller>(state, kCompression);
}

template <Compression::type kCompression>
static void CompressedInputStreamNonZeroCopyBufferProvidedByCaller(
    ::benchmark::State& state) {
  CompressedInputStreamBenchmark<NonZeroCopyBufferReader,
                                 BufferReadMode::ProvidedByCaller>(state, kCompression);
}

template <Compression::type kCompression>
static void CompressedInputStreamZeroCopyBufferReturnedByCallee(
    ::benchmark::State& state) {
  CompressedInputStreamBenchmark<::arrow::io::BufferReader,
                                 BufferReadMode::ReturnedByCallee>(state, kCompression);
}

template <Compression::type kCompression>
static void CompressedInputStreamNonZeroCopyBufferReturnedByCallee(
    ::benchmark::State& state) {
  CompressedInputStreamBenchmark<NonZeroCopyBufferReader,
                                 BufferReadMode::ReturnedByCallee>(state, kCompression);
}

#ifdef ARROW_WITH_LZ4
static void CompressedInputArguments(::benchmark::internal::Benchmark* b) {
  b->ArgNames({"num_bytes", "batch_size"})
      ->Args({8 * 1024, 8 * 1024})
      ->Args({64 * 1024, 8 * 1024})
      ->Args({64 * 1024, 64 * 1024})
      ->Args({1024 * 1024, 8 * 1024})
      ->Args({1024 * 1024, 64 * 1024})
      ->Args({1024 * 1024, 1024 * 1024});
}

// Benchmark LZ4 because it's lightweight, which makes benchmarking focused on the
// overhead of the compression input stream.
BENCHMARK_TEMPLATE(CompressedInputStreamZeroCopyBufferProvidedByCaller,
                   Compression::LZ4_FRAME)
    ->Apply(CompressedInputArguments);
BENCHMARK_TEMPLATE(CompressedInputStreamNonZeroCopyBufferProvidedByCaller,
                   Compression::LZ4_FRAME)
    ->Apply(CompressedInputArguments);
BENCHMARK_TEMPLATE(CompressedInputStreamZeroCopyBufferReturnedByCallee,
                   Compression::LZ4_FRAME)
    ->Apply(CompressedInputArguments);
BENCHMARK_TEMPLATE(CompressedInputStreamNonZeroCopyBufferReturnedByCallee,
                   Compression::LZ4_FRAME)
    ->Apply(CompressedInputArguments);
#endif

}  // namespace arrow::io
