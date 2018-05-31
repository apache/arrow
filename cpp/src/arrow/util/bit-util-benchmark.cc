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

// A naive bitmap reader implementation, meant as a baseline against
// internal::BitmapReader

class NaiveBitmapReader {
 public:
  NaiveBitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0) {}

  bool IsSet() const {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    return (bitmap_[byte_offset] & (1 << bit_offset)) == 0;
  }

  bool IsNotSet() const { return !IsSet(); }

  void Next() { ++position_; }

 private:
  const uint8_t* bitmap_;
  int64_t position_;
};

// A naive bitmap writer implementation, meant as a baseline against
// internal::BitmapWriter

class NaiveBitmapWriter {
 public:
  NaiveBitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0) {}

  void Set() {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    bitmap_[byte_offset] |= static_cast<uint8_t>(1 << bit_offset);
  }

  void Clear() {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    bitmap_[byte_offset] &= 0xFF ^ static_cast<uint8_t>(1 << bit_offset);
  }

  void Next() { ++position_; }

  void Finish() {}

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
};

static std::shared_ptr<Buffer> CreateRandomBuffer(int64_t nbytes) {
  std::shared_ptr<Buffer> buffer;
  ABORT_NOT_OK(AllocateBuffer(default_memory_pool(), nbytes, &buffer));
  memset(buffer->mutable_data(), 0, nbytes);
  test::random_bytes(nbytes, 0, buffer->mutable_data());
  return buffer;
}

template <typename BitmapReaderType>
static void BenchmarkBitmapReader(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  const uint8_t* bitmap = buffer->data();

  while (state.KeepRunning()) {
    {
      BitmapReaderType reader(bitmap, 0, num_bits);
      int64_t total = 0;
      for (int64_t i = 0; i < num_bits; i++) {
        total += reader.IsSet();
        reader.Next();
      }
      benchmark::DoNotOptimize(total);
    }
    {
      BitmapReaderType reader(bitmap, 0, num_bits);
      int64_t total = 0;
      for (int64_t i = 0; i < num_bits; i++) {
        total += !reader.IsNotSet();
        reader.Next();
      }
      benchmark::DoNotOptimize(total);
    }
  }
  state.SetBytesProcessed(2 * int64_t(state.iterations()) * nbytes);
}

template <typename BitmapWriterType>
static void BenchmarkBitmapWriter(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  uint8_t* bitmap = buffer->mutable_data();

  while (state.KeepRunning()) {
    {
      BitmapWriterType writer(bitmap, 0, num_bits);
      for (int64_t i = 0; i < num_bits; i++) {
        writer.Set();
        writer.Next();
      }
      writer.Finish();
      benchmark::ClobberMemory();
    }
    {
      BitmapWriterType writer(bitmap, 0, num_bits);
      for (int64_t i = 0; i < num_bits; i++) {
        writer.Clear();
        writer.Next();
      }
      writer.Finish();
      benchmark::ClobberMemory();
    }
  }
  state.SetBytesProcessed(2 * int64_t(state.iterations()) * nbytes);
}

static void BM_NaiveBitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<NaiveBitmapReader>(state, state.range(0));
}

static void BM_BitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<internal::BitmapReader>(state, state.range(0));
}

static void BM_NaiveBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<NaiveBitmapWriter>(state, state.range(0));
}

static void BM_BitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::BitmapWriter>(state, state.range(0));
}

static void BM_FirstTimeBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::FirstTimeBitmapWriter>(state, state.range(0));
}

static void BM_CopyBitmap(benchmark::State& state) {  // NOLINT non-const reference
  const int kBufferSize = static_cast<int>(state.range(0));
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(kBufferSize);

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

BENCHMARK(BM_NaiveBitmapReader)
    ->Args({100000})
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BitmapReader)->Args({100000})->MinTime(1.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_NaiveBitmapWriter)
    ->Args({100000})
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_BitmapWriter)->Args({100000})->MinTime(1.0)->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_FirstTimeBitmapWriter)
    ->Args({100000})
    ->MinTime(1.0)
    ->Unit(benchmark::kMicrosecond);

}  // namespace BitUtil
}  // namespace arrow
