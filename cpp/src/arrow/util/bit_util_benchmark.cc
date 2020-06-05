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
#include <array>
#include <bitset>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap.h"
#include "arrow/util/bitmap_generate.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/bitmap_visit.h"
#include "arrow/util/bitmap_writer.h"

namespace arrow {

namespace BitUtil {

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

// A naive bitmap reader implementation, meant as a baseline against
// internal::BitmapReader

class NaiveBitmapReader {
 public:
  NaiveBitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0) {}

  bool IsSet() const { return BitUtil::GetBit(bitmap_, position_); }

  bool IsNotSet() const { return !IsSet(); }

  void Next() { ++position_; }

 private:
  const uint8_t* bitmap_;
  uint64_t position_;
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
    auto bit_set_mask = (1U << bit_offset);
    bitmap_[byte_offset] = static_cast<uint8_t>(bitmap_[byte_offset] | bit_set_mask);
  }

  void Clear() {
    const int64_t byte_offset = position_ / 8;
    const int64_t bit_offset = position_ % 8;
    auto bit_clear_mask = 0xFFU ^ (1U << bit_offset);
    bitmap_[byte_offset] = static_cast<uint8_t>(bitmap_[byte_offset] & bit_clear_mask);
  }

  void Next() { ++position_; }

  void Finish() {}

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
};

#endif

static std::shared_ptr<Buffer> CreateRandomBuffer(int64_t nbytes) {
  auto buffer = *AllocateBuffer(nbytes);
  memset(buffer->mutable_data(), 0, nbytes);
  random_bytes(nbytes, 0, buffer->mutable_data());
  return std::move(buffer);
}

template <typename DoAnd>
static void BenchmarkAndImpl(benchmark::State& state, DoAnd&& do_and) {
  int64_t nbytes = state.range(0);
  int64_t offset = state.range(1);

  std::shared_ptr<Buffer> buffer_1 = CreateRandomBuffer(nbytes);
  std::shared_ptr<Buffer> buffer_2 = CreateRandomBuffer(nbytes);
  std::shared_ptr<Buffer> buffer_3 = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8 - offset;

  internal::Bitmap bitmap_1{buffer_1, 0, num_bits};
  internal::Bitmap bitmap_2{buffer_2, offset, num_bits};
  internal::Bitmap bitmap_3{buffer_3, 0, num_bits};

  for (auto _ : state) {
    do_and({bitmap_1, bitmap_2}, &bitmap_3);
    auto total = internal::CountSetBits(bitmap_3.buffer()->data(), bitmap_3.offset(),
                                        bitmap_3.length());
    benchmark::DoNotOptimize(total);
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

static void BenchmarkBitmapAnd(benchmark::State& state) {
  BenchmarkAndImpl(state, [](const internal::Bitmap(&bitmaps)[2], internal::Bitmap* out) {
    internal::BitmapAnd(bitmaps[0].buffer()->data(), bitmaps[0].offset(),
                        bitmaps[1].buffer()->data(), bitmaps[1].offset(),
                        bitmaps[0].length(), 0, out->buffer()->mutable_data());
  });
}

static void BenchmarkBitmapVisitBitsetAnd(benchmark::State& state) {
  BenchmarkAndImpl(state, [](const internal::Bitmap(&bitmaps)[2], internal::Bitmap* out) {
    int64_t i = 0;
    internal::Bitmap::VisitBits(
        bitmaps, [&](std::bitset<2> bits) { out->SetBitTo(i++, bits[0] && bits[1]); });
  });
}

static void BenchmarkBitmapVisitUInt8And(benchmark::State& state) {
  BenchmarkAndImpl(state, [](const internal::Bitmap(&bitmaps)[2], internal::Bitmap* out) {
    int64_t i = 0;
    internal::Bitmap::VisitWords(bitmaps, [&](std::array<uint8_t, 2> uint8s) {
      reinterpret_cast<uint8_t*>(out->buffer()->mutable_data())[i++] =
          uint8s[0] & uint8s[1];
    });
  });
}

static void BenchmarkBitmapVisitUInt64And(benchmark::State& state) {
  BenchmarkAndImpl(state, [](const internal::Bitmap(&bitmaps)[2], internal::Bitmap* out) {
    int64_t i = 0;
    internal::Bitmap::VisitWords(bitmaps, [&](std::array<uint64_t, 2> uint64s) {
      reinterpret_cast<uint64_t*>(out->buffer()->mutable_data())[i++] =
          uint64s[0] & uint64s[1];
    });
  });
}

template <typename BitmapReaderType>
static void BenchmarkBitmapReader(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  const uint8_t* bitmap = buffer->data();

  for (auto _ : state) {
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
  state.SetBytesProcessed(2LL * state.iterations() * nbytes);
}

template <typename VisitBitsFunctorType>
static void BenchmarkVisitBits(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  const uint8_t* bitmap = buffer->data();

  for (auto _ : state) {
    {
      int64_t total = 0;
      const auto visit = [&total](bool value) -> void { total += value; };
      VisitBitsFunctorType()(bitmap, 0, num_bits, visit);
      benchmark::DoNotOptimize(total);
    }
    {
      int64_t total = 0;
      const auto visit = [&total](bool value) -> void { total += value; };
      VisitBitsFunctorType()(bitmap, 0, num_bits, visit);
      benchmark::DoNotOptimize(total);
    }
  }
  state.SetBytesProcessed(2LL * state.iterations() * nbytes);
}

constexpr bool pattern[] = {false, false, false, true, true, true};
static_assert(
    (sizeof(pattern) / sizeof(pattern[0])) % 8 != 0,
    "pattern must not be a multiple of 8, otherwise gcc can optimize with a memset");

template <typename BitmapWriterType>
static void BenchmarkBitmapWriter(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  uint8_t* bitmap = buffer->mutable_data();

  for (auto _ : state) {
    BitmapWriterType writer(bitmap, 0, num_bits);
    int64_t pattern_index = 0;
    for (int64_t i = 0; i < num_bits; i++) {
      if (pattern[pattern_index++]) {
        writer.Set();
      } else {
        writer.Clear();
      }
      if (pattern_index == sizeof(pattern) / sizeof(bool)) {
        pattern_index = 0;
      }
      writer.Next();
    }
    writer.Finish();
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

template <typename GenerateBitsFunctorType>
static void BenchmarkGenerateBits(benchmark::State& state, int64_t nbytes) {
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  const int64_t num_bits = nbytes * 8;
  uint8_t* bitmap = buffer->mutable_data();

  while (state.KeepRunning()) {
    int64_t pattern_index = 0;
    const auto generate = [&]() -> bool {
      bool b = pattern[pattern_index++];
      if (pattern_index == sizeof(pattern) / sizeof(bool)) {
        pattern_index = 0;
      }
      return b;
    };
    GenerateBitsFunctorType()(bitmap, 0, num_bits, generate);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

static void BitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<internal::BitmapReader>(state, state.range(0));
}

static void BitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::BitmapWriter>(state, state.range(0));
}

static void FirstTimeBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<internal::FirstTimeBitmapWriter>(state, state.range(0));
}

struct GenerateBitsFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBits(bitmap, start_offset, length, g);
  }
};

struct GenerateBitsUnrolledFunctor {
  template <class Generator>
  void operator()(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
    return internal::GenerateBitsUnrolled(bitmap, start_offset, length, g);
  }
};

struct VisitBitsFunctor {
  template <class Visitor>
  void operator()(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                  Visitor&& g) {
    return internal::VisitBits(bitmap, start_offset, length, g);
  }
};

struct VisitBitsUnrolledFunctor {
  template <class Visitor>
  void operator()(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                  Visitor&& g) {
    return internal::VisitBitsUnrolled(bitmap, start_offset, length, g);
  }
};

static void GenerateBits(benchmark::State& state) {
  BenchmarkGenerateBits<GenerateBitsFunctor>(state, state.range(0));
}

static void GenerateBitsUnrolled(benchmark::State& state) {
  BenchmarkGenerateBits<GenerateBitsUnrolledFunctor>(state, state.range(0));
}

static void VisitBits(benchmark::State& state) {
  BenchmarkVisitBits<VisitBitsFunctor>(state, state.range(0));
}

static void VisitBitsUnrolled(benchmark::State& state) {
  BenchmarkVisitBits<VisitBitsUnrolledFunctor>(state, state.range(0));
}

static void SetBitsTo(benchmark::State& state) {
  int64_t nbytes = state.range(0);
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(nbytes);

  for (auto _ : state) {
    BitUtil::SetBitsTo(buffer->mutable_data(), /*offset=*/0, nbytes * 8, true);
  }
  state.SetBytesProcessed(state.iterations() * nbytes);
}

constexpr int64_t kBufferSize = 1024 * 8;

template <int64_t OffsetSrc, int64_t OffsetDest = 0>
static void CopyBitmap(benchmark::State& state) {  // NOLINT non-const reference
  const int64_t buffer_size = state.range(0);
  const int64_t bits_size = buffer_size * 8;
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(buffer_size);

  const uint8_t* src = buffer->data();
  const int64_t length = bits_size - OffsetSrc;

  auto copy = *AllocateEmptyBitmap(length);

  for (auto _ : state) {
    internal::CopyBitmap(src, OffsetSrc, length, copy->mutable_data(), OffsetDest, false);
  }

  state.SetBytesProcessed(state.iterations() * buffer_size);
}

static void CopyBitmapWithoutOffset(
    benchmark::State& state) {  // NOLINT non-const reference
  CopyBitmap<0>(state);
}

// Trigger the slow path where the source buffer is not byte aligned.
static void CopyBitmapWithOffset(benchmark::State& state) {  // NOLINT non-const reference
  CopyBitmap<4>(state);
}

// Trigger the slow path where both source and dest buffer are not byte aligend.
static void CopyBitmapWithOffsetBoth(benchmark::State& state) { CopyBitmap<3, 7>(state); }

// Benchmark the worst case of comparing two identical bitmap
template <int64_t Offset = 0>
static void BitmapEquals(benchmark::State& state) {
  const int64_t buffer_size = state.range(0);
  const int64_t bits_size = buffer_size * 8;
  std::shared_ptr<Buffer> buffer = CreateRandomBuffer(buffer_size);

  const uint8_t* src = buffer->data();
  const int64_t offset = Offset;
  const int64_t length = bits_size - offset;

  auto copy = *AllocateEmptyBitmap(length + offset);
  internal::CopyBitmap(src, 0, length, copy->mutable_data(), offset, false);

  for (auto _ : state) {
    auto is_same = internal::BitmapEquals(src, 0, copy->data(), offset, length);
    benchmark::DoNotOptimize(is_same);
  }

  state.SetBytesProcessed(state.iterations() * buffer_size);
}

template <int64_t Offset = 0>
static void BitBlockCounterSumNotNull(benchmark::State& state) {
  using internal::BitBlockCounter;

  random::RandomArrayGenerator rng(/*seed=*/0);

  const int64_t bitmap_length = 1 << 20;

  // State parameter is the average number of total values for each false
  // value. So 100 means that 1 out of 100 on average are false.
  double true_probability = 1. - 1. / state.range(0);
  auto arr = rng.Int8(bitmap_length, 0, 100, true_probability);

  const uint8_t* bitmap = arr->null_bitmap_data();

  // Compute the expected result
  int64_t expected = 0;
  const auto& int8_arr = static_cast<const Int8Array&>(*arr);
  for (int64_t i = Offset; i < bitmap_length; ++i) {
    if (int8_arr.IsValid(i)) {
      expected += int8_arr.Value(i);
    }
  }
  for (auto _ : state) {
    BitBlockCounter scanner(bitmap, Offset, bitmap_length - Offset);
    int64_t result = 0;
    int64_t position = Offset;
    while (true) {
      BitBlockCounter::Block block = scanner.NextBlock();
      if (block.length == 0) {
        break;
      }
      if (block.length == block.popcount) {
        // All not-null
        for (int64_t i = 0; i < block.length; ++i) {
          result += int8_arr.Value(position + i);
        }
      } else if (block.popcount > 0) {
        // Some but not all not-null
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(bitmap, position + i)) {
            result += int8_arr.Value(position + i);
          }
        }
      }
      position += block.length;
    }
    // Sanity check
    if (result != expected) {
      std::abort();
    }
  }
  state.SetItemsProcessed(state.iterations() * bitmap_length);
}

template <int64_t Offset = 0>
static void BitmapReaderSumNotNull(benchmark::State& state) {
  random::RandomArrayGenerator rng(/*seed=*/0);

  const int64_t bitmap_length = 1 << 20;

  // State parameter is the average number of total values for each false
  // value. So 100 means that 1 out of 100 on average are false.
  double true_probability = 1. - 1. / state.range(0);
  auto arr = rng.Int8(bitmap_length, 0, 100, true_probability);

  const uint8_t* bitmap = arr->null_bitmap_data();
  // Compute the expected result
  int64_t expected = 0;
  const auto& int8_arr = static_cast<const Int8Array&>(*arr);
  for (int64_t i = Offset; i < bitmap_length; ++i) {
    if (int8_arr.IsValid(i)) {
      expected += int8_arr.Value(i);
    }
  }
  for (auto _ : state) {
    internal::BitmapReader bit_reader(bitmap, Offset, bitmap_length - Offset);
    int64_t result = 0;
    for (int64_t i = Offset; i < bitmap_length; ++i) {
      if (bit_reader.IsSet()) {
        result += int8_arr.Value(i);
      }
      bit_reader.Next();
    }
    // Sanity check
    if (result != expected) {
      std::abort();
    }
  }
  state.SetItemsProcessed(state.iterations() * bitmap_length);
}

static void BitBlockCounterSumNotNull(benchmark::State& state) {
  BitBlockCounterSumNotNull<0>(state);
}

static void BitBlockCounterSumNotNullWithOffset(benchmark::State& state) {
  BitBlockCounterSumNotNull<4>(state);
}

static void BitmapReaderSumNotNull(benchmark::State& state) {
  BitmapReaderSumNotNull<0>(state);
}

static void BitmapReaderSumNotNullWithOffset(benchmark::State& state) {
  BitmapReaderSumNotNull<4>(state);
}

static void BitmapEqualsWithoutOffset(benchmark::State& state) { BitmapEquals<0>(state); }

static void BitmapEqualsWithOffset(benchmark::State& state) { BitmapEquals<4>(state); }

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
static void ReferenceNaiveBitmapReader(benchmark::State& state) {
  BenchmarkBitmapReader<NaiveBitmapReader>(state, state.range(0));
}

BENCHMARK(ReferenceNaiveBitmapReader)->Arg(kBufferSize);
#endif

BENCHMARK(BitmapReader)->Arg(kBufferSize);
BENCHMARK(VisitBits)->Arg(kBufferSize);
BENCHMARK(VisitBitsUnrolled)->Arg(kBufferSize);
BENCHMARK(SetBitsTo)->Arg(2)->Arg(1 << 4)->Arg(1 << 10)->Arg(1 << 17);

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE
static void ReferenceNaiveBitmapWriter(benchmark::State& state) {
  BenchmarkBitmapWriter<NaiveBitmapWriter>(state, state.range(0));
}

BENCHMARK(ReferenceNaiveBitmapWriter)->Arg(kBufferSize);
#endif

BENCHMARK(BitmapWriter)->Arg(kBufferSize);
BENCHMARK(FirstTimeBitmapWriter)->Arg(kBufferSize);
BENCHMARK(GenerateBits)->Arg(kBufferSize);
BENCHMARK(GenerateBitsUnrolled)->Arg(kBufferSize);

BENCHMARK(CopyBitmapWithoutOffset)->Arg(kBufferSize);
BENCHMARK(CopyBitmapWithOffset)->Arg(kBufferSize);
BENCHMARK(CopyBitmapWithOffsetBoth)->Arg(kBufferSize);

BENCHMARK(BitmapEqualsWithoutOffset)->Arg(kBufferSize);
BENCHMARK(BitmapEqualsWithOffset)->Arg(kBufferSize);

// Range value: average number of total values per null
BENCHMARK(BitBlockCounterSumNotNull)->Range(8, 1 << 16);
BENCHMARK(BitBlockCounterSumNotNullWithOffset)->Range(8, 1 << 16);
BENCHMARK(BitmapReaderSumNotNull)->Range(8, 1 << 16);
BENCHMARK(BitmapReaderSumNotNullWithOffset)->Range(8, 1 << 16);

#define AND_BENCHMARK_RANGES                      \
  {                                               \
    {kBufferSize * 4, kBufferSize * 16}, { 0, 2 } \
  }
BENCHMARK(BenchmarkBitmapAnd)->Ranges(AND_BENCHMARK_RANGES);
BENCHMARK(BenchmarkBitmapVisitBitsetAnd)->Ranges(AND_BENCHMARK_RANGES);
BENCHMARK(BenchmarkBitmapVisitUInt8And)->Ranges(AND_BENCHMARK_RANGES);
BENCHMARK(BenchmarkBitmapVisitUInt64And)->Ranges(AND_BENCHMARK_RANGES);

}  // namespace BitUtil
}  // namespace arrow
