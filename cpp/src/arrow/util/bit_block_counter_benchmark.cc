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
#include <cstdlib>
#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/testing/random.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {
namespace internal {

struct UnaryBitBlockBenchmark {
  benchmark::State& state;
  int64_t offset;
  int64_t bitmap_length;
  std::shared_ptr<Array> arr;
  int64_t expected;

  explicit UnaryBitBlockBenchmark(benchmark::State& state, int64_t offset = 0)
      : state(state), offset(offset), bitmap_length(1 << 20) {
    random::RandomArrayGenerator rng(/*seed=*/0);
    // State parameter is the average number of total values for each null
    // value. So 100 means that 1 out of 100 on average are null.
    double null_probability = 1. / static_cast<double>(state.range(0));
    arr = rng.Int8(bitmap_length, 0, 100, null_probability);

    // Compute the expected result
    this->expected = 0;
    const auto& int8_arr = static_cast<const Int8Array&>(*arr);
    for (int64_t i = this->offset; i < bitmap_length; ++i) {
      if (int8_arr.IsValid(i)) {
        this->expected += int8_arr.Value(i);
      }
    }
  }

  template <typename NextBlockFunc>
  void BenchBitBlockCounter(NextBlockFunc&& next_block) {
    const auto& int8_arr = static_cast<const Int8Array&>(*arr);
    const uint8_t* bitmap = arr->null_bitmap_data();
    for (auto _ : state) {
      BitBlockCounter scanner(bitmap, this->offset, bitmap_length - this->offset);
      int64_t result = 0;
      int64_t position = this->offset;
      while (true) {
        BitBlockCount block = next_block(&scanner);
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
            if (bit_util::GetBit(bitmap, position + i)) {
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

  void BenchBitmapReader() {
    const auto& int8_arr = static_cast<const Int8Array&>(*arr);
    for (auto _ : state) {
      internal::BitmapReader bit_reader(arr->null_bitmap_data(), this->offset,
                                        bitmap_length - this->offset);
      int64_t result = 0;
      for (int64_t i = this->offset; i < bitmap_length; ++i) {
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
};

struct BinaryBitBlockBenchmark {
  benchmark::State& state;
  int64_t offset;
  int64_t bitmap_length;
  std::shared_ptr<Array> left;
  std::shared_ptr<Array> right;
  int64_t expected;
  const Int8Array* left_int8;
  const Int8Array* right_int8;

  explicit BinaryBitBlockBenchmark(benchmark::State& state, int64_t offset = 0)
      : state(state), offset(offset), bitmap_length(1 << 20) {
    random::RandomArrayGenerator rng(/*seed=*/0);

    // State parameter is the average number of total values for each null
    // value. So 100 means that 1 out of 100 on average are null.
    double null_probability = 1. / static_cast<double>(state.range(0));
    left = rng.Int8(bitmap_length, 0, 100, null_probability);
    right = rng.Int8(bitmap_length, 0, 50, null_probability);
    left_int8 = static_cast<const Int8Array*>(left.get());
    right_int8 = static_cast<const Int8Array*>(right.get());

    // Compute the expected result
    expected = 0;
    for (int64_t i = this->offset; i < bitmap_length; ++i) {
      if (left_int8->IsValid(i) && right_int8->IsValid(i)) {
        expected += left_int8->Value(i) + right_int8->Value(i);
      }
    }
  }

  void BenchBitBlockCounter() {
    const uint8_t* left_bitmap = left->null_bitmap_data();
    const uint8_t* right_bitmap = right->null_bitmap_data();
    for (auto _ : state) {
      BinaryBitBlockCounter scanner(left_bitmap, this->offset, right_bitmap, this->offset,
                                    bitmap_length - this->offset);
      int64_t result = 0;
      int64_t position = this->offset;
      while (true) {
        BitBlockCount block = scanner.NextAndWord();
        if (block.length == 0) {
          break;
        }
        if (block.length == block.popcount) {
          // All not-null
          for (int64_t i = 0; i < block.length; ++i) {
            result += left_int8->Value(position + i) + right_int8->Value(position + i);
          }
        } else if (block.popcount > 0) {
          // Some but not all not-null
          for (int64_t i = 0; i < block.length; ++i) {
            if (bit_util::GetBit(left_bitmap, position + i) &&
                bit_util::GetBit(right_bitmap, position + i)) {
              result += left_int8->Value(position + i) + right_int8->Value(position + i);
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

  void BenchBitmapReader() {
    for (auto _ : state) {
      internal::BitmapReader left_reader(left->null_bitmap_data(), this->offset,
                                         bitmap_length - this->offset);
      internal::BitmapReader right_reader(right->null_bitmap_data(), this->offset,
                                          bitmap_length - this->offset);
      int64_t result = 0;
      for (int64_t i = this->offset; i < bitmap_length; ++i) {
        if (left_reader.IsSet() && right_reader.IsSet()) {
          result += left_int8->Value(i) + right_int8->Value(i);
        }
        left_reader.Next();
        right_reader.Next();
      }
      // Sanity check
      if (result != expected) {
        std::abort();
      }
    }
    state.SetItemsProcessed(state.iterations() * bitmap_length);
  }
};

static void BitBlockCounterSum(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/0)
      .BenchBitBlockCounter([](BitBlockCounter* counter) { return counter->NextWord(); });
}

static void BitBlockCounterSumWithOffset(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/4)
      .BenchBitBlockCounter([](BitBlockCounter* counter) { return counter->NextWord(); });
}

static void BitBlockCounterFourWordsSum(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/0)
      .BenchBitBlockCounter(
          [](BitBlockCounter* counter) { return counter->NextFourWords(); });
}

static void BitBlockCounterFourWordsSumWithOffset(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/4)
      .BenchBitBlockCounter(
          [](BitBlockCounter* counter) { return counter->NextFourWords(); });
}

static void BitmapReaderSum(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/0).BenchBitmapReader();
}

static void BitmapReaderSumWithOffset(benchmark::State& state) {
  UnaryBitBlockBenchmark(state, /*offset=*/4).BenchBitmapReader();
}

static void BinaryBitBlockCounterSum(benchmark::State& state) {
  BinaryBitBlockBenchmark(state, /*offset=*/0).BenchBitBlockCounter();
}

static void BinaryBitBlockCounterSumWithOffset(benchmark::State& state) {
  BinaryBitBlockBenchmark(state, /*offset=*/4).BenchBitBlockCounter();
}

static void BinaryBitmapReaderSum(benchmark::State& state) {
  BinaryBitBlockBenchmark(state, /*offset=*/0).BenchBitmapReader();
}

static void BinaryBitmapReaderSumWithOffset(benchmark::State& state) {
  BinaryBitBlockBenchmark(state, /*offset=*/4).BenchBitmapReader();
}

// Range value: average number of total values per null
BENCHMARK(BitBlockCounterSum)->Range(2, 1 << 16);
BENCHMARK(BitBlockCounterSumWithOffset)->Range(2, 1 << 16);
BENCHMARK(BitBlockCounterFourWordsSum)->Range(2, 1 << 16);
BENCHMARK(BitBlockCounterFourWordsSumWithOffset)->Range(2, 1 << 16);
BENCHMARK(BitmapReaderSum)->Range(2, 1 << 16);
BENCHMARK(BitmapReaderSumWithOffset)->Range(2, 1 << 16);
BENCHMARK(BinaryBitBlockCounterSum)->Range(2, 1 << 16);
BENCHMARK(BinaryBitBlockCounterSumWithOffset)->Range(2, 1 << 16);
BENCHMARK(BinaryBitmapReaderSum)->Range(2, 1 << 16);
BENCHMARK(BinaryBitmapReaderSumWithOffset)->Range(2, 1 << 16);

}  // namespace internal
}  // namespace arrow
