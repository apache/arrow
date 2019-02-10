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
#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/test-random.h"
#include "arrow/test-util.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/cpu-info.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/sum.h"

namespace arrow {
namespace compute {

#include <cassert>
#include <cmath>
#include <iostream>
#include <random>

using internal::CpuInfo;
static CpuInfo* cpu_info = CpuInfo::GetInstance();

static const int64_t kL1Size = cpu_info->CacheSize(CpuInfo::L1_CACHE);
static const int64_t kL2Size = cpu_info->CacheSize(CpuInfo::L2_CACHE);
static const int64_t kL3Size = cpu_info->CacheSize(CpuInfo::L3_CACHE);

namespace BitUtil = arrow::BitUtil;
using arrow::internal::BitmapReader;

template <typename T>
struct SumState {
  using ValueType = T;

  SumState() : total(0), valid_count(0) {}

  T total = 0;
  int64_t valid_count = 0;
};

template <typename T>
struct Traits {};

template <>
struct Traits<int64_t> {
  using ArrayType = typename CTypeTraits<int64_t>::ArrayType;
  static constexpr int64_t null_sentinel = std::numeric_limits<int64_t>::lowest();

  static void FixSentinel(std::shared_ptr<ArrayType>& array) {
    auto data = array->data();
    for (int64_t i = 0; i < array->length(); i++)
      if (array->IsNull(i)) {
        int64_t* val_ptr = data->GetMutableValues<int64_t>(1, i);
        *val_ptr = null_sentinel;
      }
  }

  static inline bool IsNull(int64_t val) { return val == null_sentinel; }

  static inline bool NotNull(int64_t val) { return val != null_sentinel; }
};

template <typename T>
struct Summer {
 public:
  using ValueType = T;
  using ArrowType = typename CTypeTraits<T>::ArrowType;
};

template <typename T>
struct SumNoNulls : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    for (int64_t i = 0; i < array.length(); ++i) {
      local.total += values[i];
    }

    local.valid_count = array.length();
    *state = local;
  }
};

template <typename T>
struct SumNoNullsUnrolled : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    const auto length = array.length();
    const int64_t length_rounded = BitUtil::RoundDown(length, 8);
    for (int64_t i = 0; i < length_rounded; i += 8) {
      local.total += values[i + 0] + values[i + 1] + values[i + 2] + values[i + 3] +
                     values[i + 4] + values[i + 5] + values[i + 6] + values[i + 7];
    }

    for (int64_t i = length_rounded; i < length; ++i) {
      local.total += values[i];
    }

    local.valid_count = length;

    *state = local;
  }
};

template <typename T>
struct SumSentinel : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    const auto length = array.length();
    for (int64_t i = 0; i < length; i++) {
      // NaN is not equal to itself
      local.total += values[i] * Traits<T>::NotNull(values[i]);
      local.valid_count++;
    }

    *state = local;
  }
};

template <typename T>
struct SumSentinelUnrolled : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

#define SUM_NOT_NULL(ITEM)                                                  \
  do {                                                                      \
    local.total += values[i + ITEM] * Traits<T>::NotNull(values[i + ITEM]); \
    local.valid_count++;                                                    \
  } while (0)

    const auto values = array.raw_values();
    const auto length = array.length();
    const int64_t length_rounded = BitUtil::RoundDown(length, 8);
    for (int64_t i = 0; i < length_rounded; i += 8) {
      SUM_NOT_NULL(0);
      SUM_NOT_NULL(1);
      SUM_NOT_NULL(2);
      SUM_NOT_NULL(3);
      SUM_NOT_NULL(4);
      SUM_NOT_NULL(5);
      SUM_NOT_NULL(6);
      SUM_NOT_NULL(7);
    }

#undef SUM_NOT_NULL

    for (int64_t i = length_rounded * 8; i < length; ++i) {
      local.total += values[i] * Traits<T>::NotNull(values[i]);
      ++local.valid_count;
    }

    *state = local;
  }
};

template <typename T>
struct SumBitmapNaive : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    const auto bitmap = array.null_bitmap_data();
    const auto length = array.length();

    for (int64_t i = 0; i < length; ++i) {
      if (BitUtil::GetBit(bitmap, i)) {
        local.total += values[i];
        ++local.valid_count;
      }
    }

    *state = local;
  }
};

template <typename T>
struct SumBitmapReader : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    const auto bitmap = array.null_bitmap_data();
    const auto length = array.length();
    BitmapReader bit_reader(bitmap, 0, length);
    for (int64_t i = 0; i < length; ++i) {
      if (bit_reader.IsSet()) {
        local.total += values[i];
        ++local.valid_count;
      }

      bit_reader.Next();
    }

    *state = local;
  }
};

template <typename T>
struct SumBitmapVectorizeUnroll : public Summer<T> {
  using ArrayType = typename CTypeTraits<T>::ArrayType;

  static void Sum(const ArrayType& array, SumState<T>* state) {
    SumState<T> local;

    const auto values = array.raw_values();
    const auto bitmap = array.null_bitmap_data();
    const auto length = array.length();
    const int64_t length_rounded = BitUtil::RoundDown(length, 8);
    for (int64_t i = 0; i < length_rounded; i += 8) {
      const uint8_t valid_byte = bitmap[i / 8];

#define SUM_SHIFT(ITEM) (values[i + ITEM] * ((valid_byte >> ITEM) & 1))

      if (valid_byte < 0xFF) {
        // Some nulls
        local.total += SUM_SHIFT(0);
        local.total += SUM_SHIFT(1);
        local.total += SUM_SHIFT(2);
        local.total += SUM_SHIFT(3);
        local.total += SUM_SHIFT(4);
        local.total += SUM_SHIFT(5);
        local.total += SUM_SHIFT(6);
        local.total += SUM_SHIFT(7);
        local.valid_count += BitUtil::kBytePopcount[valid_byte];
      } else {
        // No nulls
        local.total += values[i + 0] + values[i + 1] + values[i + 2] + values[i + 3] +
                       values[i + 4] + values[i + 5] + values[i + 6] + values[i + 7];
        local.valid_count += 8;
      }
    }

#undef SUM_SHIFT

    for (int64_t i = length_rounded; i < length; ++i) {
      if (BitUtil::GetBit(bitmap, i)) {
        local.total = values[i];
        ++local.valid_count;
      }
    }

    *state = local;
  }
};

template <typename Functor>
void BenchSum(benchmark::State& state) {
  using T = typename Functor::ValueType;

  const int64_t array_size = state.range(0) / sizeof(int64_t);
  const double null_percent = static_cast<double>(state.range(1)) / 100.0;
  auto rand = random::RandomArrayGenerator(1923);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, null_percent));

  Traits<T>::FixSentinel(array);

  for (auto _ : state) {
    SumState<T> sum_state;
    Functor::Sum(*array, &sum_state);
    benchmark::DoNotOptimize(sum_state);
  }

  state.counters["size"] = static_cast<double>(state.range(0));
  state.counters["null_percent"] = static_cast<double>(state.range(1));
  state.SetBytesProcessed(state.iterations() * array_size * sizeof(T));
}

static void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->Unit(benchmark::kMicrosecond);

  for (auto size : {kL1Size, kL2Size, kL3Size, kL3Size * 4})
    for (auto nulls : std::vector<int64_t>({0, 1, 10, 50}))
      bench->Args({static_cast<int64_t>(size), nulls});
}

BENCHMARK_TEMPLATE(BenchSum, SumNoNulls<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumNoNullsUnrolled<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumSentinel<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumSentinelUnrolled<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumBitmapNaive<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumBitmapReader<int64_t>)->Apply(SetArgs);
BENCHMARK_TEMPLATE(BenchSum, SumBitmapVectorizeUnroll<int64_t>)->Apply(SetArgs);

static void BenchSumKernel(benchmark::State& state) {
  const int64_t array_size = state.range(0) / sizeof(int64_t);
  const double null_percent = static_cast<double>(state.range(1)) / 100.0;
  auto rand = random::RandomArrayGenerator(1923);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, null_percent));

  FunctionContext ctx;
  for (auto _ : state) {
    Datum out;
    ABORT_NOT_OK(Sum(&ctx, Datum(array), &out));
    benchmark::DoNotOptimize(out);
  }

  state.counters["size"] = static_cast<double>(state.range(0));
  state.counters["null_percent"] = static_cast<double>(state.range(1));
  state.SetBytesProcessed(state.iterations() * array_size * sizeof(int64_t));
}

BENCHMARK(BenchSumKernel)->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
