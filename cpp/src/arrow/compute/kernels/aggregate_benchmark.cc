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

#include "arrow/compute/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"

namespace arrow {
namespace compute {

#include <cassert>
#include <cmath>
#include <iostream>
#include <random>

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

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
void ReferenceSum(benchmark::State& state) {
  using T = typename Functor::ValueType;

  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(1923);
  auto array = std::static_pointer_cast<NumericArray<Int64Type>>(
      rand.Int64(array_size, -100, 100, args.null_proportion));

  Traits<T>::FixSentinel(array);

  for (auto _ : state) {
    SumState<T> sum_state;
    Functor::Sum(*array, &sum_state);
    benchmark::DoNotOptimize(sum_state);
  }
}

BENCHMARK_TEMPLATE(ReferenceSum, SumNoNulls<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumNoNullsUnrolled<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumSentinel<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumSentinelUnrolled<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumBitmapNaive<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumBitmapReader<int64_t>)->Apply(BenchmarkSetArgs);
BENCHMARK_TEMPLATE(ReferenceSum, SumBitmapVectorizeUnroll<int64_t>)
    ->Apply(BenchmarkSetArgs);
#endif  // ARROW_WITH_BENCHMARKS_REFERENCE

//
// Sum
//

template <typename ArrowType>
static void SumKernel(benchmark::State& state) {
  using CType = typename TypeTraits<ArrowType>::CType;

  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1923);
  auto array = rand.Numeric<ArrowType>(array_size, -100, 100, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Sum(array).status());
  }
}

static void SumKernelArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});  // 1M
}

#define SUM_KERNEL_BENCHMARK(FuncName, Type)                                \
  static void FuncName(benchmark::State& state) { SumKernel<Type>(state); } \
  BENCHMARK(FuncName)->Apply(SumKernelArgs)

SUM_KERNEL_BENCHMARK(SumKernelFloat, FloatType);
SUM_KERNEL_BENCHMARK(SumKernelDouble, DoubleType);
SUM_KERNEL_BENCHMARK(SumKernelInt8, Int8Type);
SUM_KERNEL_BENCHMARK(SumKernelInt16, Int16Type);
SUM_KERNEL_BENCHMARK(SumKernelInt32, Int32Type);
SUM_KERNEL_BENCHMARK(SumKernelInt64, Int64Type);

//
// Mode
//

template <typename ArrowType>
void ModeKernelBench(benchmark::State& state) {
  using CType = typename TypeTraits<ArrowType>::CType;

  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1924);
  auto array = rand.Numeric<ArrowType>(array_size, -100, 100, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Mode(array).status());
  }
}

template <>
void ModeKernelBench<BooleanType>(benchmark::State& state) {
  RegressionArgs args(state);
  auto rand = random::RandomArrayGenerator(1924);
  auto array = rand.Boolean(args.size * 8, 0.5, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Mode(array).status());
  }
}

static void ModeKernelBenchArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});  // 1M
}

#define MODE_KERNEL_BENCHMARK(FuncName, Type)                                     \
  static void FuncName(benchmark::State& state) { ModeKernelBench<Type>(state); } \
  BENCHMARK(FuncName)->Apply(ModeKernelBenchArgs)

MODE_KERNEL_BENCHMARK(ModeKernelBoolean, BooleanType);
MODE_KERNEL_BENCHMARK(ModeKernelInt8, Int8Type);
MODE_KERNEL_BENCHMARK(ModeKernelInt16, Int16Type);
MODE_KERNEL_BENCHMARK(ModeKernelInt32, Int32Type);
MODE_KERNEL_BENCHMARK(ModeKernelInt64, Int64Type);

//
// MinMax
//

template <typename ArrowType>
static void MinMaxKernelBench(benchmark::State& state) {
  using CType = typename TypeTraits<ArrowType>::CType;

  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1923);
  auto array = rand.Numeric<ArrowType>(array_size, -100, 100, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(MinMax(array).status());
  }
}

static void MinMaxKernelBenchArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});  // 1M
}

#define MINMAX_KERNEL_BENCHMARK(FuncName, Type)                                     \
  static void FuncName(benchmark::State& state) { MinMaxKernelBench<Type>(state); } \
  BENCHMARK(FuncName)->Apply(MinMaxKernelBenchArgs)

MINMAX_KERNEL_BENCHMARK(MinMaxKernelFloat, FloatType);
MINMAX_KERNEL_BENCHMARK(MinMaxKernelDouble, DoubleType);
MINMAX_KERNEL_BENCHMARK(MinMaxKernelInt8, Int8Type);
MINMAX_KERNEL_BENCHMARK(MinMaxKernelInt16, Int16Type);
MINMAX_KERNEL_BENCHMARK(MinMaxKernelInt32, Int32Type);
MINMAX_KERNEL_BENCHMARK(MinMaxKernelInt64, Int64Type);

//
// Count
//

static void CountKernelBenchInt64(benchmark::State& state) {
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(int64_t);
  auto rand = random::RandomArrayGenerator(1923);
  auto array = rand.Numeric<Int64Type>(array_size, -100, 100, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Count(array->Slice(1, array_size)).status());
  }
}
BENCHMARK(CountKernelBenchInt64)->Args({1 * 1024 * 1024, 2});  // 1M with 50% null.

//
// Variance
//

template <typename ArrowType>
void VarianceKernelBench(benchmark::State& state) {
  using CType = typename TypeTraits<ArrowType>::CType;

  VarianceOptions options;
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1925);
  auto array = rand.Numeric<ArrowType>(array_size, -100000, 100000, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Variance(array, options).status());
  }
}

static void VarianceKernelBenchArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});
}

#define VARIANCE_KERNEL_BENCHMARK(FuncName, Type)                                     \
  static void FuncName(benchmark::State& state) { VarianceKernelBench<Type>(state); } \
  BENCHMARK(FuncName)->Apply(VarianceKernelBenchArgs)

VARIANCE_KERNEL_BENCHMARK(VarianceKernelInt32, Int32Type);
VARIANCE_KERNEL_BENCHMARK(VarianceKernelInt64, Int64Type);
VARIANCE_KERNEL_BENCHMARK(VarianceKernelFloat, FloatType);
VARIANCE_KERNEL_BENCHMARK(VarianceKernelDouble, DoubleType);

//
// Quantile
//

template <typename ArrowType>
void QuantileKernelBench(benchmark::State& state) {
  using CType = typename TypeTraits<ArrowType>::CType;

  QuantileOptions options;
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1926);
  auto array = rand.Numeric<ArrowType>(array_size, -30000, 30000, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Quantile(array, options).status());
  }
}

static void QuantileKernelBenchArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});
}

#define QUANTILE_KERNEL_BENCHMARK(FuncName, Type)                                     \
  static void FuncName(benchmark::State& state) { QuantileKernelBench<Type>(state); } \
  BENCHMARK(FuncName)->Apply(QuantileKernelBenchArgs)

QUANTILE_KERNEL_BENCHMARK(QuantileKernelInt32, Int32Type);
QUANTILE_KERNEL_BENCHMARK(QuantileKernelInt64, Int64Type);
QUANTILE_KERNEL_BENCHMARK(QuantileKernelDouble, DoubleType);

}  // namespace compute
}  // namespace arrow
