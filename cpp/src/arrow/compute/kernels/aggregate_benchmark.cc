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

#include "arrow/array/array_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/exec/groupby.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/string.h"

namespace arrow {
namespace compute {

#include <cassert>
#include <cmath>
#include <iostream>
#include <random>

using arrow::internal::ToChars;

#ifdef ARROW_WITH_BENCHMARKS_REFERENCE

namespace bit_util = arrow::bit_util;
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
    const int64_t length_rounded = bit_util::RoundDown(length, 8);
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
    const int64_t length_rounded = bit_util::RoundDown(length, 8);
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
      if (bit_util::GetBit(bitmap, i)) {
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
    const int64_t length_rounded = bit_util::RoundDown(length, 8);
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
        local.valid_count += bit_util::kBytePopcount[valid_byte];
      } else {
        // No nulls
        local.total += values[i + 0] + values[i + 1] + values[i + 2] + values[i + 3] +
                       values[i + 4] + values[i + 5] + values[i + 6] + values[i + 7];
        local.valid_count += 8;
      }
    }

#undef SUM_SHIFT

    for (int64_t i = length_rounded; i < length; ++i) {
      if (bit_util::GetBit(bitmap, i)) {
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
// GroupBy
//

std::shared_ptr<RecordBatch> RecordBatchFromArrays(
    const std::vector<std::shared_ptr<Array>>& arguments,
    const std::vector<std::shared_ptr<Array>>& keys) {
  std::vector<std::shared_ptr<Field>> fields;
  std::vector<std::shared_ptr<Array>> all_arrays;
  int64_t length = -1;
  if (arguments.empty()) {
    DCHECK(!keys.empty());
    length = keys[0]->length();
  } else {
    length = arguments[0]->length();
  }
  for (std::size_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
    const auto& arg = arguments[arg_idx];
    DCHECK_EQ(arg->length(), length);
    fields.push_back(field("arg" + ToChars(arg_idx), arg->type()));
    all_arrays.push_back(arg);
  }
  for (std::size_t key_idx = 0; key_idx < keys.size(); key_idx++) {
    const auto& key = keys[key_idx];
    DCHECK_EQ(key->length(), length);
    fields.push_back(field("key" + ToChars(key_idx), key->type()));
    all_arrays.push_back(key);
  }
  return RecordBatch::Make(schema(std::move(fields)), length, std::move(all_arrays));
}

static void BenchmarkGroupBy(benchmark::State& state, std::vector<Aggregate> aggregates,
                             const std::vector<std::shared_ptr<Array>>& arguments,
                             const std::vector<std::shared_ptr<Array>>& keys) {
  std::shared_ptr<RecordBatch> batch = RecordBatchFromArrays(arguments, keys);
  std::vector<FieldRef> key_refs;
  for (std::size_t key_idx = 0; key_idx < keys.size(); key_idx++) {
    key_refs.emplace_back(static_cast<int>(key_idx + arguments.size()));
  }
  for (std::size_t arg_idx = 0; arg_idx < arguments.size(); arg_idx++) {
    aggregates[arg_idx].target = {FieldRef(static_cast<int>(arg_idx))};
  }
  for (auto _ : state) {
    ABORT_NOT_OK(BatchGroupBy(batch, aggregates, key_refs));
  }
}

#define GROUP_BY_BENCHMARK(Name, Impl)                               \
  static void Name(benchmark::State& state) {                        \
    RegressionArgs args(state, false);                               \
    auto rng = random::RandomArrayGenerator(1923);                   \
    (Impl)();                                                        \
  }                                                                  \
  BENCHMARK(Name)->Apply([](benchmark::internal::Benchmark* bench) { \
    BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});             \
  })

// Grouped Sum

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/16,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/256,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumStringSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.StringWithRepeats(args.size,
                                   /*unique=*/4096,
                                   /*min_length=*/3,
                                   /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/15);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/255);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumIntegerSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto key = rng.Int64(args.size,
                       /*min=*/0,
                       /*max=*/4095);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByTinyIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/4);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/4,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedBySmallIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/15);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/16,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

GROUP_BY_BENCHMARK(SumDoublesGroupedByMediumIntStringPairSet, [&] {
  auto summand = rng.Float64(args.size,
                             /*min=*/0.0,
                             /*max=*/1.0e14,
                             /*null_probability=*/args.null_proportion,
                             /*nan_probability=*/args.null_proportion / 10);

  auto int_key = rng.Int64(args.size,
                           /*min=*/0,
                           /*max=*/63);
  auto str_key = rng.StringWithRepeats(args.size,
                                       /*unique=*/64,
                                       /*min_length=*/3,
                                       /*max_length=*/32);

  BenchmarkGroupBy(state, {{"hash_sum", ""}}, {summand}, {int_key, str_key});
});

// Grouped MinMax

GROUP_BY_BENCHMARK(MinMaxDoublesGroupedByMediumInt, [&] {
  auto input = rng.Float64(args.size,
                           /*min=*/0.0,
                           /*max=*/1.0e14,
                           /*null_probability=*/args.null_proportion,
                           /*nan_probability=*/args.null_proportion / 10);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

GROUP_BY_BENCHMARK(MinMaxShortStringsGroupedByMediumInt, [&] {
  auto input = rng.String(args.size,
                          /*min_length=*/0,
                          /*max_length=*/64,
                          /*null_probability=*/args.null_proportion);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

GROUP_BY_BENCHMARK(MinMaxLongStringsGroupedByMediumInt, [&] {
  auto input = rng.String(args.size,
                          /*min_length=*/0,
                          /*max_length=*/512,
                          /*null_probability=*/args.null_proportion);
  auto int_key = rng.Int64(args.size, /*min=*/0, /*max=*/63);

  BenchmarkGroupBy(state, {{"hash_min_max", ""}}, {input}, {int_key});
});

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
void ModeKernel(benchmark::State& state, int min, int max) {
  using CType = typename TypeTraits<ArrowType>::CType;

  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1924);
  auto array = rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Mode(array).status());
  }
}

template <typename ArrowType>
void ModeKernelNarrow(benchmark::State& state) {
  ModeKernel<ArrowType>(state, -5000, 8000);  // max - min < 16384
}

template <>
void ModeKernelNarrow<Int8Type>(benchmark::State& state) {
  ModeKernel<Int8Type>(state, -128, 127);
}

template <>
void ModeKernelNarrow<BooleanType>(benchmark::State& state) {
  RegressionArgs args(state);
  auto rand = random::RandomArrayGenerator(1924);
  auto array = rand.Boolean(args.size * 8, 0.5, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Mode(array).status());
  }
}

template <typename ArrowType>
void ModeKernelWide(benchmark::State& state) {
  ModeKernel<ArrowType>(state, -1234567, 7654321);
}

static void ModeKernelArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});  // 1M
}

BENCHMARK_TEMPLATE(ModeKernelNarrow, BooleanType)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelNarrow, Int8Type)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelNarrow, Int32Type)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelNarrow, Int64Type)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelWide, Int32Type)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelWide, Int64Type)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelWide, FloatType)->Apply(ModeKernelArgs);
BENCHMARK_TEMPLATE(ModeKernelWide, DoubleType)->Apply(ModeKernelArgs);

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

static std::vector<double> deciles() {
  return {0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
}

static std::vector<double> centiles() {
  std::vector<double> q(101);
  for (int i = 0; i <= 100; ++i) {
    q[i] = i / 100.0;
  }
  return q;
}

template <typename ArrowType>
void QuantileKernel(benchmark::State& state, int min, int max, std::vector<double> q) {
  using CType = typename TypeTraits<ArrowType>::CType;

  QuantileOptions options(std::move(q));
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(CType);
  auto rand = random::RandomArrayGenerator(1926);
  auto array = rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(Quantile(array, options).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

template <typename ArrowType>
void QuantileKernelMedian(benchmark::State& state, int min, int max) {
  QuantileKernel<ArrowType>(state, min, max, {0.5});
}

template <typename ArrowType>
void QuantileKernelMedianWide(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, 0, 1 << 24, {0.5});
}

template <typename ArrowType>
void QuantileKernelMedianNarrow(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, -30000, 30000, {0.5});
}

template <typename ArrowType>
void QuantileKernelDecilesWide(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, 0, 1 << 24, deciles());
}

template <typename ArrowType>
void QuantileKernelDecilesNarrow(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, -30000, 30000, deciles());
}

template <typename ArrowType>
void QuantileKernelCentilesWide(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, 0, 1 << 24, centiles());
}

template <typename ArrowType>
void QuantileKernelCentilesNarrow(benchmark::State& state) {
  QuantileKernel<ArrowType>(state, -30000, 30000, centiles());
}

static void QuantileKernelArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 * 1024 * 1024});
}

BENCHMARK_TEMPLATE(QuantileKernelMedianNarrow, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelMedianWide, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelMedianNarrow, Int64Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelMedianWide, Int64Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelMedianWide, DoubleType)->Apply(QuantileKernelArgs);

BENCHMARK_TEMPLATE(QuantileKernelDecilesNarrow, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelDecilesWide, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelDecilesWide, DoubleType)->Apply(QuantileKernelArgs);

BENCHMARK_TEMPLATE(QuantileKernelCentilesNarrow, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelCentilesWide, Int32Type)->Apply(QuantileKernelArgs);
BENCHMARK_TEMPLATE(QuantileKernelCentilesWide, DoubleType)->Apply(QuantileKernelArgs);

static void TDigestKernelDouble(benchmark::State& state, std::vector<double> q) {
  TDigestOptions options{std::move(q)};
  RegressionArgs args(state);
  const int64_t array_size = args.size / sizeof(double);
  auto rand = random::RandomArrayGenerator(1926);
  auto array = rand.Numeric<DoubleType>(array_size, 0, 1 << 24, args.null_proportion);

  for (auto _ : state) {
    ABORT_NOT_OK(TDigest(array, options).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

static void TDigestKernelDoubleMedian(benchmark::State& state) {
  TDigestKernelDouble(state, {0.5});
}

static void TDigestKernelDoubleDeciles(benchmark::State& state) {
  TDigestKernelDouble(state, deciles());
}

static void TDigestKernelDoubleCentiles(benchmark::State& state) {
  TDigestKernelDouble(state, centiles());
}

BENCHMARK(TDigestKernelDoubleMedian)->Apply(QuantileKernelArgs);
BENCHMARK(TDigestKernelDoubleDeciles)->Apply(QuantileKernelArgs);
BENCHMARK(TDigestKernelDoubleCentiles)->Apply(QuantileKernelArgs);

}  // namespace compute
}  // namespace arrow
