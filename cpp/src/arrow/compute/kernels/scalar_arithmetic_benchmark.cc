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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x94378165;

using BinaryOp = Result<Datum>(const Datum&, const Datum&, ArithmeticOptions,
                               ExecContext*);

// Add explicit overflow-checked shortcuts, for easy benchmark parametering.
static Result<Datum> AddChecked(const Datum& left, const Datum& right,
                                ArithmeticOptions options = ArithmeticOptions(),
                                ExecContext* ctx = NULLPTR) {
  options.check_overflow = true;
  return Add(left, right, std::move(options), ctx);
}

static Result<Datum> SubtractChecked(const Datum& left, const Datum& right,
                                     ArithmeticOptions options = ArithmeticOptions(),
                                     ExecContext* ctx = NULLPTR) {
  options.check_overflow = true;
  return Subtract(left, right, std::move(options), ctx);
}

static Result<Datum> MultiplyChecked(const Datum& left, const Datum& right,
                                     ArithmeticOptions options = ArithmeticOptions(),
                                     ExecContext* ctx = NULLPTR) {
  options.check_overflow = true;
  return Multiply(left, right, std::move(options), ctx);
}

static Result<Datum> DivideChecked(const Datum& left, const Datum& right,
                                   ArithmeticOptions options = ArithmeticOptions(),
                                   ExecContext* ctx = NULLPTR) {
  options.check_overflow = true;
  return Divide(left, right, std::move(options), ctx);
}

template <BinaryOp& Op, typename ArrowType, typename CType = typename ArrowType::c_type>
static void ArrayScalarKernel(benchmark::State& state) {
  RegressionArgs args(state);

  const int64_t array_size = args.size / sizeof(CType);

  // Choose values so as to avoid overflow on all ops and types
  auto min = static_cast<CType>(6);
  auto max = static_cast<CType>(min + 15);
  Datum rhs(static_cast<CType>(6));

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, min, max, args.null_proportion));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, rhs, ArithmeticOptions(), nullptr).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

template <BinaryOp& Op, typename ArrowType, typename CType = typename ArrowType::c_type>
static void ArrayArrayKernel(benchmark::State& state) {
  RegressionArgs args(state);

  // Choose values so as to avoid overflow on all ops and types
  const int64_t array_size = args.size / sizeof(CType);
  auto rmin = static_cast<CType>(1);
  auto rmax = static_cast<CType>(rmin + 6);  // 7
  auto lmin = static_cast<CType>(rmax + 1);  // 8
  auto lmax = static_cast<CType>(lmin + 6);  // 14

  auto rand = random::RandomArrayGenerator(kSeed);
  auto lhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, lmin, lmax, args.null_proportion));
  auto rhs = std::static_pointer_cast<NumericArray<ArrowType>>(
      rand.Numeric<ArrowType>(array_size, rmin, rmax, args.null_proportion));

  for (auto _ : state) {
    ABORT_NOT_OK(Op(lhs, rhs, ArithmeticOptions(), nullptr).status());
  }
  state.SetItemsProcessed(state.iterations() * array_size);
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"size", "inverse_null_proportion"});

  for (const auto inverse_null_proportion : std::vector<ArgsType>({100, 0})) {
    bench->Args({static_cast<ArgsType>(kL2Size), inverse_null_proportion});
  }
}

#define DECLARE_ARITHMETIC_BENCHMARKS(BENCHMARK, OP)             \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int64Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int32Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int16Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int8Type)->Apply(SetArgs);   \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt64Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt32Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt16Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt8Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, FloatType)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, DoubleType)->Apply(SetArgs)

// Checked floating-point variants of arithmetic operations are identical to
// non-checked variants, so do not bother measuring them.

#define DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(BENCHMARK, OP)     \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int64Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int32Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int16Type)->Apply(SetArgs);  \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, Int8Type)->Apply(SetArgs);   \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt64Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt32Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt16Type)->Apply(SetArgs); \
  BENCHMARK_TEMPLATE(BENCHMARK, OP, UInt8Type)->Apply(SetArgs);

DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Add);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Add);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Subtract);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Subtract);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Multiply);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Multiply);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayArrayKernel, Divide);
DECLARE_ARITHMETIC_BENCHMARKS(ArrayScalarKernel, Divide);

DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayArrayKernel, AddChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayScalarKernel, AddChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayArrayKernel, SubtractChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayScalarKernel, SubtractChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayArrayKernel, MultiplyChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayScalarKernel, MultiplyChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayArrayKernel, DivideChecked);
DECLARE_ARITHMETIC_CHECKED_BENCHMARKS(ArrayScalarKernel, DivideChecked);

}  // namespace compute
}  // namespace arrow
