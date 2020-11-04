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

#include "arrow/array/array_base.h"
#include "arrow/compute/api.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {

using internal::checked_cast;

namespace compute {

constexpr int32_t kSeed = 0xfede4a7e;
constexpr int64_t kScalarCount = 1 << 10;

inline ScalarVector ToScalars(std::shared_ptr<Array> arr) {
  ScalarVector scalars{static_cast<size_t>(arr->length())};
  int64_t i = 0;
  for (auto& scalar : scalars) {
    scalar = arr->GetScalar(i++).ValueOrDie();
  }
  return scalars;
}

void BM_CastDispatch(benchmark::State& state) {  // NOLINT non-const reference
  // Repeatedly invoke a trivial Cast: the main cost should be dispatch
  random::RandomArrayGenerator rag(kSeed);

  auto int_scalars = ToScalars(rag.Int64(kScalarCount, 0, 1 << 20));

  auto double_type = float64();
  for (auto _ : state) {
    Datum timestamp_scalar;
    for (Datum int_scalar : int_scalars) {
      ASSERT_OK_AND_ASSIGN(timestamp_scalar, Cast(int_scalar, double_type));
    }
    benchmark::DoNotOptimize(timestamp_scalar);
  }

  state.SetItemsProcessed(state.iterations() * kScalarCount);
}

void BM_CastDispatchBaseline(benchmark::State& state) {  // NOLINT non-const reference
  // Repeatedly invoke a trivial Cast with all dispatch outside the hot loop
  random::RandomArrayGenerator rag(kSeed);

  auto int_scalars = ToScalars(rag.Int64(kScalarCount, 0, 1 << 20));

  auto double_type = float64();
  CastOptions cast_options;
  cast_options.to_type = double_type;
  ASSERT_OK_AND_ASSIGN(auto cast_function, GetCastFunction(double_type));
  ASSERT_OK_AND_ASSIGN(auto cast_kernel,
                       cast_function->DispatchExact({int_scalars[0]->type}));
  const auto& exec = static_cast<const ScalarKernel*>(cast_kernel)->exec;

  ExecContext exec_context;
  KernelContext kernel_context(&exec_context);
  auto cast_state =
      cast_kernel->init(&kernel_context, {cast_kernel, {double_type}, &cast_options});
  ABORT_NOT_OK(kernel_context.status());
  kernel_context.SetState(cast_state.get());

  for (auto _ : state) {
    Datum timestamp_scalar = MakeNullScalar(double_type);
    for (Datum int_scalar : int_scalars) {
      exec(&kernel_context, {{std::move(int_scalar)}, 1}, &timestamp_scalar);
      ABORT_NOT_OK(kernel_context.status());
    }
    benchmark::DoNotOptimize(timestamp_scalar);
  }

  state.SetItemsProcessed(state.iterations() * kScalarCount);
}

void BM_AddDispatch(benchmark::State& state) {  // NOLINT non-const reference
  ExecContext exec_context;
  KernelContext kernel_context(&exec_context);

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto add_function, GetFunctionRegistry()->GetFunction("add"));
    ASSERT_OK_AND_ASSIGN(auto add_kernel,
                         checked_cast<const ScalarFunction&>(*add_function)
                             .DispatchExact({int64(), int64()}));
    benchmark::DoNotOptimize(add_kernel);
  }

  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_CastDispatch)->MinTime(1.0);
BENCHMARK(BM_CastDispatchBaseline)->MinTime(1.0);
BENCHMARK(BM_AddDispatch)->MinTime(1.0);

}  // namespace compute
}  // namespace arrow
