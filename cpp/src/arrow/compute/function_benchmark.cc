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
#include "arrow/compute/exec_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

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

void BM_CastDispatch(benchmark::State& state) {
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

void BM_CastDispatchBaseline(benchmark::State& state) {
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
  auto cast_state = cast_kernel
                        ->init(&kernel_context,
                               KernelInitArgs{cast_kernel, {double_type}, &cast_options})
                        .ValueOrDie();
  kernel_context.SetState(cast_state.get());

  for (auto _ : state) {
    Datum timestamp_scalar = MakeNullScalar(double_type);
    for (Datum int_scalar : int_scalars) {
      ABORT_NOT_OK(
          exec(&kernel_context, {{std::move(int_scalar)}, 1}, &timestamp_scalar));
    }
    benchmark::DoNotOptimize(timestamp_scalar);
  }

  state.SetItemsProcessed(state.iterations() * kScalarCount);
}

void BM_AddDispatch(benchmark::State& state) {
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

static ScalarVector MakeScalarsForIsValid(int64_t nitems) {
  std::vector<std::shared_ptr<Scalar>> scalars;
  scalars.reserve(nitems);
  for (int64_t i = 0; i < nitems; ++i) {
    if (i & 0x10) {
      scalars.emplace_back(MakeNullScalar(int64()));
    } else {
      scalars.emplace_back(*MakeScalar(int64(), i));
    }
  }
  return scalars;
}

void BM_ExecuteScalarFunctionOnScalar(benchmark::State& state) {
  // Execute a trivial function, with argument dispatch in the hot path
  const int64_t N = 10000;

  auto function = checked_pointer_cast<ScalarFunction>(
      *GetFunctionRegistry()->GetFunction("is_valid"));
  const auto scalars = MakeScalarsForIsValid(N);

  ExecContext exec_context;
  KernelContext kernel_context(&exec_context);

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto& scalar : scalars) {
      const Datum result =
          *function->Execute({Datum(scalar)}, function->default_options(), &exec_context);
      total += result.scalar()->is_valid;
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * N);
}

void BM_ExecuteScalarKernelOnScalar(benchmark::State& state) {
  // Execute a trivial function, with argument dispatch outside the hot path
  const int64_t N = 10000;

  auto function = *GetFunctionRegistry()->GetFunction("is_valid");
  auto kernel = *function->DispatchExact({ValueDescr::Scalar(int64())});
  const auto& exec = static_cast<const ScalarKernel&>(*kernel).exec;

  const auto scalars = MakeScalarsForIsValid(N);

  ExecContext exec_context;
  KernelContext kernel_context(&exec_context);

  for (auto _ : state) {
    int64_t total = 0;
    for (const auto& scalar : scalars) {
      Datum result{MakeNullScalar(int64())};
      ABORT_NOT_OK(exec(&kernel_context, ExecBatch{{scalar}, /*length=*/1}, &result));
      total += result.scalar()->is_valid;
    }
    benchmark::DoNotOptimize(total);
  }

  state.SetItemsProcessed(state.iterations() * N);
}

void BM_ExecBatchIterator(benchmark::State& state) {
  // Measure overhead related to splitting ExecBatch into smaller ExecBatches
  // for parallelism or more optimal CPU cache affinity
  random::RandomArrayGenerator rag(kSeed);

  const int64_t length = 1 << 20;
  const int num_fields = 32;

  std::vector<Datum> args(num_fields);
  for (int i = 0; i < num_fields; ++i) {
    args[i] = rag.Int64(length, 0, 100)->data();
  }

  const int64_t blocksize = state.range(0);
  for (auto _ : state) {
    std::unique_ptr<detail::ExecBatchIterator> it =
        *detail::ExecBatchIterator::Make(args, blocksize);
    ExecBatch batch;
    while (it->Next(&batch)) {
      for (int i = 0; i < num_fields; ++i) {
        auto data = batch.values[i].array()->buffers[1]->data();
        benchmark::DoNotOptimize(data);
      }
    }
    benchmark::DoNotOptimize(batch);
  }
  // Provides comparability across blocksizes by looking at the iterations per
  // second. So 1000 iterations/second means that input splitting associated
  // with ExecBatchIterator takes up 1ms every time.
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_CastDispatch);
BENCHMARK(BM_CastDispatchBaseline);
BENCHMARK(BM_AddDispatch);
BENCHMARK(BM_ExecuteScalarFunctionOnScalar);
BENCHMARK(BM_ExecuteScalarKernelOnScalar);
BENCHMARK(BM_ExecBatchIterator)->RangeMultiplier(4)->Range(1024, 64 * 1024);

}  // namespace compute
}  // namespace arrow
